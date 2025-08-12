"""Lookup miss handling strategies for data enrichment.

This module provides sophisticated strategies for handling lookup misses in the
data enrichment pipeline. When reference data is not available in the cache,
these handlers provide fallback mechanisms to maintain pipeline throughput.

Key Features:
- Multiple fallback strategies (DLQ, retry, alternative sources, ML prediction)
- Configurable retry policies with exponential backoff
- Dead letter queue for failed lookups requiring manual intervention
- Integration with alternative data sources (APIs, secondary caches)
- Machine learning-based prediction for missing reference data
- Metrics tracking for lookup miss patterns and resolution rates

Architecture:
    Lookup Miss → Strategy Selection → Fallback Processing → Resolution/DLQ

The miss handler integrates with the enrichment task to provide seamless
fallback processing when primary lookup sources fail.

Usage:
    handler = LookupMissHandler([
        RetryStrategy(max_retries=3),
        AlternativeSourceStrategy(backup_cache),
        MLPredictionStrategy(model),
        DLQStrategy()
    ])
    
    result = await handler.handle_miss(lookup_key, context)
"""

import asyncio
import json
import time
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Union, Callable
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from enum import Enum
import random
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from dag_orchestrator.context import PipelineContext
from pipeline.kafka_producer import KafkaProducerAsync
from config.settings import get_settings
from config.logging_config import get_logger


class MissResolutionStatus(Enum):
    """Status of lookup miss resolution attempts."""
    RESOLVED = "resolved"
    RETRY_LATER = "retry_later"
    SEND_TO_DLQ = "send_to_dlq"
    USE_FALLBACK = "use_fallback"
    SKIP = "skip"


@dataclass
class LookupMissRecord:
    """Record of a lookup miss for tracking and retry."""
    lookup_key: str
    reference_type: str
    miss_timestamp: str
    retry_count: int = 0
    max_retries: int = 3
    next_retry_time: Optional[str] = None
    original_record: Dict[str, Any] = None
    resolution_status: str = MissResolutionStatus.RETRY_LATER.value
    error_messages: List[str] = None
    
    def __post_init__(self):
        if self.error_messages is None:
            self.error_messages = []
    
    def can_retry(self) -> bool:
        """Check if this miss can be retried."""
        if self.retry_count >= self.max_retries:
            return False
        
        if self.next_retry_time:
            next_retry = datetime.fromisoformat(self.next_retry_time)
            return datetime.utcnow() >= next_retry
        
        return True
    
    def schedule_retry(self, delay_seconds: int):
        """Schedule the next retry attempt."""
        self.next_retry_time = (
            datetime.utcnow() + timedelta(seconds=delay_seconds)
        ).isoformat()
        self.retry_count += 1


class LookupMissStrategy(ABC):
    """
    Abstract base class for lookup miss handling strategies.
    
    Each strategy implements a specific approach to resolving lookup misses,
    such as retrying, using alternative sources, or applying ML predictions.
    Strategies are applied in order until one succeeds or all fail.
    """
    
    def __init__(self, name: str, priority: int = 100):
        """
        Initialize strategy.
        
        Args:
            name: Strategy name for logging and metrics
            priority: Strategy priority (lower = higher priority)
        """
        self.name = name
        self.priority = priority
        self.logger = get_logger(f"miss_strategy.{name}")
        self.success_count = 0
        self.failure_count = 0
    
    @abstractmethod
    async def handle_miss(
        self, 
        miss_record: LookupMissRecord,
        context: PipelineContext
    ) -> tuple[MissResolutionStatus, Optional[Dict[str, Any]]]:
        """
        Handle a lookup miss.
        
        Args:
            miss_record: Details of the lookup miss
            context: Pipeline context
            
        Returns:
            Tuple of (resolution_status, resolved_data_or_none)
        """
        pass
    
    def get_stats(self) -> Dict[str, Any]:
        """Get strategy performance statistics."""
        total = self.success_count + self.failure_count
        success_rate = self.success_count / max(total, 1)
        
        return {
            "name": self.name,
            "priority": self.priority,
            "success_count": self.success_count,
            "failure_count": self.failure_count,
            "success_rate": success_rate,
            "total_attempts": total
        }


class RetryStrategy(LookupMissStrategy):
    """
    Strategy that retries failed lookups with exponential backoff.
    
    This strategy is useful for transient failures where the data might
    become available after a short delay (e.g., cache refresh, network issues).
    """
    
    def __init__(
        self,
        max_retries: int = 3,
        base_delay_seconds: int = 60,
        max_delay_seconds: int = 3600,
        backoff_multiplier: float = 2.0,
        jitter: bool = True
    ):
        """
        Initialize retry strategy.
        
        Args:
            max_retries: Maximum number of retry attempts
            base_delay_seconds: Base delay between retries
            max_delay_seconds: Maximum delay between retries
            backoff_multiplier: Exponential backoff multiplier
            jitter: Whether to add random jitter to delays
        """
        super().__init__("retry", priority=50)
        self.max_retries = max_retries
        self.base_delay_seconds = base_delay_seconds
        self.max_delay_seconds = max_delay_seconds
        self.backoff_multiplier = backoff_multiplier
        self.jitter = jitter
    
    async def handle_miss(
        self, 
        miss_record: LookupMissRecord,
        context: PipelineContext
    ) -> tuple[MissResolutionStatus, Optional[Dict[str, Any]]]:
        """Handle miss with retry logic."""
        
        # Check if we can retry
        if not miss_record.can_retry():
            self.failure_count += 1
            return MissResolutionStatus.SEND_TO_DLQ, None
        
        # Calculate retry delay with exponential backoff
        delay = min(
            self.base_delay_seconds * (self.backoff_multiplier ** miss_record.retry_count),
            self.max_delay_seconds
        )
        
        # Add jitter to prevent thundering herd
        if self.jitter:
            delay *= (0.5 + random.random() * 0.5)
        
        # Schedule retry
        miss_record.schedule_retry(int(delay))
        miss_record.resolution_status = MissResolutionStatus.RETRY_LATER.value
        
        self.logger.debug(
            f"Scheduled retry for {miss_record.lookup_key} in {delay:.0f} seconds "
            f"(attempt {miss_record.retry_count}/{self.max_retries})"
        )
        
        return MissResolutionStatus.RETRY_LATER, None


class AlternativeSourceStrategy(LookupMissStrategy):
    """
    Strategy that queries alternative data sources for missing references.
    
    This strategy attempts to resolve lookups using backup data sources
    such as secondary caches, external APIs, or historical data stores.
    """
    
    def __init__(
        self,
        alternative_sources: List[Callable[[str, str], Optional[Dict[str, Any]]]] = None,
        timeout_seconds: float = 5.0
    ):
        """
        Initialize alternative source strategy.
        
        Args:
            alternative_sources: List of functions that can lookup reference data
            timeout_seconds: Timeout for alternative source queries
        """
        super().__init__("alternative_source", priority=75)
        self.alternative_sources = alternative_sources or []
        self.timeout_seconds = timeout_seconds
    
    async def handle_miss(
        self, 
        miss_record: LookupMissRecord,
        context: PipelineContext
    ) -> tuple[MissResolutionStatus, Optional[Dict[str, Any]]]:
        """Handle miss by querying alternative sources."""
        
        for i, source_func in enumerate(self.alternative_sources):
            try:
                # Query alternative source with timeout
                result = await asyncio.wait_for(
                    asyncio.get_event_loop().run_in_executor(
                        None,
                        source_func,
                        miss_record.lookup_key,
                        miss_record.reference_type
                    ),
                    timeout=self.timeout_seconds
                )
                
                if result:
                    self.success_count += 1
                    self.logger.info(
                        f"Resolved {miss_record.lookup_key} using alternative source {i}"
                    )
                    return MissResolutionStatus.RESOLVED, result
                    
            except asyncio.TimeoutError:
                self.logger.warning(
                    f"Alternative source {i} timed out for {miss_record.lookup_key}"
                )
            except Exception as e:
                self.logger.warning(
                    f"Alternative source {i} failed for {miss_record.lookup_key}: {e}"
                )
        
        self.failure_count += 1
        return MissResolutionStatus.USE_FALLBACK, None
    
    def add_alternative_source(self, source_func: Callable[[str, str], Optional[Dict[str, Any]]]):
        """Add an alternative data source."""
        self.alternative_sources.append(source_func)


class MLPredictionStrategy(LookupMissStrategy):
    """
    Strategy that uses machine learning to predict missing reference data.
    
    This strategy applies ML models to predict likely values for missing
    reference data based on available record attributes and historical patterns.
    """
    
    def __init__(
        self,
        model: Optional[Any] = None,
        feature_extractors: Dict[str, Callable[[Dict[str, Any]], Any]] = None,
        confidence_threshold: float = 0.8
    ):
        """
        Initialize ML prediction strategy.
        
        Args:
            model: ML model for predictions (scikit-learn, TensorFlow, etc.)
            feature_extractors: Functions to extract features from records
            confidence_threshold: Minimum confidence for using predictions
        """
        super().__init__("ml_prediction", priority=80)
        self.model = model
        self.feature_extractors = feature_extractors or {}
        self.confidence_threshold = confidence_threshold
    
    async def handle_miss(
        self, 
        miss_record: LookupMissRecord,
        context: PipelineContext
    ) -> tuple[MissResolutionStatus, Optional[Dict[str, Any]]]:
        """Handle miss using ML prediction."""
        
        if not self.model or not self.feature_extractors:
            return MissResolutionStatus.USE_FALLBACK, None
        
        try:
            # Extract features from the original record
            features = {}
            for feature_name, extractor in self.feature_extractors.items():
                try:
                    features[feature_name] = extractor(miss_record.original_record or {})
                except Exception as e:
                    self.logger.warning(f"Feature extraction failed for {feature_name}: {e}")
                    features[feature_name] = None
            
            # Make prediction
            prediction_result = await self._make_prediction(features, miss_record.reference_type)
            
            if prediction_result and prediction_result.get("confidence", 0) >= self.confidence_threshold:
                self.success_count += 1
                self.logger.info(
                    f"ML prediction for {miss_record.lookup_key} "
                    f"(confidence: {prediction_result['confidence']:.2f})"
                )
                
                # Add metadata to indicate this is a prediction
                predicted_data = prediction_result["data"]
                predicted_data["_ml_predicted"] = True
                predicted_data["_ml_confidence"] = prediction_result["confidence"]
                predicted_data["_ml_model_version"] = getattr(self.model, "version", "unknown")
                
                return MissResolutionStatus.RESOLVED, predicted_data
            
        except Exception as e:
            self.logger.error(f"ML prediction failed for {miss_record.lookup_key}: {e}")
        
        self.failure_count += 1
        return MissResolutionStatus.USE_FALLBACK, None
    
    async def _make_prediction(
        self, 
        features: Dict[str, Any], 
        reference_type: str
    ) -> Optional[Dict[str, Any]]:
        """Make ML prediction based on features."""
        # This is a placeholder for ML prediction logic
        # In practice, you would:
        # 1. Preprocess features according to model requirements
        # 2. Run inference using your ML framework
        # 3. Post-process predictions into the expected format
        
        # Example structure:
        return {
            "data": {
                "predicted_field": "predicted_value",
                "confidence_score": 0.85
            },
            "confidence": 0.85
        }


class FallbackValueStrategy(LookupMissStrategy):
    """
    Strategy that provides sensible default values for missing references.
    
    This strategy uses configured default values or business rules to provide
    fallback data when lookups fail, ensuring pipeline continuity.
    """
    
    def __init__(
        self,
        fallback_values: Dict[str, Dict[str, Any]] = None,
        fallback_generators: Dict[str, Callable[[Dict[str, Any]], Dict[str, Any]]] = None
    ):
        """
        Initialize fallback value strategy.
        
        Args:
            fallback_values: Static fallback values by reference type
            fallback_generators: Functions to generate fallback values
        """
        super().__init__("fallback_values", priority=90)
        self.fallback_values = fallback_values or {}
        self.fallback_generators = fallback_generators or {}
    
    async def handle_miss(
        self, 
        miss_record: LookupMissRecord,
        context: PipelineContext
    ) -> tuple[MissResolutionStatus, Optional[Dict[str, Any]]]:
        """Handle miss by providing fallback values."""
        
        # Try generator function first
        if miss_record.reference_type in self.fallback_generators:
            try:
                generator_func = self.fallback_generators[miss_record.reference_type]
                fallback_data = generator_func(miss_record.original_record or {})
                
                if fallback_data:
                    fallback_data["_fallback_generated"] = True
                    self.success_count += 1
                    return MissResolutionStatus.RESOLVED, fallback_data
                    
            except Exception as e:
                self.logger.warning(f"Fallback generator failed: {e}")
        
        # Use static fallback values
        if miss_record.reference_type in self.fallback_values:
            fallback_data = self.fallback_values[miss_record.reference_type].copy()
            fallback_data["_fallback_static"] = True
            self.success_count += 1
            return MissResolutionStatus.RESOLVED, fallback_data
        
        self.failure_count += 1
        return MissResolutionStatus.SEND_TO_DLQ, None


class DLQStrategy(LookupMissStrategy):
    """
    Strategy that sends unresolvable lookup misses to a dead letter queue.
    
    This strategy is the final fallback that sends failed lookups to a DLQ
    for manual investigation and resolution.
    """
    
    def __init__(
        self,
        dlq_topic: str = "lookup-misses-dlq",
        include_original_record: bool = True
    ):
        """
        Initialize DLQ strategy.
        
        Args:
            dlq_topic: Kafka topic for dead letter queue
            include_original_record: Whether to include original record in DLQ
        """
        super().__init__("dlq", priority=200)  # Lowest priority (last resort)
        self.dlq_topic = dlq_topic
        self.include_original_record = include_original_record
        self.kafka_producer = None
    
    async def handle_miss(
        self, 
        miss_record: LookupMissRecord,
        context: PipelineContext
    ) -> tuple[MissResolutionStatus, Optional[Dict[str, Any]]]:
        """Handle miss by sending to DLQ."""
        
        try:
            # Initialize Kafka producer if needed
            if not self.kafka_producer:
                settings = get_settings()
                self.kafka_producer = KafkaProducerAsync(settings.kafka)
            
            # Create DLQ message
            dlq_message = {
                "miss_record": asdict(miss_record),
                "sent_to_dlq_at": datetime.utcnow().isoformat(),
                "pipeline_context_id": context.get_metadata().get("created_at", "unknown")
            }
            
            if self.include_original_record:
                dlq_message["original_record"] = miss_record.original_record
            
            # Send to DLQ
            await self.kafka_producer.produce(
                json.dumps(dlq_message, default=str),
                key=f"{miss_record.reference_type}_{miss_record.lookup_key}"
            )
            
            self.success_count += 1
            self.logger.info(f"Sent {miss_record.lookup_key} to DLQ")
            
            return MissResolutionStatus.SEND_TO_DLQ, None
            
        except Exception as e:
            self.logger.error(f"Failed to send {miss_record.lookup_key} to DLQ: {e}")
            self.failure_count += 1
            return MissResolutionStatus.SKIP, None


class LookupMissHandler:
    """
    Main handler for lookup misses using multiple resolution strategies.
    
    This class orchestrates multiple strategies to resolve lookup misses,
    applying them in order of priority until one succeeds or all fail.
    It also maintains metrics and retry queues for failed lookups.
    
    Key Features:
    1. Multiple configurable resolution strategies
    2. Priority-based strategy execution
    3. Retry queue management for delayed resolution
    4. Comprehensive metrics and logging
    5. Integration with monitoring systems
    """
    
    def __init__(
        self,
        strategies: List[LookupMissStrategy] = None,
        retry_queue_size: int = 10000,
        cleanup_interval_minutes: int = 60
    ):
        """
        Initialize lookup miss handler.
        
        Args:
            strategies: List of resolution strategies (creates defaults if None)
            retry_queue_size: Maximum size of retry queue
            cleanup_interval_minutes: How often to clean up old retry entries
        """
        self.strategies = strategies or self._create_default_strategies()
        self.strategies.sort(key=lambda s: s.priority)  # Sort by priority
        
        self.logger = get_logger("lookup_miss_handler")
        
        # Retry management
        self.retry_queue: Dict[str, LookupMissRecord] = {}
        self.retry_queue_size = retry_queue_size
        
        # Metrics
        self.total_misses = 0
        self.resolved_count = 0
        self.dlq_count = 0
        self.skip_count = 0
        
        # Background cleanup
        self._cleanup_task = None
        self._stop_cleanup = asyncio.Event()
        
        # Start cleanup task
        self._start_cleanup_task(cleanup_interval_minutes)
    
    def _create_default_strategies(self) -> List[LookupMissStrategy]:
        """Create default set of resolution strategies."""
        return [
            RetryStrategy(max_retries=2, base_delay_seconds=60),
            AlternativeSourceStrategy(),
            FallbackValueStrategy({
                "customers": {
                    "customer_name": "Unknown Customer",
                    "customer_type": "Regular",
                    "segment": "Default"
                },
                "products": {
                    "product_name": "Unknown Product",
                    "category": "General"
                },
                "locations": {
                    "location_name": "Unknown Location",
                    "region": "Unknown"
                }
            }),
            DLQStrategy()
        ]
    
    async def handle_lookup_miss(
        self,
        lookup_key: str,
        reference_type: str,
        original_record: Dict[str, Any],
        context: PipelineContext
    ) -> tuple[MissResolutionStatus, Optional[Dict[str, Any]]]:
        """
        Handle a lookup miss using configured strategies.
        
        Args:
            lookup_key: The key that failed to lookup
            reference_type: Type of reference data (customers, products, etc.)
            original_record: Original record being enriched
            context: Pipeline context
            
        Returns:
            Tuple of (resolution_status, resolved_data_or_none)
        """
        self.total_misses += 1
        
        # Check if this miss is already in retry queue
        miss_id = f"{reference_type}_{lookup_key}"
        if miss_id in self.retry_queue:
            miss_record = self.retry_queue[miss_id]
            if not miss_record.can_retry():
                # Remove from retry queue and continue with strategies
                del self.retry_queue[miss_id]
            else:
                # Still in retry period, return retry status
                return MissResolutionStatus.RETRY_LATER, None
        else:
            # Create new miss record
            miss_record = LookupMissRecord(
                lookup_key=lookup_key,
                reference_type=reference_type,
                miss_timestamp=datetime.utcnow().isoformat(),
                original_record=original_record
            )
        
        # Try each strategy in priority order
        for strategy in self.strategies:
            try:
                self.logger.debug(f"Trying strategy {strategy.name} for {lookup_key}")
                
                status, data = await strategy.handle_miss(miss_record, context)
                
                if status == MissResolutionStatus.RESOLVED:
                    self.resolved_count += 1
                    # Remove from retry queue if it was there
                    if miss_id in self.retry_queue:
                        del self.retry_queue[miss_id]
                    
                    self.logger.info(
                        f"Resolved {lookup_key} using strategy {strategy.name}"
                    )
                    return status, data
                
                elif status == MissResolutionStatus.RETRY_LATER:
                    # Add to retry queue if not already there
                    if miss_id not in self.retry_queue:
                        if len(self.retry_queue) < self.retry_queue_size:
                            self.retry_queue[miss_id] = miss_record
                        else:
                            self.logger.warning("Retry queue full, sending to DLQ")
                            self.dlq_count += 1
                            return MissResolutionStatus.SEND_TO_DLQ, None
                    return status, None
                
                elif status == MissResolutionStatus.SEND_TO_DLQ:
                    self.dlq_count += 1
                    # Remove from retry queue
                    if miss_id in self.retry_queue:
                        del self.retry_queue[miss_id]
                    return status, None
                
                elif status == MissResolutionStatus.SKIP:
                    self.skip_count += 1
                    # Remove from retry queue
                    if miss_id in self.retry_queue:
                        del self.retry_queue[miss_id]
                    return status, None
                
                # STATUS.USE_FALLBACK means try next strategy
                continue
                
            except Exception as e:
                self.logger.error(f"Strategy {strategy.name} failed for {lookup_key}: {e}")
                continue
        
        # All strategies failed
        self.skip_count += 1
        return MissResolutionStatus.SKIP, None
    
    def _start_cleanup_task(self, interval_minutes: int):
        """Start background task for retry queue cleanup."""
        async def cleanup_loop():
            while not self._stop_cleanup.is_set():
                try:
                    await asyncio.sleep(interval_minutes * 60)
                    await self._cleanup_retry_queue()
                except asyncio.CancelledError:
                    break
                except Exception as e:
                    self.logger.error(f"Error in cleanup task: {e}")
        
        self._cleanup_task = asyncio.create_task(cleanup_loop())
    
    async def _cleanup_retry_queue(self):
        """Clean up expired entries from retry queue."""
        current_time = datetime.utcnow()
        expired_keys = []
        
        for miss_id, miss_record in self.retry_queue.items():
            # Remove entries that have exceeded max retries
            if not miss_record.can_retry():
                expired_keys.append(miss_id)
            
            # Remove very old entries (older than 24 hours)
            miss_time = datetime.fromisoformat(miss_record.miss_timestamp)
            if (current_time - miss_time).total_seconds() > 86400:  # 24 hours
                expired_keys.append(miss_id)
        
        # Remove expired entries
        for key in expired_keys:
            del self.retry_queue[key]
        
        if expired_keys:
            self.logger.info(f"Cleaned up {len(expired_keys)} expired retry entries")
    
    def add_strategy(self, strategy: LookupMissStrategy):
        """Add a new resolution strategy."""
        self.strategies.append(strategy)
        self.strategies.sort(key=lambda s: s.priority)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get comprehensive handler statistics."""
        resolution_rate = self.resolved_count / max(self.total_misses, 1)
        
        return {
            "total_misses": self.total_misses,
            "resolved_count": self.resolved_count,
            "dlq_count": self.dlq_count,
            "skip_count": self.skip_count,
            "resolution_rate": resolution_rate,
            "retry_queue_size": len(self.retry_queue),
            "strategy_stats": [strategy.get_stats() for strategy in self.strategies]
        }
    
    async def process_retry_queue(self, context: PipelineContext) -> List[Dict[str, Any]]:
        """Process retry queue and return resolved records."""
        resolved_records = []
        
        for miss_id, miss_record in list(self.retry_queue.items()):
            if miss_record.can_retry():
                try:
                    status, data = await self.handle_lookup_miss(
                        miss_record.lookup_key,
                        miss_record.reference_type,
                        miss_record.original_record or {},
                        context
                    )
                    
                    if status == MissResolutionStatus.RESOLVED and data:
                        # Create resolved record
                        resolved_record = (miss_record.original_record or {}).copy()
                        resolved_record.update(data)
                        resolved_record["_resolved_from_retry"] = True
                        resolved_records.append(resolved_record)
                        
                except Exception as e:
                    self.logger.error(f"Error processing retry for {miss_id}: {e}")
        
        return resolved_records
    
    async def shutdown(self):
        """Shutdown the miss handler."""
        self._stop_cleanup.set()
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass
        
        # Close any Kafka producers in strategies
        for strategy in self.strategies:
            if hasattr(strategy, 'kafka_producer') and strategy.kafka_producer:
                await strategy.kafka_producer.close()
        
        self.logger.info("Lookup miss handler shutdown completed")
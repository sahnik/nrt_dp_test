"""Lookup Cache Manager for data enrichment.

This module provides tasks for managing lookup caches used in data enrichment.
The cache is populated from Kafka topics containing reference data from Oracle
and other external sources.

Key Features:
- In-memory lookup cache with fast O(1) lookups
- Kafka consumer for cache warming and updates
- Cache refresh strategies (startup, periodic, forced)
- Cache eviction policies for memory management
- Metrics and monitoring for cache performance

Architecture:
    Kafka Lookup Topic → Cache Manager → In-Memory Cache → Enrichment Tasks
    
The cache manager consumes from a compacted Kafka topic to ensure it always
has the latest version of reference data. It supports both full refresh and
incremental updates.

Usage in DAG:
    cache_task = LookupCacheTask(
        name="lookup_cache",
        refresh_strategy="startup",
        max_cache_size=1000000
    )
    
    enrichment_task = DataEnrichmentTask(
        name="enrichment", 
        lookup_fields=["customer_id", "product_id"]
    )
"""

import asyncio
import json
import time
from typing import Dict, Any, Optional, List, Set
from datetime import datetime, timedelta
from collections import defaultdict
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '../..'))

from dag_orchestrator.task import Task
from dag_orchestrator.context import PipelineContext
from pipeline.kafka_consumer import KafkaConsumer
from config.settings import get_settings
from config.logging_config import get_logger


class LookupCache:
    """
    In-memory lookup cache with eviction and metrics.
    
    This class manages the in-memory storage of lookup data, providing fast
    access patterns optimized for the enrichment pipeline. It includes cache
    eviction policies and comprehensive metrics tracking.
    
    Features:
    - Multiple index structures for different lookup patterns
    - LRU eviction when cache size limits are reached
    - Cache hit/miss metrics tracking
    - Memory usage monitoring
    - TTL support for time-sensitive data
    
    Attributes:
        data: Main cache storage (key -> value mapping)
        indexes: Secondary indexes for multi-key lookups
        access_times: LRU tracking for eviction
        metrics: Cache performance metrics
    """
    
    def __init__(self, max_size: int = 1000000, ttl_seconds: Optional[int] = None):
        """
        Initialize the lookup cache.
        
        Args:
            max_size: Maximum number of entries to store
            ttl_seconds: Time-to-live for cache entries (None = no TTL)
        """
        self.max_size = max_size
        self.ttl_seconds = ttl_seconds
        self.logger = get_logger("lookup_cache")
        
        # Cache storage
        self.data: Dict[str, Any] = {}
        self.indexes: Dict[str, Dict[str, Set[str]]] = defaultdict(lambda: defaultdict(set))
        
        # LRU tracking
        self.access_times: Dict[str, float] = {}
        
        # TTL tracking
        self.expiry_times: Dict[str, float] = {} if ttl_seconds else None
        
        # Metrics
        self.metrics = {
            "hits": 0,
            "misses": 0,
            "evictions": 0,
            "size": 0,
            "last_refresh": None,
            "refresh_count": 0
        }
        
    def get(self, key: str) -> Optional[Any]:
        """
        Retrieve value from cache with LRU and TTL handling.
        
        Args:
            key: Cache key to lookup
            
        Returns:
            Cached value or None if not found/expired
        """
        # Check TTL expiration
        if self.expiry_times and key in self.expiry_times:
            if time.time() > self.expiry_times[key]:
                self._remove_key(key)
                self.metrics["misses"] += 1
                return None
        
        if key in self.data:
            # Update LRU
            self.access_times[key] = time.time()
            self.metrics["hits"] += 1
            return self.data[key]
        else:
            self.metrics["misses"] += 1
            return None
    
    def put(self, key: str, value: Any, index_fields: Optional[Dict[str, Any]] = None):
        """
        Store value in cache with optional indexing.
        
        Args:
            key: Cache key
            value: Value to store
            index_fields: Additional fields to create indexes for
        """
        # Check if we need to evict entries
        if len(self.data) >= self.max_size and key not in self.data:
            self._evict_lru()
        
        # Store the main entry
        self.data[key] = value
        self.access_times[key] = time.time()
        
        # Set TTL if configured
        if self.expiry_times is not None:
            self.expiry_times[key] = time.time() + self.ttl_seconds
        
        # Create secondary indexes
        if index_fields:
            for field_name, field_value in index_fields.items():
                if field_value is not None:
                    self.indexes[field_name][str(field_value)].add(key)
        
        self.metrics["size"] = len(self.data)
    
    def get_by_index(self, index_name: str, index_value: str) -> List[Any]:
        """
        Retrieve values using secondary index.
        
        Args:
            index_name: Name of the index field
            index_value: Value to lookup in the index
            
        Returns:
            List of matching values
        """
        keys = self.indexes.get(index_name, {}).get(str(index_value), set())
        results = []
        
        for key in keys:
            value = self.get(key)  # This updates LRU and handles TTL
            if value is not None:
                results.append(value)
        
        return results
    
    def remove(self, key: str):
        """Remove entry from cache."""
        self._remove_key(key)
    
    def clear(self):
        """Clear all cache entries."""
        self.data.clear()
        self.indexes.clear()
        self.access_times.clear()
        if self.expiry_times:
            self.expiry_times.clear()
        self.metrics["size"] = 0
    
    def _remove_key(self, key: str):
        """Internal method to remove a key and clean up indexes."""
        if key in self.data:
            # Remove from main storage
            del self.data[key]
            del self.access_times[key]
            
            if self.expiry_times and key in self.expiry_times:
                del self.expiry_times[key]
            
            # Clean up indexes
            for index_dict in self.indexes.values():
                for value_set in index_dict.values():
                    value_set.discard(key)
            
            self.metrics["size"] = len(self.data)
    
    def _evict_lru(self):
        """Evict least recently used entry."""
        if not self.access_times:
            return
        
        # Find LRU key
        lru_key = min(self.access_times.keys(), key=lambda k: self.access_times[k])
        self._remove_key(lru_key)
        self.metrics["evictions"] += 1
        
        self.logger.debug(f"Evicted LRU cache entry: {lru_key}")
    
    def get_metrics(self) -> Dict[str, Any]:
        """Get cache performance metrics."""
        total_requests = self.metrics["hits"] + self.metrics["misses"]
        hit_rate = self.metrics["hits"] / total_requests if total_requests > 0 else 0
        
        return {
            **self.metrics,
            "hit_rate": hit_rate,
            "miss_rate": 1 - hit_rate,
            "memory_usage": len(self.data) / self.max_size if self.max_size > 0 else 0
        }


class LookupCacheTask(Task):
    """
    DAG task for managing lookup cache from Kafka topic.
    
    This task is responsible for maintaining the in-memory lookup cache that
    provides fast reference data access for enrichment tasks. It consumes from
    a Kafka topic containing reference data and keeps the cache updated.
    
    Key Responsibilities:
    1. Initialize and warm the lookup cache on startup
    2. Consume updates from Kafka lookup topic
    3. Handle cache refresh strategies (startup, periodic, manual)
    4. Provide cache metrics and health monitoring
    5. Make cache available to downstream enrichment tasks
    
    Refresh Strategies:
    - "startup": Refresh cache once when pipeline starts
    - "periodic": Refresh cache at regular intervals
    - "continuous": Continuously consume updates (streaming mode)
    - "manual": Only refresh when explicitly requested
    
    The task adds the cache instance to the pipeline context so that downstream
    enrichment tasks can access it for lookups.
    """
    
    def __init__(
        self,
        name: str = "lookup_cache",
        refresh_strategy: str = "startup",
        refresh_interval_minutes: int = 60,
        max_cache_size: int = 1000000,
        cache_ttl_seconds: Optional[int] = None,
        lookup_topic: str = "lookup-data",
        retries: int = 3,
        retry_delay: float = 2.0
    ):
        """
        Initialize the lookup cache task.
        
        Args:
            name: Task name
            refresh_strategy: When to refresh cache ("startup", "periodic", "continuous", "manual")
            refresh_interval_minutes: Minutes between periodic refreshes
            max_cache_size: Maximum number of cache entries
            cache_ttl_seconds: Cache entry TTL (None for no expiration)
            lookup_topic: Kafka topic containing lookup data
            retries: Number of retry attempts
            retry_delay: Delay between retries
        """
        super().__init__(name, retries, retry_delay)
        
        self.refresh_strategy = refresh_strategy
        self.refresh_interval_minutes = refresh_interval_minutes
        self.lookup_topic = lookup_topic
        self.logger = get_logger(f"task.{name}")
        
        # Initialize cache
        self.cache = LookupCache(max_cache_size, cache_ttl_seconds)
        
        # Kafka consumer for cache updates
        self.kafka_consumer = None
        self.last_refresh = None
        
        # Settings
        self.settings = get_settings()
    
    async def execute(self, context: PipelineContext) -> PipelineContext:
        """
        Execute the lookup cache management task.
        
        Args:
            context: Pipeline context
            
        Returns:
            Updated context with cache reference
        """
        try:
            # Check if cache refresh is needed
            if self._should_refresh_cache():
                await self._refresh_cache()
            
            # Make cache available to downstream tasks
            context.set("lookup_cache", self.cache)
            
            # Add cache metrics to task output
            context.add_task_output(self.name, {
                "cache_metrics": self.cache.get_metrics(),
                "refresh_strategy": self.refresh_strategy,
                "last_refresh": self.last_refresh.isoformat() if self.last_refresh else None
            })
            
            self.logger.debug(f"Cache task completed - {self.cache.get_metrics()}")
            
            return context
            
        except Exception as e:
            self.logger.error(f"Cache task failed: {e}")
            context.add_error(self.name, str(e))
            
            # Provide empty cache on failure to avoid blocking pipeline
            context.set("lookup_cache", LookupCache(0))  # Empty cache
            raise
    
    def _should_refresh_cache(self) -> bool:
        """Determine if cache should be refreshed based on strategy."""
        if self.refresh_strategy == "manual":
            return False
        
        if self.refresh_strategy == "startup":
            return self.last_refresh is None
        
        if self.refresh_strategy == "periodic":
            if self.last_refresh is None:
                return True
            
            time_since_refresh = datetime.utcnow() - self.last_refresh
            return time_since_refresh >= timedelta(minutes=self.refresh_interval_minutes)
        
        if self.refresh_strategy == "continuous":
            # For continuous mode, we'd implement a background consumer
            # For now, treat it like startup + periodic
            return self.last_refresh is None
        
        return False
    
    async def _refresh_cache(self):
        """Refresh the lookup cache from Kafka topic."""
        self.logger.info(f"Refreshing lookup cache from topic: {self.lookup_topic}")
        
        try:
            # Create temporary consumer to read lookup data
            consumer = KafkaConsumer(
                settings=self.settings.kafka,
                topics=[self.lookup_topic],
                consumer_group=f"{self.settings.kafka.consumer_group}_cache"
            )
            
            # Seek to beginning to get all data (compacted topic)
            await consumer.seek_to_beginning()
            
            records_loaded = 0
            
            # Consume all available messages
            async for message in consumer.consume_with_timeout(timeout_seconds=30):
                try:
                    # Parse lookup data
                    lookup_data = json.loads(message.value)
                    
                    # Extract key and data
                    cache_key = message.key if message.key else f"lookup_{records_loaded}"
                    
                    # Create index fields for secondary lookups
                    index_fields = {}
                    if "data" in lookup_data:
                        data_record = lookup_data["data"]
                        
                        # Create indexes for common lookup fields
                        for field in ["customer_id", "product_id", "location_id", "user_id"]:
                            if field in data_record:
                                index_fields[field] = data_record[field]
                    
                    # Store in cache
                    self.cache.put(cache_key, lookup_data, index_fields)
                    records_loaded += 1
                    
                except Exception as e:
                    self.logger.warning(f"Failed to process lookup record: {e}")
                    continue
            
            await consumer.close()
            
            self.last_refresh = datetime.utcnow()
            self.cache.metrics["last_refresh"] = self.last_refresh.isoformat()
            self.cache.metrics["refresh_count"] += 1
            
            self.logger.info(
                f"Cache refresh completed - Loaded {records_loaded} records, "
                f"Cache size: {self.cache.metrics['size']}"
            )
            
        except Exception as e:
            self.logger.error(f"Cache refresh failed: {e}")
            raise
    
    async def force_refresh(self):
        """Manually force a cache refresh."""
        self.logger.info("Forcing cache refresh")
        await self._refresh_cache()
    
    async def cleanup(self):
        """Cleanup resources."""
        if self.kafka_consumer:
            await self.kafka_consumer.close()
        
        # Clear cache to free memory
        self.cache.clear()
        
        self.logger.info("Lookup cache task cleanup completed")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get detailed cache statistics."""
        return {
            "task_name": self.name,
            "refresh_strategy": self.refresh_strategy,
            "last_refresh": self.last_refresh.isoformat() if self.last_refresh else None,
            "cache_metrics": self.cache.get_metrics(),
            "lookup_topic": self.lookup_topic
        }


class CacheWarmer:
    """
    Utility class for warming up caches in background.
    
    This class can be used to pre-warm caches before pipeline startup
    or to refresh caches in background threads without blocking the
    main pipeline execution.
    """
    
    def __init__(self, cache_task: LookupCacheTask):
        """
        Initialize cache warmer.
        
        Args:
            cache_task: The cache task to warm up
        """
        self.cache_task = cache_task
        self.logger = get_logger("cache_warmer")
        
    async def warm_cache(self) -> Dict[str, Any]:
        """
        Warm up the cache and return statistics.
        
        Returns:
            Dictionary with warming statistics
        """
        start_time = time.time()
        
        try:
            await self.cache_task.force_refresh()
            
            duration = time.time() - start_time
            stats = self.cache_task.get_cache_stats()
            stats["warm_duration_seconds"] = duration
            
            self.logger.info(f"Cache warming completed in {duration:.2f} seconds")
            return stats
            
        except Exception as e:
            self.logger.error(f"Cache warming failed: {e}")
            raise
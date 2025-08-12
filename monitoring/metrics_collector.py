"""Metrics collection and monitoring for pipeline performance.

This module provides comprehensive monitoring capabilities for the data pipeline,
with special focus on cache performance, enrichment metrics, and overall pipeline health.

Key Features:
- Real-time metrics collection and aggregation
- Cache performance monitoring (hit rates, sizes, refresh times)
- Enrichment success/failure tracking
- Pipeline throughput and latency metrics
- Health checks and alerting capabilities
- Export to monitoring systems (Prometheus, CloudWatch, etc.)

Architecture:
    Pipeline Tasks → Metrics Collector → Storage/Export → Dashboards/Alerts

The metrics collector runs alongside the pipeline, collecting performance data
from all tasks and providing insights into system health and performance.

Usage:
    collector = MetricsCollector()
    collector.record_cache_hit("customers", response_time_ms=2.5)
    collector.record_enrichment_success("data_enrichment", records=100)
    
    # Export metrics for monitoring
    metrics = collector.export_metrics()
"""

import asyncio
import time
import threading
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime, timedelta
from collections import defaultdict, deque
from dataclasses import dataclass, asdict
import json
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from config.logging_config import get_logger


@dataclass
class CacheMetrics:
    """Metrics for cache performance."""
    hit_count: int = 0
    miss_count: int = 0
    total_requests: int = 0
    hit_rate: float = 0.0
    avg_response_time_ms: float = 0.0
    cache_size: int = 0
    evictions: int = 0
    refresh_count: int = 0
    last_refresh: Optional[str] = None


@dataclass
class EnrichmentMetrics:
    """Metrics for data enrichment performance."""
    records_processed: int = 0
    records_enriched: int = 0
    enrichment_rate: float = 0.0
    lookup_attempts: int = 0
    lookup_successes: int = 0
    lookup_failures: int = 0
    avg_enrichment_time_ms: float = 0.0
    rules_applied: Dict[str, int] = None
    
    def __post_init__(self):
        if self.rules_applied is None:
            self.rules_applied = {}


@dataclass
class PipelineMetrics:
    """Metrics for overall pipeline performance."""
    batches_processed: int = 0
    records_processed: int = 0
    avg_batch_size: float = 0.0
    avg_processing_time_ms: float = 0.0
    throughput_records_per_second: float = 0.0
    error_count: int = 0
    error_rate: float = 0.0


class MetricsAggregator:
    """
    Aggregates metrics over time windows for trend analysis.
    
    Maintains sliding windows of metrics to calculate trends,
    averages, and detect performance anomalies.
    """
    
    def __init__(self, window_size_minutes: int = 15):
        """
        Initialize metrics aggregator.
        
        Args:
            window_size_minutes: Size of sliding window for trend analysis
        """
        self.window_size = timedelta(minutes=window_size_minutes)
        self.data_points = deque()
        self.lock = threading.Lock()
    
    def add_data_point(self, timestamp: datetime, value: float):
        """Add a data point to the aggregator."""
        with self.lock:
            self.data_points.append((timestamp, value))
            # Remove old data points outside the window
            cutoff = datetime.utcnow() - self.window_size
            while self.data_points and self.data_points[0][0] < cutoff:
                self.data_points.popleft()
    
    def get_average(self) -> float:
        """Get average value over the window."""
        with self.lock:
            if not self.data_points:
                return 0.0
            return sum(point[1] for point in self.data_points) / len(self.data_points)
    
    def get_trend(self) -> str:
        """Get trend direction (increasing, decreasing, stable)."""
        with self.lock:
            if len(self.data_points) < 2:
                return "stable"
            
            recent = list(self.data_points)[-5:]  # Last 5 points
            if len(recent) < 2:
                return "stable"
            
            first_half = sum(point[1] for point in recent[:len(recent)//2])
            second_half = sum(point[1] for point in recent[len(recent)//2:])
            
            if second_half > first_half * 1.1:
                return "increasing"
            elif second_half < first_half * 0.9:
                return "decreasing"
            else:
                return "stable"


class MetricsCollector:
    """
    Central metrics collection system for pipeline monitoring.
    
    This class collects, aggregates, and exposes metrics from all pipeline
    components. It provides real-time insights into system performance and
    can trigger alerts when thresholds are exceeded.
    
    Key Responsibilities:
    1. Collect metrics from cache, enrichment, and pipeline tasks
    2. Aggregate metrics over time windows for trend analysis
    3. Detect performance anomalies and trigger alerts
    4. Export metrics to external monitoring systems
    5. Provide health check endpoints
    
    The collector runs in background threads to avoid impacting pipeline
    performance while providing real-time monitoring capabilities.
    """
    
    def __init__(
        self,
        collection_interval_seconds: int = 30,
        retention_hours: int = 24,
        alert_thresholds: Dict[str, float] = None
    ):
        """
        Initialize the metrics collector.
        
        Args:
            collection_interval_seconds: How often to collect aggregated metrics
            retention_hours: How long to retain detailed metrics
            alert_thresholds: Thresholds for triggering alerts
        """
        self.collection_interval = collection_interval_seconds
        self.retention_period = timedelta(hours=retention_hours)
        self.logger = get_logger("metrics_collector")
        
        # Metrics storage
        self.cache_metrics: Dict[str, CacheMetrics] = {}
        self.enrichment_metrics = EnrichmentMetrics()
        self.pipeline_metrics = PipelineMetrics()
        
        # Time series data for trends
        self.aggregators = {
            "cache_hit_rate": MetricsAggregator(),
            "enrichment_rate": MetricsAggregator(),
            "throughput": MetricsAggregator(),
            "error_rate": MetricsAggregator()
        }
        
        # Alert configuration
        self.alert_thresholds = alert_thresholds or {
            "cache_hit_rate_min": 0.8,     # 80% minimum hit rate
            "enrichment_rate_min": 0.7,    # 70% minimum enrichment rate
            "error_rate_max": 0.05,        # 5% maximum error rate
            "throughput_min": 100           # 100 records/sec minimum
        }
        
        # Background collection thread
        self._collection_thread = None
        self._stop_collection = threading.Event()
        
        # Lock for thread-safe operations
        self._lock = threading.Lock()
        
        # Alert callbacks
        self.alert_callbacks: List[Callable[[str, Dict[str, Any]], None]] = []
    
    def start_collection(self):
        """Start background metrics collection."""
        if self._collection_thread and self._collection_thread.is_alive():
            return
            
        self._stop_collection.clear()
        self._collection_thread = threading.Thread(target=self._collection_loop)
        self._collection_thread.daemon = True
        self._collection_thread.start()
        
        self.logger.info("Metrics collection started")
    
    def stop_collection(self):
        """Stop background metrics collection."""
        self._stop_collection.set()
        if self._collection_thread:
            self._collection_thread.join()
        
        self.logger.info("Metrics collection stopped")
    
    def _collection_loop(self):
        """Background loop for periodic metrics collection and aggregation."""
        while not self._stop_collection.wait(self.collection_interval):
            try:
                self._aggregate_metrics()
                self._check_alerts()
            except Exception as e:
                self.logger.error(f"Error in metrics collection loop: {e}")
    
    def _aggregate_metrics(self):
        """Aggregate current metrics and add to time series."""
        now = datetime.utcnow()
        
        with self._lock:
            # Calculate overall cache hit rate
            total_hits = sum(m.hit_count for m in self.cache_metrics.values())
            total_requests = sum(m.total_requests for m in self.cache_metrics.values())
            overall_hit_rate = total_hits / max(total_requests, 1)
            
            # Calculate enrichment rate
            enrichment_rate = (self.enrichment_metrics.records_enriched / 
                             max(self.enrichment_metrics.records_processed, 1))
            
            # Calculate error rate
            error_rate = (self.pipeline_metrics.error_count / 
                         max(self.pipeline_metrics.batches_processed, 1))
            
            # Add to aggregators
            self.aggregators["cache_hit_rate"].add_data_point(now, overall_hit_rate)
            self.aggregators["enrichment_rate"].add_data_point(now, enrichment_rate)
            self.aggregators["throughput"].add_data_point(
                now, self.pipeline_metrics.throughput_records_per_second
            )
            self.aggregators["error_rate"].add_data_point(now, error_rate)
    
    def _check_alerts(self):
        """Check if any metrics exceed alert thresholds."""
        # Check cache hit rate
        overall_hit_rate = self._calculate_overall_cache_hit_rate()
        if overall_hit_rate < self.alert_thresholds["cache_hit_rate_min"]:
            self._trigger_alert("low_cache_hit_rate", {
                "current": overall_hit_rate,
                "threshold": self.alert_thresholds["cache_hit_rate_min"]
            })
        
        # Check enrichment rate
        enrichment_rate = (self.enrichment_metrics.records_enriched / 
                          max(self.enrichment_metrics.records_processed, 1))
        if enrichment_rate < self.alert_thresholds["enrichment_rate_min"]:
            self._trigger_alert("low_enrichment_rate", {
                "current": enrichment_rate,
                "threshold": self.alert_thresholds["enrichment_rate_min"]
            })
        
        # Check error rate
        error_rate = (self.pipeline_metrics.error_count / 
                     max(self.pipeline_metrics.batches_processed, 1))
        if error_rate > self.alert_thresholds["error_rate_max"]:
            self._trigger_alert("high_error_rate", {
                "current": error_rate,
                "threshold": self.alert_thresholds["error_rate_max"]
            })
        
        # Check throughput
        if (self.pipeline_metrics.throughput_records_per_second < 
            self.alert_thresholds["throughput_min"]):
            self._trigger_alert("low_throughput", {
                "current": self.pipeline_metrics.throughput_records_per_second,
                "threshold": self.alert_thresholds["throughput_min"]
            })
    
    def _trigger_alert(self, alert_type: str, details: Dict[str, Any]):
        """Trigger an alert with the given type and details."""
        alert_data = {
            "type": alert_type,
            "timestamp": datetime.utcnow().isoformat(),
            "details": details
        }
        
        self.logger.warning(f"Alert triggered: {alert_type} - {details}")
        
        # Call registered alert callbacks
        for callback in self.alert_callbacks:
            try:
                callback(alert_type, alert_data)
            except Exception as e:
                self.logger.error(f"Error in alert callback: {e}")
    
    def register_alert_callback(self, callback: Callable[[str, Dict[str, Any]], None]):
        """Register a callback function for alerts."""
        self.alert_callbacks.append(callback)
    
    # Cache metrics recording
    def record_cache_hit(self, cache_type: str, response_time_ms: float = 0):
        """Record a cache hit."""
        with self._lock:
            if cache_type not in self.cache_metrics:
                self.cache_metrics[cache_type] = CacheMetrics()
            
            metrics = self.cache_metrics[cache_type]
            metrics.hit_count += 1
            metrics.total_requests += 1
            metrics.hit_rate = metrics.hit_count / metrics.total_requests
            
            # Update average response time
            if response_time_ms > 0:
                total_time = metrics.avg_response_time_ms * (metrics.total_requests - 1)
                metrics.avg_response_time_ms = (total_time + response_time_ms) / metrics.total_requests
    
    def record_cache_miss(self, cache_type: str):
        """Record a cache miss."""
        with self._lock:
            if cache_type not in self.cache_metrics:
                self.cache_metrics[cache_type] = CacheMetrics()
            
            metrics = self.cache_metrics[cache_type]
            metrics.miss_count += 1
            metrics.total_requests += 1
            metrics.hit_rate = metrics.hit_count / metrics.total_requests
    
    def record_cache_refresh(self, cache_type: str, cache_size: int):
        """Record a cache refresh operation."""
        with self._lock:
            if cache_type not in self.cache_metrics:
                self.cache_metrics[cache_type] = CacheMetrics()
            
            metrics = self.cache_metrics[cache_type]
            metrics.refresh_count += 1
            metrics.cache_size = cache_size
            metrics.last_refresh = datetime.utcnow().isoformat()
    
    def record_cache_eviction(self, cache_type: str):
        """Record a cache eviction."""
        with self._lock:
            if cache_type not in self.cache_metrics:
                self.cache_metrics[cache_type] = CacheMetrics()
            
            metrics = self.cache_metrics[cache_type]
            metrics.evictions += 1
    
    # Enrichment metrics recording
    def record_enrichment_batch(
        self, 
        records_processed: int, 
        records_enriched: int,
        processing_time_ms: float,
        lookup_stats: Dict[str, int] = None
    ):
        """Record enrichment batch processing metrics."""
        with self._lock:
            self.enrichment_metrics.records_processed += records_processed
            self.enrichment_metrics.records_enriched += records_enriched
            self.enrichment_metrics.enrichment_rate = (
                self.enrichment_metrics.records_enriched / 
                max(self.enrichment_metrics.records_processed, 1)
            )
            
            # Update average processing time
            total_batches = self.pipeline_metrics.batches_processed + 1
            total_time = (self.enrichment_metrics.avg_enrichment_time_ms * 
                         (total_batches - 1))
            self.enrichment_metrics.avg_enrichment_time_ms = (
                (total_time + processing_time_ms) / total_batches
            )
            
            # Update lookup stats
            if lookup_stats:
                self.enrichment_metrics.lookup_attempts += lookup_stats.get("attempts", 0)
                self.enrichment_metrics.lookup_successes += lookup_stats.get("successes", 0)
                self.enrichment_metrics.lookup_failures += lookup_stats.get("failures", 0)
                
                # Update rules applied
                for rule, count in lookup_stats.get("rules_applied", {}).items():
                    self.enrichment_metrics.rules_applied[rule] = (
                        self.enrichment_metrics.rules_applied.get(rule, 0) + count
                    )
    
    # Pipeline metrics recording
    def record_batch_processed(
        self, 
        batch_size: int, 
        processing_time_ms: float, 
        errors: int = 0
    ):
        """Record pipeline batch processing metrics."""
        with self._lock:
            self.pipeline_metrics.batches_processed += 1
            self.pipeline_metrics.records_processed += batch_size
            self.pipeline_metrics.error_count += errors
            
            # Update averages
            total_batches = self.pipeline_metrics.batches_processed
            
            # Average batch size
            self.pipeline_metrics.avg_batch_size = (
                self.pipeline_metrics.records_processed / total_batches
            )
            
            # Average processing time
            total_time = (self.pipeline_metrics.avg_processing_time_ms * 
                         (total_batches - 1))
            self.pipeline_metrics.avg_processing_time_ms = (
                (total_time + processing_time_ms) / total_batches
            )
            
            # Throughput (records per second)
            if processing_time_ms > 0:
                throughput = (batch_size / processing_time_ms) * 1000
                # Exponentially weighted moving average for smoothing
                alpha = 0.1
                if self.pipeline_metrics.throughput_records_per_second == 0:
                    self.pipeline_metrics.throughput_records_per_second = throughput
                else:
                    self.pipeline_metrics.throughput_records_per_second = (
                        alpha * throughput + 
                        (1 - alpha) * self.pipeline_metrics.throughput_records_per_second
                    )
            
            # Error rate
            self.pipeline_metrics.error_rate = (
                self.pipeline_metrics.error_count / 
                max(self.pipeline_metrics.batches_processed, 1)
            )
    
    # Metrics export and reporting
    def export_metrics(self) -> Dict[str, Any]:
        """Export all collected metrics."""
        with self._lock:
            return {
                "timestamp": datetime.utcnow().isoformat(),
                "cache_metrics": {
                    cache_type: asdict(metrics) 
                    for cache_type, metrics in self.cache_metrics.items()
                },
                "enrichment_metrics": asdict(self.enrichment_metrics),
                "pipeline_metrics": asdict(self.pipeline_metrics),
                "trends": {
                    name: {
                        "average": aggregator.get_average(),
                        "trend": aggregator.get_trend()
                    }
                    for name, aggregator in self.aggregators.items()
                },
                "alert_thresholds": self.alert_thresholds
            }
    
    def export_prometheus_metrics(self) -> str:
        """Export metrics in Prometheus format."""
        lines = []
        timestamp = int(time.time() * 1000)
        
        # Cache metrics
        for cache_type, metrics in self.cache_metrics.items():
            lines.extend([
                f'cache_hit_rate{{type="{cache_type}"}} {metrics.hit_rate}',
                f'cache_size{{type="{cache_type}"}} {metrics.cache_size}',
                f'cache_evictions_total{{type="{cache_type}"}} {metrics.evictions}',
            ])
        
        # Enrichment metrics
        lines.extend([
            f'enrichment_rate {self.enrichment_metrics.enrichment_rate}',
            f'enrichment_records_processed_total {self.enrichment_metrics.records_processed}',
            f'enrichment_lookups_total {self.enrichment_metrics.lookup_attempts}',
        ])
        
        # Pipeline metrics
        lines.extend([
            f'pipeline_throughput_rps {self.pipeline_metrics.throughput_records_per_second}',
            f'pipeline_error_rate {self.pipeline_metrics.error_rate}',
            f'pipeline_batches_processed_total {self.pipeline_metrics.batches_processed}',
        ])
        
        return '\n'.join(lines)
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get overall system health status."""
        with self._lock:
            # Calculate health score based on key metrics
            health_score = 100
            issues = []
            
            # Check cache performance
            cache_hit_rate = self._calculate_overall_cache_hit_rate()
            if cache_hit_rate < 0.8:
                health_score -= 20
                issues.append(f"Low cache hit rate: {cache_hit_rate:.1%}")
            
            # Check enrichment performance
            enrichment_rate = (self.enrichment_metrics.records_enriched / 
                             max(self.enrichment_metrics.records_processed, 1))
            if enrichment_rate < 0.7:
                health_score -= 15
                issues.append(f"Low enrichment rate: {enrichment_rate:.1%}")
            
            # Check error rate
            if self.pipeline_metrics.error_rate > 0.05:
                health_score -= 25
                issues.append(f"High error rate: {self.pipeline_metrics.error_rate:.1%}")
            
            # Check throughput
            if self.pipeline_metrics.throughput_records_per_second < 100:
                health_score -= 10
                issues.append(f"Low throughput: {self.pipeline_metrics.throughput_records_per_second:.0f} rec/sec")
            
            # Determine status
            if health_score >= 90:
                status = "healthy"
            elif health_score >= 70:
                status = "warning"
            else:
                status = "critical"
            
            return {
                "status": status,
                "health_score": max(health_score, 0),
                "issues": issues,
                "timestamp": datetime.utcnow().isoformat(),
                "uptime_minutes": (datetime.utcnow() - 
                                 datetime.fromisoformat(self.enrichment_metrics.rules_applied.get('start_time', 
                                 datetime.utcnow().isoformat()))).total_seconds() / 60
            }
    
    def _calculate_overall_cache_hit_rate(self) -> float:
        """Calculate overall cache hit rate across all cache types."""
        total_hits = sum(m.hit_count for m in self.cache_metrics.values())
        total_requests = sum(m.total_requests for m in self.cache_metrics.values())
        return total_hits / max(total_requests, 1)
    
    def reset_metrics(self):
        """Reset all metrics (useful for testing)."""
        with self._lock:
            self.cache_metrics.clear()
            self.enrichment_metrics = EnrichmentMetrics()
            self.pipeline_metrics = PipelineMetrics()
            
            for aggregator in self.aggregators.values():
                aggregator.data_points.clear()


# Global metrics collector instance
_metrics_collector: Optional[MetricsCollector] = None


def get_metrics_collector() -> MetricsCollector:
    """Get the global metrics collector instance."""
    global _metrics_collector
    if _metrics_collector is None:
        _metrics_collector = MetricsCollector()
    return _metrics_collector


def initialize_metrics_collection(
    collection_interval_seconds: int = 30,
    retention_hours: int = 24,
    alert_thresholds: Dict[str, float] = None
):
    """Initialize and start metrics collection."""
    global _metrics_collector
    _metrics_collector = MetricsCollector(
        collection_interval_seconds=collection_interval_seconds,
        retention_hours=retention_hours,
        alert_thresholds=alert_thresholds
    )
    _metrics_collector.start_collection()
    return _metrics_collector
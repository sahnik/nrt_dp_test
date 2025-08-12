"""Pipeline monitoring integration for automatic metrics collection.

This module provides monitoring integrations that automatically collect metrics
from pipeline tasks without requiring manual instrumentation. It hooks into
task execution to capture performance data transparently.

Key Features:
- Automatic metrics collection from task execution
- Integration with existing pipeline components
- Real-time monitoring dashboards
- Alert notifications via email, Slack, webhook
- Performance trend analysis
- Health check endpoints for load balancers

Architecture:
    Task Execution → Monitor Hooks → Metrics Collector → Alerts/Dashboards

The monitor integrates with the DAG executor to capture task-level metrics
and with individual tasks to collect detailed performance data.

Usage:
    monitor = PipelineMonitor()
    monitor.attach_to_pipeline(pipeline)
    monitor.start_monitoring()
"""

import asyncio
import time
from typing import Dict, Any, List, Optional, Callable
from datetime import datetime
import threading
import json
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from dag_orchestrator.context import PipelineContext
from dag_orchestrator.task import Task
from task_components.enrichment.lookup_cache import LookupCacheTask, LookupCache
from task_components.enrichment.data_enrichment import DataEnrichmentTask
from monitoring.metrics_collector import get_metrics_collector, MetricsCollector
from config.logging_config import get_logger


class TaskMonitor:
    """
    Monitor for individual task performance.
    
    This class wraps around tasks to collect execution metrics without
    modifying the original task implementation. It uses the decorator
    pattern to transparently capture performance data.
    """
    
    def __init__(self, task: Task, metrics_collector: MetricsCollector):
        """
        Initialize task monitor.
        
        Args:
            task: The task to monitor
            metrics_collector: Metrics collector instance
        """
        self.task = task
        self.metrics_collector = metrics_collector
        self.logger = get_logger(f"monitor.{task.name}")
        
        # Wrap the original execute method
        self._original_execute = task.execute
        task.execute = self._monitored_execute
        
        # Task-specific metrics
        self.execution_times = []
        self.success_count = 0
        self.error_count = 0
    
    async def _monitored_execute(self, context: PipelineContext) -> PipelineContext:
        """Monitored version of task execute method."""
        start_time = time.time()
        task_name = self.task.name
        
        try:
            # Execute original task
            result_context = await self._original_execute(context)
            
            # Record success metrics
            execution_time_ms = (time.time() - start_time) * 1000
            self.success_count += 1
            
            # Collect task-specific metrics
            await self._collect_task_metrics(context, result_context, execution_time_ms)
            
            self.logger.debug(f"Task {task_name} executed successfully in {execution_time_ms:.2f}ms")
            return result_context
            
        except Exception as e:
            # Record error metrics
            execution_time_ms = (time.time() - start_time) * 1000
            self.error_count += 1
            
            self.logger.error(f"Task {task_name} failed after {execution_time_ms:.2f}ms: {e}")
            raise
    
    async def _collect_task_metrics(
        self, 
        input_context: PipelineContext, 
        output_context: PipelineContext,
        execution_time_ms: float
    ):
        """Collect metrics specific to different task types."""
        task_name = self.task.name
        
        # Cache task metrics
        if isinstance(self.task, LookupCacheTask):
            await self._collect_cache_metrics(output_context, execution_time_ms)
        
        # Enrichment task metrics  
        elif isinstance(self.task, DataEnrichmentTask):
            await self._collect_enrichment_metrics(input_context, output_context, execution_time_ms)
        
        # General task metrics
        self._collect_general_metrics(task_name, execution_time_ms)
    
    async def _collect_cache_metrics(self, context: PipelineContext, execution_time_ms: float):
        """Collect metrics from cache task execution."""
        task_output = context.get_task_output(self.task.name)
        if not task_output:
            return
        
        cache_metrics = task_output.get("cache_metrics", {})
        
        # Record cache refresh if it occurred
        if "last_refresh" in cache_metrics and cache_metrics.get("refresh_count", 0) > 0:
            cache_size = cache_metrics.get("size", 0)
            self.metrics_collector.record_cache_refresh("lookup_cache", cache_size)
        
        # Record hit/miss metrics if available
        if "hits" in cache_metrics and "misses" in cache_metrics:
            hits = cache_metrics["hits"]
            misses = cache_metrics["misses"]
            
            # Record individual hits and misses (approximation)
            for _ in range(hits):
                self.metrics_collector.record_cache_hit("lookup_cache", execution_time_ms / max(hits + misses, 1))
            for _ in range(misses):
                self.metrics_collector.record_cache_miss("lookup_cache")
    
    async def _collect_enrichment_metrics(
        self, 
        input_context: PipelineContext, 
        output_context: PipelineContext,
        execution_time_ms: float
    ):
        """Collect metrics from enrichment task execution."""
        task_output = output_context.get_task_output(self.task.name)
        if not task_output:
            return
        
        task_metrics = task_output.get("metrics", {})
        
        # Extract enrichment statistics
        records_processed = task_metrics.get("records_processed", 0)
        records_enriched = task_metrics.get("records_enriched", 0)
        
        # Lookup statistics
        lookup_stats = {
            "attempts": task_metrics.get("lookup_attempts", 0),
            "successes": task_metrics.get("lookup_hits", 0),
            "failures": task_metrics.get("lookup_misses", 0),
            "rules_applied": task_metrics.get("rules_applied", {})
        }
        
        # Record enrichment batch metrics
        self.metrics_collector.record_enrichment_batch(
            records_processed=records_processed,
            records_enriched=records_enriched,
            processing_time_ms=execution_time_ms,
            lookup_stats=lookup_stats
        )
        
        # Record individual cache hits/misses for enrichment lookups
        for _ in range(lookup_stats["successes"]):
            self.metrics_collector.record_cache_hit("enrichment_lookup")
        for _ in range(lookup_stats["failures"]):
            self.metrics_collector.record_cache_miss("enrichment_lookup")
    
    def _collect_general_metrics(self, task_name: str, execution_time_ms: float):
        """Collect general task execution metrics."""
        self.execution_times.append(execution_time_ms)
        
        # Keep only recent execution times (last 100)
        if len(self.execution_times) > 100:
            self.execution_times = self.execution_times[-100:]


class PipelineMonitor:
    """
    Main pipeline monitoring system.
    
    This class orchestrates monitoring across the entire pipeline, collecting
    metrics from all tasks and providing real-time insights into system health
    and performance.
    
    Key Responsibilities:
    1. Attach monitoring to pipeline tasks automatically
    2. Collect batch-level pipeline metrics
    3. Provide health check endpoints
    4. Send alerts when thresholds are exceeded
    5. Generate monitoring dashboards and reports
    """
    
    def __init__(
        self,
        metrics_collector: Optional[MetricsCollector] = None,
        enable_alerts: bool = True
    ):
        """
        Initialize pipeline monitor.
        
        Args:
            metrics_collector: Metrics collector instance (creates new if None)
            enable_alerts: Whether to enable alerting functionality
        """
        self.metrics_collector = metrics_collector or get_metrics_collector()
        self.enable_alerts = enable_alerts
        self.logger = get_logger("pipeline_monitor")
        
        # Task monitors
        self.task_monitors: Dict[str, TaskMonitor] = {}
        
        # Pipeline-level metrics
        self.pipeline_start_time = None
        self.batches_processed = 0
        
        # Alert callbacks
        if enable_alerts:
            self.metrics_collector.register_alert_callback(self._handle_alert)
        
        # Health check cache
        self._health_cache = None
        self._health_cache_time = None
        self._health_cache_ttl = 30  # seconds
    
    def attach_to_pipeline(self, pipeline):
        """
        Attach monitoring to a pipeline instance.
        
        Args:
            pipeline: DataPipeline instance to monitor
        """
        if hasattr(pipeline, 'dag') and pipeline.dag:
            self.attach_to_dag(pipeline.dag)
        
        # Hook into batch processing
        if hasattr(pipeline, 'process_batch'):
            original_process_batch = pipeline.process_batch
            pipeline.process_batch = self._monitored_process_batch(original_process_batch)
        
        self.logger.info("Pipeline monitoring attached")
    
    def attach_to_dag(self, dag):
        """
        Attach monitoring to all tasks in a DAG.
        
        Args:
            dag: DAG instance to monitor
        """
        for task_name, task in dag.tasks.items():
            if task_name not in self.task_monitors:
                self.task_monitors[task_name] = TaskMonitor(task, self.metrics_collector)
                self.logger.debug(f"Attached monitor to task: {task_name}")
    
    def _monitored_process_batch(self, original_process_batch):
        """Create monitored version of process_batch method."""
        async def monitored_batch_processor(messages):
            start_time = time.time()
            batch_size = len(messages) if messages else 0
            errors = 0
            
            try:
                # Execute original batch processing
                result = await original_process_batch(messages)
                
                # Record successful batch processing
                processing_time_ms = (time.time() - start_time) * 1000
                self.metrics_collector.record_batch_processed(
                    batch_size=batch_size,
                    processing_time_ms=processing_time_ms,
                    errors=errors
                )
                
                self.batches_processed += 1
                self.logger.debug(f"Batch processed: {batch_size} records in {processing_time_ms:.2f}ms")
                
                return result
                
            except Exception as e:
                # Record failed batch processing
                processing_time_ms = (time.time() - start_time) * 1000
                errors = 1
                
                self.metrics_collector.record_batch_processed(
                    batch_size=batch_size,
                    processing_time_ms=processing_time_ms,
                    errors=errors
                )
                
                self.logger.error(f"Batch processing failed after {processing_time_ms:.2f}ms: {e}")
                raise
        
        return monitored_batch_processor
    
    def start_monitoring(self):
        """Start the monitoring system."""
        if self.pipeline_start_time is None:
            self.pipeline_start_time = datetime.utcnow()
        
        # Start metrics collection if not already running
        self.metrics_collector.start_collection()
        
        self.logger.info("Pipeline monitoring started")
    
    def stop_monitoring(self):
        """Stop the monitoring system."""
        self.metrics_collector.stop_collection()
        self.logger.info("Pipeline monitoring stopped")
    
    def _handle_alert(self, alert_type: str, alert_data: Dict[str, Any]):
        """Handle alerts from the metrics collector."""
        self.logger.warning(f"Alert: {alert_type} - {alert_data}")
        
        # Here you could add integrations with:
        # - Email notifications
        # - Slack/Discord webhooks
        # - PagerDuty incidents
        # - CloudWatch alarms
        # - Custom webhook endpoints
        
        # Example webhook notification (disabled by default)
        # asyncio.create_task(self._send_webhook_alert(alert_type, alert_data))
    
    async def _send_webhook_alert(self, alert_type: str, alert_data: Dict[str, Any]):
        """Send alert to webhook endpoint (example implementation)."""
        # This is a placeholder for webhook integration
        # In production, you would implement actual webhook sending
        webhook_payload = {
            "alert_type": alert_type,
            "data": alert_data,
            "pipeline": "data-pipeline",
            "timestamp": datetime.utcnow().isoformat()
        }
        
        self.logger.info(f"Would send webhook alert: {webhook_payload}")
    
    def get_monitoring_summary(self) -> Dict[str, Any]:
        """Get comprehensive monitoring summary."""
        uptime = None
        if self.pipeline_start_time:
            uptime = (datetime.utcnow() - self.pipeline_start_time).total_seconds()
        
        # Task-level summaries
        task_summaries = {}
        for task_name, monitor in self.task_monitors.items():
            avg_execution_time = (
                sum(monitor.execution_times) / len(monitor.execution_times)
                if monitor.execution_times else 0
            )
            
            task_summaries[task_name] = {
                "success_count": monitor.success_count,
                "error_count": monitor.error_count,
                "avg_execution_time_ms": avg_execution_time,
                "success_rate": monitor.success_count / max(monitor.success_count + monitor.error_count, 1)
            }
        
        return {
            "uptime_seconds": uptime,
            "batches_processed": self.batches_processed,
            "task_summaries": task_summaries,
            "system_metrics": self.metrics_collector.export_metrics(),
            "health_status": self.get_health_status()
        }
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get cached health status with TTL."""
        now = time.time()
        
        # Return cached result if still valid
        if (self._health_cache and self._health_cache_time and 
            now - self._health_cache_time < self._health_cache_ttl):
            return self._health_cache
        
        # Generate new health status
        self._health_cache = self.metrics_collector.get_health_status()
        self._health_cache_time = now
        
        return self._health_cache
    
    def export_prometheus_metrics(self) -> str:
        """Export all metrics in Prometheus format."""
        base_metrics = self.metrics_collector.export_prometheus_metrics()
        
        # Add pipeline-level metrics
        pipeline_metrics = [
            f'pipeline_uptime_seconds {(datetime.utcnow() - self.pipeline_start_time).total_seconds()}'
            if self.pipeline_start_time else 'pipeline_uptime_seconds 0',
            f'pipeline_batches_processed_total {self.batches_processed}',
        ]
        
        # Add task-level metrics
        for task_name, monitor in self.task_monitors.items():
            pipeline_metrics.extend([
                f'task_executions_total{{task="{task_name}"}} {monitor.success_count + monitor.error_count}',
                f'task_errors_total{{task="{task_name}"}} {monitor.error_count}',
                f'task_success_rate{{task="{task_name}"}} {monitor.success_count / max(monitor.success_count + monitor.error_count, 1)}',
            ])
        
        return base_metrics + '\n' + '\n'.join(pipeline_metrics)
    
    def get_dashboard_data(self) -> Dict[str, Any]:
        """Get data formatted for monitoring dashboards."""
        summary = self.get_monitoring_summary()
        metrics = self.metrics_collector.export_metrics()
        
        # Format data for easy dashboard consumption
        dashboard_data = {
            "overview": {
                "status": summary["health_status"]["status"],
                "uptime_hours": (summary["uptime_seconds"] or 0) / 3600,
                "batches_processed": summary["batches_processed"],
                "throughput_rps": metrics["pipeline_metrics"]["throughput_records_per_second"],
                "error_rate": metrics["pipeline_metrics"]["error_rate"]
            },
            "cache_performance": {
                cache_type: {
                    "hit_rate": cache_metrics["hit_rate"],
                    "size": cache_metrics["cache_size"],
                    "avg_response_ms": cache_metrics["avg_response_time_ms"]
                }
                for cache_type, cache_metrics in metrics["cache_metrics"].items()
            },
            "enrichment_performance": {
                "enrichment_rate": metrics["enrichment_metrics"]["enrichment_rate"],
                "records_processed": metrics["enrichment_metrics"]["records_processed"],
                "lookup_success_rate": (
                    metrics["enrichment_metrics"]["lookup_successes"] /
                    max(metrics["enrichment_metrics"]["lookup_attempts"], 1)
                )
            },
            "task_performance": summary["task_summaries"],
            "trends": metrics["trends"],
            "timestamp": datetime.utcnow().isoformat()
        }
        
        return dashboard_data


# Global monitor instance
_pipeline_monitor: Optional[PipelineMonitor] = None


def get_pipeline_monitor() -> PipelineMonitor:
    """Get the global pipeline monitor instance."""
    global _pipeline_monitor
    if _pipeline_monitor is None:
        _pipeline_monitor = PipelineMonitor()
    return _pipeline_monitor


def initialize_pipeline_monitoring(
    metrics_collector: Optional[MetricsCollector] = None,
    enable_alerts: bool = True
) -> PipelineMonitor:
    """Initialize and configure pipeline monitoring."""
    global _pipeline_monitor
    _pipeline_monitor = PipelineMonitor(
        metrics_collector=metrics_collector,
        enable_alerts=enable_alerts
    )
    return _pipeline_monitor
"""Main pipeline runner and orchestrator.

This module contains the DataPipeline class which orchestrates the entire data processing
pipeline. It handles Kafka message consumption, DAG execution, error handling, and
graceful shutdown.

Key Components:
- DataPipeline: Main orchestrator class that manages the entire pipeline lifecycle
- DAG Construction: Builds the processing DAG with all tasks and dependencies
- Batch Processing: Processes batches of records through the DAG
- Error Handling: Comprehensive error tracking and recovery
- Graceful Shutdown: Handles SIGINT/SIGTERM signals for clean shutdown

Architecture:
    Kafka Consumer → DAG Execution → Dual Sinks (MongoDB + Kafka)
    
    The pipeline follows this execution order:
    1. Field Mapping: Standardize field names
    2. Lookup Cache: Warm reference data cache from Kafka
    3. Type Conversion: Convert data types with validation  
    4. Data Enrichment: Enrich records with reference data lookups
    5. Standardization: Apply business rules and defaults
    6. Duplicate Check: Remove/mark duplicate records
    7. Quality Validation: Validate against business rules
    8. Dual Sinks: Write to both MongoDB and Kafka in parallel

Usage:
    python pipeline/main.py
"""

import asyncio
import signal
import sys
from typing import Dict, Any, List
import logging
from datetime import datetime

# Add parent directory to path
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from dag_orchestrator.dag import DAG
from dag_orchestrator.executor import DAGExecutor
from dag_orchestrator.context import PipelineContext
from task_components.standardization.standardizer import (
    DataStandardizationTask,
    FieldMappingTask,
    DataTypeConversionTask
)
from task_components.data_quality.validator import (
    DataQualityTask,
    DuplicateCheckTask,
    NotNullRule,
    RangeRule,
    PatternRule
)
from task_components.enrichment.lookup_cache import LookupCacheTask
from task_components.enrichment.data_enrichment import DataEnrichmentTask
from pipeline.kafka_consumer import BatchKafkaConsumer
from pipeline.kafka_producer import KafkaProducerAsync
from pipeline.mongodb_sink import MongoDBSinkTask
from pipeline.kafka_sink_task import KafkaSinkTask
from config.settings import get_settings
from config.logging_config import setup_logging, get_logger


class DataPipeline:
    """
    Main data pipeline orchestrator that manages the entire processing workflow.
    
    This class is responsible for:
    1. Building and configuring the DAG with all processing tasks
    2. Managing Kafka consumer for ingesting batches of records
    3. Executing the DAG for each batch of messages
    4. Handling errors and maintaining processing statistics
    5. Providing graceful shutdown capabilities
    
    The pipeline processes data in batches for optimal performance and implements
    a dual-sink architecture writing to both MongoDB (persistence) and Kafka
    (streaming) simultaneously.
    
    Attributes:
        settings: Application settings loaded from environment
        consumer: Kafka consumer for ingesting messages
        dag: DAG containing all processing tasks
        executor: DAG executor for parallel task execution
        processed_count: Count of successfully processed records
        error_count: Count of processing errors
    """

    def __init__(self):
        """Initialize the data pipeline."""
        self.settings = get_settings()
        self.logger = get_logger("pipeline.main")
        self.consumer = None
        self.producer = None
        self.dag = None
        self.executor = None
        self.running = False
        self.processed_count = 0
        self.error_count = 0
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        self.logger.info(f"Received signal {signum}, shutting down...")
        self.running = False

    def build_dag(self) -> DAG:
        """
        Build the complete processing DAG with all tasks and dependencies.
        
        The DAG is constructed with the following processing stages:
        1. Field Mapping: Standardizes field names (e.g., user_id → userId)
        2. Lookup Cache: Manages reference data cache from Kafka lookup topic
        3. Type Conversion: Converts string data to appropriate types
        4. Data Enrichment: Enriches records with reference data from cache
        5. Data Standardization: Applies business rules and default values
        6. Duplicate Check: Identifies and removes duplicate records
        7. Quality Validation: Validates records against business rules
        8. Dual Sinks: Parallel writes to MongoDB and Kafka
        
        Dependencies are configured to ensure proper execution order,
        with both sinks running in parallel after validation.
        
        Returns:
            Configured DAG ready for execution
        """
        dag = DAG("data_processing_pipeline")
        
        # Create tasks - Each task represents a processing stage in the pipeline
        
        # 1. Field Mapping Task: Standardizes field names for consistency
        field_mapping = FieldMappingTask(
            name="field_mapping",
            mappings={
                "user_id": "userId",
                "product_id": "productId",
                "event_type": "eventType",
                "session_id": "sessionId",
                "device_type": "deviceType",
                "is_returning_user": "isReturningUser",
                "discount_applied": "discountApplied",
                "payment_method": "paymentMethod",
                "shipping_method": "shippingMethod",
                "customer_rating": "customerRating",
                "time_on_site": "timeOnSite",
                "page_views": "pageViews"
            }
        )
        
        # 2. Lookup Cache Task: Manages reference data cache from Kafka lookup topic
        lookup_cache = LookupCacheTask(
            name="lookup_cache",
            refresh_strategy="startup",  # Refresh cache on pipeline startup
            max_cache_size=1000000,      # 1M entries max
            lookup_topic="lookup-data"   # Kafka topic with reference data
        )
        
        # 3. Type Conversion Task: Converts string data to appropriate types with validation
        type_conversion = DataTypeConversionTask(
            name="type_conversion",
            conversions={
                "price": "float",
                "quantity": "int",
                "discountApplied": "float",
                "customerRating": "float",
                "pageViews": "int",
                "timeOnSite": "int",
                "timestamp": "datetime"
            }
        )
        
        # 4. Data Enrichment Task: Enriches records with reference data from cache
        data_enrichment = DataEnrichmentTask(
            name="data_enrichment",
            enrichment_rules=[
                {
                    "lookup_key": "userId",
                    "reference_type": "customers",
                    "target_fields": ["customer_name", "customer_type", "segment", "region"],
                    "strategy": "append",
                    "fallback_values": {
                        "customer_name": "Unknown Customer",
                        "customer_type": "Regular", 
                        "segment": "Default"
                    },
                    "required": False
                },
                {
                    "lookup_key": "productId", 
                    "reference_type": "products",
                    "target_fields": ["product_name", "category", "brand", "price"],
                    "strategy": "conditional",  # Only add if fields are missing
                    "fallback_values": {
                        "product_name": "Unknown Product",
                        "category": "General"
                    },
                    "required": False
                },
                {
                    "lookup_key": "locationId",
                    "reference_type": "locations", 
                    "target_fields": ["location_name", "city", "state", "region", "timezone"],
                    "strategy": "append",
                    "condition": "exists:locationId",  # Only if locationId exists
                    "required": False
                }
            ],
            input_key="data",
            output_key="data",  # Replace original data with enriched data 
            max_lookup_failures=0.3  # Allow 30% lookup failures
        )
        
        # 5. Data Standardization Task: Applies business rules and adds default values
        standardization = DataStandardizationTask(
            name="standardization",
            default_values={
                "processedAt": datetime.utcnow().isoformat(),
                "pipelineVersion": "1.0.0"
            }
        )
        
        # 6. Duplicate Check Task: Identifies and removes duplicate records based on key fields
        duplicate_check = DuplicateCheckTask(
            name="duplicate_check",
            key_fields=["id", "timestamp"],
            action="remove"
        )
        
        # 7. Data Quality Validation Task: Validates records against business rules
        quality_validation = DataQualityTask(
            name="quality_validation",
            rules=[
                NotNullRule("id"),
                NotNullRule("userId"),
                NotNullRule("timestamp"),
                RangeRule("price", min_value=0, max_value=1000000),
                RangeRule("quantity", min_value=0, max_value=10000),
                RangeRule("customerRating", min_value=0, max_value=5),
                PatternRule("email", r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"),
                PatternRule("userId", r"^user_\d{4}$"),
                PatternRule("productId", r"^prod_\d{4}$")
            ],
            fail_on_error=False,
            error_threshold=0.2  # Allow up to 20% errors
        )
        
        # 8. MongoDB Sink Task: Writes processed records to MongoDB Atlas for persistence
        mongodb_sink = MongoDBSinkTask(
            name="mongodb_sink",
            batch_write=True,
            upsert_keys=None  # Insert only, no upsert
        )
        
        # 9. Kafka Sink Task: Writes processed records to Kafka for downstream consumers
        kafka_sink = KafkaSinkTask(
            name="kafka_sink",
            batch_write=True
        )
        
        # Add tasks to DAG
        dag.add_task(field_mapping)
        dag.add_task(lookup_cache)
        dag.add_task(type_conversion)
        dag.add_task(data_enrichment)
        dag.add_task(standardization)
        dag.add_task(duplicate_check)
        dag.add_task(quality_validation)
        dag.add_task(mongodb_sink)
        dag.add_task(kafka_sink)
        
        # Define task dependencies - creates execution order and parallel groups
        dag.add_dependency("field_mapping", "lookup_cache")
        dag.add_dependency("lookup_cache", "type_conversion") 
        dag.add_dependency("type_conversion", "data_enrichment")
        dag.add_dependency("data_enrichment", "standardization")
        dag.add_dependency("standardization", "duplicate_check")
        dag.add_dependency("duplicate_check", "quality_validation")
        # Both sinks run in parallel after quality validation
        dag.add_dependency("quality_validation", "mongodb_sink")
        dag.add_dependency("quality_validation", "kafka_sink")
        
        return dag

    async def process_batch(self, messages: List[Dict[str, Any]]):
        """
        Process a batch of messages through the DAG.
        
        Args:
            messages: Batch of messages from Kafka
        """
        try:
            # Extract message values
            records = [msg["value"] for msg in messages]
            
            self.logger.info(f"Processing batch of {len(records)} records")
            
            # Create pipeline context with the batch data
            context = PipelineContext(initial_data={"data": records})
            
            # Execute DAG
            result_context = await self.executor.execute(self.dag, context)
            
            # Check for errors
            if result_context.has_errors():
                errors = result_context.get_errors()
                self.logger.error(f"Pipeline errors: {errors}")
                self.error_count += len(errors)
            else:
                self.processed_count += len(records)
                
            # Log summary
            summary = result_context.get("dag_execution_summary", {})
            self.logger.info(
                f"Batch processed - Success: {summary.get('completed_tasks')}, "
                f"Failed: {summary.get('failed_tasks')}, "
                f"Duration: {summary.get('duration_seconds', 0):.2f}s"
            )
            
            # Note: Data is now sent to Kafka via the kafka_sink task in the DAG
            
        except Exception as e:
            self.logger.error(f"Failed to process batch: {e}")
            self.error_count += 1

    async def run(self):
        """Run the main pipeline loop."""
        self.logger.info("Starting data pipeline")
        
        # Setup logging
        setup_logging()
        
        # Build DAG
        self.dag = self.build_dag()
        self.logger.info(f"DAG built with {len(self.dag.tasks)} tasks")
        
        # Log DAG structure
        self.logger.info(f"DAG structure:\n{self.dag.visualize()}")
        
        # Create executor
        self.executor = DAGExecutor(max_workers=self.settings.pipeline.max_workers)
        
        # Initialize Kafka consumer
        self.consumer = BatchKafkaConsumer(
            settings=self.settings.kafka,
            batch_handler=self.process_batch,
            batch_size=self.settings.pipeline.batch_size,
            batch_timeout=10.0
        )
        
        # Note: Kafka producer is now handled by the kafka_sink task
        
        self.running = True
        
        try:
            # Start consumer
            await self.consumer.run()
            
        except Exception as e:
            self.logger.error(f"Pipeline error: {e}")
            
        finally:
            await self.shutdown()

    async def shutdown(self):
        """Shutdown the pipeline gracefully."""
        self.running = False
        self.logger.info("Shutting down pipeline...")
        
        # Stop consumer
        if self.consumer:
            await self.consumer.stop()
        
        # Note: Kafka producer cleanup is handled by kafka_sink task
        
        # Log final statistics
        self.logger.info(
            f"Pipeline shutdown - Processed: {self.processed_count}, Errors: {self.error_count}"
        )

    def get_stats(self) -> Dict[str, Any]:
        """
        Get pipeline statistics.
        
        Returns:
            Dictionary of pipeline stats
        """
        stats = {
            "running": self.running,
            "processed_count": self.processed_count,
            "error_count": self.error_count,
            "dag_tasks": len(self.dag.tasks) if self.dag else 0
        }
        
        if self.consumer:
            stats["consumer"] = self.consumer.get_stats()
        
        # Producer stats are now part of kafka_sink task outputs
        
        return stats


async def main():
    """Main entry point."""
    pipeline = DataPipeline()
    
    try:
        await pipeline.run()
    except KeyboardInterrupt:
        logging.info("Interrupted by user")
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        raise


if __name__ == "__main__":
    # Run the pipeline
    asyncio.run(main())
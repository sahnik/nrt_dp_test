"""Kafka sink task for the pipeline."""

import json
from typing import Dict, Any, Optional
import pandas as pd

import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from dag_orchestrator.task import Task
from dag_orchestrator.context import PipelineContext
from pipeline.kafka_producer import KafkaProducerAsync
from config.settings import get_settings, KafkaSettings
from config.logging_config import get_logger


class KafkaSinkTask(Task):
    """
    Task for writing data to Kafka sink topic.
    """

    def __init__(
        self,
        name: str = "kafka_sink",
        settings: Optional[KafkaSettings] = None,
        batch_write: bool = True,
        retries: int = 3,
        retry_delay: float = 1.0
    ):
        """
        Initialize the Kafka sink task.
        
        Args:
            name: Task name
            settings: Kafka settings
            batch_write: Whether to use batch writing
            retries: Number of retry attempts
            retry_delay: Delay between retries
        """
        super().__init__(name, retries, retry_delay)
        self.settings = settings or get_settings().kafka
        self.batch_write = batch_write
        self.producer = None
        self.logger = get_logger(f"task.{name}")

    async def execute(self, context: PipelineContext) -> PipelineContext:
        """
        Execute the Kafka sink task.
        
        Args:
            context: Pipeline context containing data
            
        Returns:
            Updated context with write results
        """
        # Get clean data from context (processed by data quality task)
        data = context.get("clean_data")
        
        if data is None:
            # Fall back to regular data if clean_data is not available
            data = context.get("data")
        
        if data is None:
            self.logger.warning("No data found in context")
            return context
        
        # Initialize producer if not already done
        if self.producer is None:
            self.producer = KafkaProducerAsync(self.settings)
        
        try:
            # Convert data to list of records
            if isinstance(data, pd.DataFrame):
                records = data.to_dict('records')
            elif isinstance(data, list):
                records = data
            elif isinstance(data, dict):
                records = [data]
            else:
                self.logger.error(f"Unsupported data type: {type(data)}")
                context.add_error(self.name, f"Unsupported data type: {type(data)}")
                return context
            
            # Prepare records for Kafka (ensure JSON serializable)
            kafka_records = []
            keys = []
            
            for record in records:
                # Create a copy to avoid modifying original
                kafka_record = dict(record)
                
                # Convert any non-JSON serializable types
                for key, value in kafka_record.items():
                    if pd.isna(value):
                        kafka_record[key] = None
                    elif hasattr(value, 'isoformat'):  # datetime objects
                        kafka_record[key] = value.isoformat()
                    elif isinstance(value, pd.Timestamp):
                        kafka_record[key] = str(value) if not pd.isna(value) else None
                
                # Add processing metadata
                kafka_record["_processed_at"] = context.get_metadata().get("created_at").isoformat()
                kafka_record["_pipeline_version"] = "1.0.0"
                kafka_record["_sink_type"] = "kafka"
                
                kafka_records.append(kafka_record)
                
                # Use record ID as key, or generate one
                key = record.get("id") or record.get("userId") or f"rec_{len(keys)+1}"
                keys.append(key)
            
            # Write to Kafka
            if self.batch_write and len(kafka_records) > 1:
                # Batch write mode
                results = await self.producer.produce_batch(kafka_records, keys)
                
                # Count successful deliveries
                successful = sum(1 for r in results if not isinstance(r, Exception))
                failed = len(results) - successful
                
                context.add_task_output(self.name, {
                    "records_sent": successful,
                    "records_failed": failed,
                    "topic": self.settings.topic_sink,
                    "mode": "batch"
                })
                
                if failed > 0:
                    context.add_warning(self.name, f"{failed} records failed to send to Kafka")
                
            else:
                # Single record writes
                successful = 0
                failed = 0
                
                for i, record in enumerate(kafka_records):
                    try:
                        await self.producer.produce(record, keys[i])
                        successful += 1
                    except Exception as e:
                        self.logger.error(f"Failed to send record {i}: {e}")
                        failed += 1
                
                context.add_task_output(self.name, {
                    "records_sent": successful,
                    "records_failed": failed,
                    "topic": self.settings.topic_sink,
                    "mode": "single"
                })
            
            self.logger.info(f"Successfully sent {successful} records to Kafka topic: {self.settings.topic_sink}")
            
        except Exception as e:
            self.logger.error(f"Failed to write to Kafka: {e}")
            context.add_error(self.name, str(e))
            raise
        
        return context

    async def cleanup(self):
        """Cleanup resources."""
        if self.producer:
            await self.producer.close()
            self.producer = None
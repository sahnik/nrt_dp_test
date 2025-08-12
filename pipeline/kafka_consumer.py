"""Kafka consumer for the pipeline."""

import json
import asyncio
from typing import Optional, Callable, Dict, Any, List
from confluent_kafka import Consumer, KafkaError, KafkaException
import logging
from datetime import datetime

from config.settings import get_settings, KafkaSettings
from config.logging_config import get_logger


class KafkaConsumerAsync:
    """
    Asynchronous Kafka consumer for pipeline data ingestion.
    """

    def __init__(
        self,
        settings: Optional[KafkaSettings] = None,
        message_handler: Optional[Callable] = None
    ):
        """
        Initialize the Kafka consumer.
        
        Args:
            settings: Kafka settings (uses default if not provided)
            message_handler: Async function to handle messages
        """
        self.settings = settings or get_settings().kafka
        self.message_handler = message_handler
        self.logger = get_logger("kafka_consumer")
        self.consumer = None
        self.running = False
        self.messages_consumed = 0
        self.errors = 0
        self._consumer_task = None

    def _init_consumer(self):
        """Initialize Kafka consumer."""
        try:
            config = self.settings.get_consumer_config()
            self.consumer = Consumer(config)
            self.consumer.subscribe([self.settings.topic_source])
            self.logger.info(
                f"Kafka consumer initialized - Topic: {self.settings.topic_source}, "
                f"Group: {self.settings.consumer_group}"
            )
        except Exception as e:
            self.logger.error(f"Failed to initialize Kafka consumer: {e}")
            raise

    async def consume_messages(self, batch_size: int = 1, timeout: float = 1.0) -> List[Dict[str, Any]]:
        """
        Consume messages from Kafka in batches.
        
        Args:
            batch_size: Number of messages to consume in a batch
            timeout: Timeout for polling in seconds
            
        Returns:
            List of consumed messages
        """
        messages = []
        
        while len(messages) < batch_size and self.running:
            msg = await asyncio.get_event_loop().run_in_executor(
                None, self.consumer.poll, timeout
            )
            
            if msg is None:
                continue
                
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    self.logger.debug(f"Reached end of partition {msg.partition()}")
                else:
                    self.logger.error(f"Consumer error: {msg.error()}")
                    self.errors += 1
                continue
            
            try:
                # Decode message
                key = msg.key().decode('utf-8') if msg.key() else None
                value = json.loads(msg.value().decode('utf-8'))
                
                message_data = {
                    "key": key,
                    "value": value,
                    "topic": msg.topic(),
                    "partition": msg.partition(),
                    "offset": msg.offset(),
                    "timestamp": msg.timestamp()[1] if msg.timestamp()[0] != -1 else None,
                    "headers": msg.headers() if msg.headers() else []
                }
                
                messages.append(message_data)
                self.messages_consumed += 1
                
            except json.JSONDecodeError as e:
                self.logger.error(f"Failed to decode message: {e}")
                self.errors += 1
            except Exception as e:
                self.logger.error(f"Error processing message: {e}")
                self.errors += 1
        
        return messages

    async def process_message(self, message: Dict[str, Any]) -> bool:
        """
        Process a single message using the configured handler.
        
        Args:
            message: Message to process
            
        Returns:
            True if processing succeeded
        """
        try:
            if self.message_handler:
                await self.message_handler(message)
            else:
                self.logger.debug(f"No message handler configured, message: {message['key']}")
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to process message {message.get('key')}: {e}")
            return False

    async def run(self, batch_size: int = 1):
        """
        Run the consumer loop.
        
        Args:
            batch_size: Number of messages to process in a batch
        """
        self.logger.info(f"Starting Kafka consumer - Batch size: {batch_size}")
        
        # Initialize consumer
        if not self.consumer:
            self._init_consumer()
        
        self.running = True
        consecutive_errors = 0
        max_consecutive_errors = 10
        
        try:
            while self.running:
                try:
                    # Consume messages in batch
                    messages = await self.consume_messages(batch_size=batch_size)
                    
                    if messages:
                        self.logger.debug(f"Consumed {len(messages)} messages")
                        
                        # Process messages
                        if batch_size == 1 and messages:
                            # Process single message
                            success = await self.process_message(messages[0])
                            if success:
                                consecutive_errors = 0
                        else:
                            # Process batch
                            tasks = [self.process_message(msg) for msg in messages]
                            results = await asyncio.gather(*tasks, return_exceptions=True)
                            
                            # Check for failures
                            failures = sum(1 for r in results if isinstance(r, Exception) or not r)
                            if failures > 0:
                                self.logger.warning(f"Failed to process {failures}/{len(messages)} messages")
                            else:
                                consecutive_errors = 0
                        
                        # Commit offsets if auto-commit is disabled
                        if not self.settings.enable_auto_commit:
                            await self.commit_offsets()
                    
                    # Log progress
                    if self.messages_consumed > 0 and self.messages_consumed % 100 == 0:
                        self.logger.info(
                            f"Progress - Messages: {self.messages_consumed}, Errors: {self.errors}"
                        )
                    
                except KafkaException as e:
                    self.logger.error(f"Kafka exception: {e}")
                    consecutive_errors += 1
                    
                    if consecutive_errors >= max_consecutive_errors:
                        self.logger.error(f"Too many consecutive errors ({consecutive_errors}), stopping consumer")
                        break
                    
                    # Exponential backoff
                    await asyncio.sleep(min(2 ** consecutive_errors, 60))
                    
                except Exception as e:
                    self.logger.error(f"Unexpected error in consumer loop: {e}")
                    consecutive_errors += 1
                    
                    if consecutive_errors >= max_consecutive_errors:
                        break
                    
                    await asyncio.sleep(1)
        
        finally:
            await self.stop()

    async def commit_offsets(self):
        """Commit consumed message offsets."""
        try:
            await asyncio.get_event_loop().run_in_executor(
                None, self.consumer.commit, None, True
            )
            self.logger.debug("Offsets committed")
        except Exception as e:
            self.logger.error(f"Failed to commit offsets: {e}")

    async def stop(self):
        """Stop the consumer and cleanup."""
        self.running = False
        
        if self.consumer:
            try:
                # Commit final offsets
                if not self.settings.enable_auto_commit:
                    await self.commit_offsets()
                
                # Close consumer
                await asyncio.get_event_loop().run_in_executor(
                    None, self.consumer.close
                )
                self.logger.info("Kafka consumer closed")
                
            except Exception as e:
                self.logger.error(f"Error closing consumer: {e}")
        
        self.logger.info(
            f"Consumer stopped - Messages consumed: {self.messages_consumed}, Errors: {self.errors}"
        )

    async def start(self, batch_size: int = 1):
        """
        Start the consumer as an async task.
        
        Args:
            batch_size: Number of messages to process in a batch
            
        Returns:
            Async task for the consumer
        """
        if not self._consumer_task or self._consumer_task.done():
            self._consumer_task = asyncio.create_task(self.run(batch_size))
        return self._consumer_task

    def get_stats(self) -> Dict[str, Any]:
        """
        Get consumer statistics.
        
        Returns:
            Dictionary of consumer stats
        """
        return {
            "messages_consumed": self.messages_consumed,
            "errors": self.errors,
            "running": self.running,
            "topic": self.settings.topic_source,
            "consumer_group": self.settings.consumer_group
        }


class BatchKafkaConsumer(KafkaConsumerAsync):
    """
    Kafka consumer optimized for batch processing.
    """

    def __init__(
        self,
        settings: Optional[KafkaSettings] = None,
        batch_handler: Optional[Callable] = None,
        batch_size: int = 100,
        batch_timeout: float = 10.0
    ):
        """
        Initialize batch Kafka consumer.
        
        Args:
            settings: Kafka settings
            batch_handler: Async function to handle message batches
            batch_size: Maximum batch size
            batch_timeout: Maximum time to wait for a full batch
        """
        super().__init__(settings)
        self.batch_handler = batch_handler
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.current_batch = []
        self.batch_start_time = None

    async def process_batch(self, messages: List[Dict[str, Any]]) -> bool:
        """
        Process a batch of messages.
        
        Args:
            messages: Batch of messages to process
            
        Returns:
            True if processing succeeded
        """
        try:
            if self.batch_handler:
                await self.batch_handler(messages)
                self.logger.info(f"Processed batch of {len(messages)} messages")
            return True
        except Exception as e:
            self.logger.error(f"Failed to process batch: {e}")
            return False

    async def run(self, **kwargs):
        """Run the batch consumer."""
        self.logger.info(
            f"Starting batch consumer - Batch size: {self.batch_size}, "
            f"Timeout: {self.batch_timeout}s"
        )
        
        # Initialize consumer
        if not self.consumer:
            self._init_consumer()
        
        self.running = True
        self.current_batch = []
        self.batch_start_time = datetime.utcnow()
        
        try:
            while self.running:
                # Consume a single message
                messages = await self.consume_messages(batch_size=1, timeout=0.1)
                
                if messages:
                    self.current_batch.extend(messages)
                
                # Check if batch is ready
                batch_elapsed = (datetime.utcnow() - self.batch_start_time).total_seconds()
                
                if (len(self.current_batch) >= self.batch_size or 
                    (self.current_batch and batch_elapsed >= self.batch_timeout)):
                    
                    # Process the batch
                    success = await self.process_batch(self.current_batch)
                    
                    if success and not self.settings.enable_auto_commit:
                        await self.commit_offsets()
                    
                    # Reset batch
                    self.current_batch = []
                    self.batch_start_time = datetime.utcnow()
        
        finally:
            # Process any remaining messages
            if self.current_batch:
                await self.process_batch(self.current_batch)
                if not self.settings.enable_auto_commit:
                    await self.commit_offsets()
            
            await self.stop()
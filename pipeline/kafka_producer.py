"""Kafka producer for the pipeline sink."""

import json
import asyncio
from typing import Optional, Dict, Any, List
from confluent_kafka import Producer
import logging

from config.settings import get_settings, KafkaSettings
from config.logging_config import get_logger


class KafkaProducerAsync:
    """
    Asynchronous Kafka producer for pipeline data output.
    """

    def __init__(self, settings: Optional[KafkaSettings] = None):
        """
        Initialize the Kafka producer.
        
        Args:
            settings: Kafka settings (uses default if not provided)
        """
        self.settings = settings or get_settings().kafka
        self.logger = get_logger("kafka_producer")
        self.producer = None
        self.messages_produced = 0
        self.errors = 0
        self._delivery_futures = {}

    def _init_producer(self):
        """Initialize Kafka producer."""
        try:
            config = self.settings.get_producer_config()
            self.producer = Producer(config)
            self.logger.info(f"Kafka producer initialized: {self.settings.bootstrap_servers}")
        except Exception as e:
            self.logger.error(f"Failed to initialize Kafka producer: {e}")
            raise

    def _delivery_callback(self, err, msg, future):
        """
        Delivery callback for produced messages.
        
        Args:
            err: Error if delivery failed
            msg: Message that was delivered or failed
            future: Future to complete with result
        """
        if err is not None:
            self.logger.error(f"Message delivery failed: {err}")
            self.errors += 1
            future.set_exception(Exception(f"Delivery failed: {err}"))
        else:
            self.logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}] @ {msg.offset()}")
            self.messages_produced += 1
            future.set_result({
                "topic": msg.topic(),
                "partition": msg.partition(),
                "offset": msg.offset()
            })

    async def produce(
        self,
        message: Dict[str, Any],
        key: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Produce a single message to Kafka.
        
        Args:
            message: Message to produce (will be JSON serialized)
            key: Optional message key
            headers: Optional message headers
            
        Returns:
            Delivery result with topic, partition, and offset
        """
        if not self.producer:
            self._init_producer()
        
        try:
            # Serialize message
            value = json.dumps(message).encode('utf-8')
            key_bytes = key.encode('utf-8') if key else None
            
            # Convert headers to list of tuples
            header_list = [(k, v.encode('utf-8')) for k, v in headers.items()] if headers else None
            
            # Create future for async delivery
            future = asyncio.Future()
            
            # Produce message
            self.producer.produce(
                topic=self.settings.topic_sink,
                key=key_bytes,
                value=value,
                headers=header_list,
                callback=lambda err, msg: self._delivery_callback(err, msg, future)
            )
            
            # Trigger delivery
            self.producer.poll(0)
            
            # Wait for delivery
            result = await future
            return result
            
        except Exception as e:
            self.logger.error(f"Failed to produce message: {e}")
            self.errors += 1
            raise

    async def produce_batch(
        self,
        messages: List[Dict[str, Any]],
        keys: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """
        Produce a batch of messages to Kafka.
        
        Args:
            messages: List of messages to produce
            keys: Optional list of message keys (must match messages length)
            
        Returns:
            List of delivery results
        """
        if not self.producer:
            self._init_producer()
        
        if keys and len(keys) != len(messages):
            raise ValueError("Keys list must match messages list length")
        
        futures = []
        
        try:
            for i, message in enumerate(messages):
                # Serialize message
                value = json.dumps(message).encode('utf-8')
                key = keys[i].encode('utf-8') if keys and keys[i] else None
                
                # Create future for async delivery
                future = asyncio.Future()
                futures.append(future)
                
                # Produce message
                self.producer.produce(
                    topic=self.settings.topic_sink,
                    key=key,
                    value=value,
                    callback=lambda err, msg, f=future: self._delivery_callback(err, msg, f)
                )
            
            # Trigger delivery for all messages
            self.producer.poll(0)
            
            # Wait for all deliveries
            results = await asyncio.gather(*futures, return_exceptions=True)
            
            # Count successes and failures
            successes = sum(1 for r in results if not isinstance(r, Exception))
            failures = len(results) - successes
            
            if failures > 0:
                self.logger.warning(f"Batch production: {successes} succeeded, {failures} failed")
            else:
                self.logger.info(f"Successfully produced batch of {len(messages)} messages")
            
            return results
            
        except Exception as e:
            self.logger.error(f"Failed to produce batch: {e}")
            raise

    async def flush(self, timeout: float = 10.0):
        """
        Flush any pending messages.
        
        Args:
            timeout: Maximum time to wait for flush in seconds
        """
        if self.producer:
            try:
                await asyncio.get_event_loop().run_in_executor(
                    None, self.producer.flush, timeout
                )
                self.logger.debug("Producer flushed")
            except Exception as e:
                self.logger.error(f"Failed to flush producer: {e}")

    async def close(self):
        """Close the producer and cleanup."""
        if self.producer:
            await self.flush()
            self.producer = None
            self.logger.info(
                f"Producer closed - Messages produced: {self.messages_produced}, Errors: {self.errors}"
            )

    def get_stats(self) -> Dict[str, Any]:
        """
        Get producer statistics.
        
        Returns:
            Dictionary of producer stats
        """
        stats = {
            "messages_produced": self.messages_produced,
            "errors": self.errors,
            "topic": self.settings.topic_sink
        }
        
        if self.producer:
            # Get internal producer stats
            stats["queue_size"] = len(self.producer)
        
        return stats

    async def __aenter__(self):
        """Async context manager entry."""
        if not self.producer:
            self._init_producer()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()


class BufferedKafkaProducer(KafkaProducerAsync):
    """
    Kafka producer with internal buffering for improved throughput.
    """

    def __init__(
        self,
        settings: Optional[KafkaSettings] = None,
        buffer_size: int = 1000,
        flush_interval: float = 5.0
    ):
        """
        Initialize buffered Kafka producer.
        
        Args:
            settings: Kafka settings
            buffer_size: Maximum buffer size before auto-flush
            flush_interval: Maximum time between flushes in seconds
        """
        super().__init__(settings)
        self.buffer_size = buffer_size
        self.flush_interval = flush_interval
        self.buffer = []
        self.buffer_keys = []
        self._flush_task = None
        self._last_flush = asyncio.get_event_loop().time()

    async def _auto_flush_loop(self):
        """Background task to periodically flush the buffer."""
        while True:
            await asyncio.sleep(self.flush_interval)
            if self.buffer:
                await self.flush_buffer()

    async def produce(
        self,
        message: Dict[str, Any],
        key: Optional[str] = None,
        headers: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        Add message to buffer and flush if needed.
        
        Args:
            message: Message to produce
            key: Optional message key
            headers: Optional message headers
            
        Returns:
            Placeholder result (actual delivery happens on flush)
        """
        self.buffer.append(message)
        self.buffer_keys.append(key)
        
        # Check if buffer should be flushed
        if len(self.buffer) >= self.buffer_size:
            await self.flush_buffer()
        
        # Start auto-flush task if not running
        if not self._flush_task or self._flush_task.done():
            self._flush_task = asyncio.create_task(self._auto_flush_loop())
        
        return {"status": "buffered", "buffer_size": len(self.buffer)}

    async def flush_buffer(self):
        """Flush the internal buffer to Kafka."""
        if not self.buffer:
            return
        
        try:
            # Send batch
            await super().produce_batch(self.buffer, self.buffer_keys)
            
            # Clear buffer
            self.buffer.clear()
            self.buffer_keys.clear()
            self._last_flush = asyncio.get_event_loop().time()
            
            self.logger.debug(f"Flushed buffer with {len(self.buffer)} messages")
            
        except Exception as e:
            self.logger.error(f"Failed to flush buffer: {e}")
            # Keep buffer for retry
            raise

    async def close(self):
        """Close the producer and flush any remaining messages."""
        # Cancel auto-flush task
        if self._flush_task and not self._flush_task.done():
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
        
        # Flush remaining messages
        if self.buffer:
            await self.flush_buffer()
        
        # Close producer
        await super().close()
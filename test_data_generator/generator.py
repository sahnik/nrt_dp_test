"""Test data generator for the pipeline."""

import json
import random
import time
import argparse
import signal
import sys
from datetime import datetime, timedelta
from typing import Dict, Any, Optional, List
from confluent_kafka import Producer
import logging

# Add parent directory to path for imports
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from config.settings import get_settings, TestDataSettings
from config.logging_config import setup_logging, get_logger


class TestDataGenerator:
    """
    Generates test data and publishes to Kafka topic.
    """

    def __init__(self, settings: Optional[TestDataSettings] = None):
        """
        Initialize the test data generator.
        
        Args:
            settings: Test data settings (uses default if not provided)
        """
        try:
            self.settings = settings or get_settings().test_data
            self.kafka_settings = get_settings().kafka
        except Exception as e:
            # Fallback to default settings if configuration fails
            self.settings = TestDataSettings()
            from config.settings import KafkaSettings
            self.kafka_settings = KafkaSettings()
        self.logger = get_logger("test_data_generator")
        self.producer = None
        self.running = False
        self.records_generated = 0
        self.errors = 0
        
        # Setup signal handlers
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        self.logger.info(f"Received signal {signum}, shutting down...")
        self.stop()
        sys.exit(0)

    def _init_producer(self):
        """Initialize Kafka producer."""
        try:
            config = self.kafka_settings.get_producer_config()
            self.producer = Producer(config)
            self.logger.info(f"Kafka producer initialized: {self.kafka_settings.bootstrap_servers}")
        except Exception as e:
            self.logger.error(f"Failed to initialize Kafka producer: {e}")
            raise

    def generate_valid_record(self) -> Dict[str, Any]:
        """
        Generate a valid test record.
        
        Returns:
            Valid test record
        """
        return {
            "id": f"rec_{self.records_generated:08d}",
            "timestamp": datetime.utcnow().isoformat(),
            "user_id": f"user_{random.randint(1, 1000):04d}",
            "event_type": random.choice(["click", "view", "purchase", "add_to_cart", "remove_from_cart"]),
            "product_id": f"prod_{random.randint(1, 500):04d}",
            "category": random.choice(["electronics", "clothing", "books", "home", "sports"]),
            "price": round(random.uniform(10.0, 1000.0), 2),
            "quantity": random.randint(1, 10),
            "session_id": f"sess_{random.randint(1, 10000):06d}",
            "device_type": random.choice(["mobile", "desktop", "tablet"]),
            "browser": random.choice(["chrome", "firefox", "safari", "edge"]),
            "country": random.choice(["US", "UK", "CA", "DE", "FR", "JP", "AU"]),
            "referrer": random.choice(["google", "facebook", "direct", "email", "instagram"]),
            "page_views": random.randint(1, 20),
            "time_on_site": random.randint(10, 3600),
            "is_returning_user": random.choice([True, False]),
            "discount_applied": random.choice([0, 5, 10, 15, 20]),
            "payment_method": random.choice(["credit_card", "paypal", "apple_pay", "google_pay"]),
            "shipping_method": random.choice(["standard", "express", "overnight"]),
            "customer_rating": round(random.uniform(1.0, 5.0), 1)
        }

    def generate_invalid_record(self) -> Dict[str, Any]:
        """
        Generate an invalid test record with various data quality issues.
        
        Returns:
            Invalid test record
        """
        invalid_types = random.choice([
            "missing_required",
            "invalid_type",
            "out_of_range",
            "malformed",
            "null_values"
        ])
        
        record = self.generate_valid_record()
        
        if invalid_types == "missing_required":
            # Remove required fields
            del record["id"]
            del record["timestamp"]
        elif invalid_types == "invalid_type":
            # Set wrong data types
            record["price"] = "not_a_number"
            record["quantity"] = "five"
            record["is_returning_user"] = "yes"
        elif invalid_types == "out_of_range":
            # Set values outside expected ranges
            record["price"] = -100.0
            record["quantity"] = -5
            record["customer_rating"] = 10.0
            record["discount_applied"] = 150
        elif invalid_types == "malformed":
            # Malformed data
            record["timestamp"] = "not-a-timestamp"
            record["user_id"] = ""
            record["email"] = "not-an-email"
        elif invalid_types == "null_values":
            # Unexpected null values
            record["user_id"] = None
            record["product_id"] = None
            record["price"] = None
        
        return record

    def generate_edge_case_record(self) -> Dict[str, Any]:
        """
        Generate edge case test records.
        
        Returns:
            Edge case test record
        """
        edge_types = random.choice([
            "unicode",
            "very_large",
            "very_small",
            "special_chars",
            "empty_strings"
        ])
        
        record = self.generate_valid_record()
        
        if edge_types == "unicode":
            # Unicode characters
            record["user_id"] = "user_🚀_emoji"
            record["product_name"] = "产品名称"  # Chinese characters
            record["description"] = "Café résumé naïve"  # Accented characters
        elif edge_types == "very_large":
            # Very large values
            record["price"] = 999999999.99
            record["quantity"] = 999999
            record["page_views"] = 999999
            record["description"] = "x" * 10000  # Very long string
        elif edge_types == "very_small":
            # Very small/zero values
            record["price"] = 0.01
            record["quantity"] = 0
            record["time_on_site"] = 0
        elif edge_types == "special_chars":
            # Special characters
            record["user_id"] = "user'; DROP TABLE users;--"  # SQL injection attempt
            record["product_id"] = "<script>alert('xss')</script>"  # XSS attempt
            record["description"] = "Line1\nLine2\tTab\r\nLine3"  # Control characters
        elif edge_types == "empty_strings":
            # Empty strings
            record["category"] = ""
            record["referrer"] = ""
            record["browser"] = ""
        
        return record

    def generate_record(self) -> Dict[str, Any]:
        """
        Generate a test record based on the configured pattern.
        
        Returns:
            Test record
        """
        if self.settings.pattern == "valid":
            return self.generate_valid_record()
        elif self.settings.pattern == "invalid":
            return self.generate_invalid_record()
        elif self.settings.pattern == "edge":
            return self.generate_edge_case_record()
        elif self.settings.pattern == "mixed":
            # Generate mixed pattern based on invalid_ratio
            if random.random() < self.settings.invalid_ratio:
                return self.generate_invalid_record() if random.random() < 0.5 else self.generate_edge_case_record()
            else:
                return self.generate_valid_record()
        else:
            return self.generate_valid_record()

    def delivery_report(self, err, msg):
        """
        Kafka delivery report callback.
        
        Args:
            err: Error if delivery failed
            msg: Message that was delivered or failed
        """
        if err is not None:
            self.logger.error(f"Message delivery failed: {err}")
            self.errors += 1
        else:
            self.logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")

    def publish_record(self, record: Dict[str, Any]):
        """
        Publish a record to Kafka.
        
        Args:
            record: Record to publish
        """
        try:
            # Serialize record to JSON
            message = json.dumps(record)
            
            # Produce to Kafka
            self.producer.produce(
                topic=self.kafka_settings.topic_source,
                key=record.get("id", str(self.records_generated)).encode('utf-8'),
                value=message.encode('utf-8'),
                callback=self.delivery_report
            )
            
            # Trigger delivery reports
            self.producer.poll(0)
            
        except Exception as e:
            self.logger.error(f"Failed to publish record: {e}")
            self.errors += 1

    def run(self):
        """Run the test data generator."""
        self.logger.info(f"Starting test data generator - Rate: {self.settings.rate} records/second, Pattern: {self.settings.pattern}")
        
        # Initialize producer
        self._init_producer()
        
        self.running = True
        interval = 1.0 / self.settings.rate if self.settings.rate > 0 else 1.0
        
        try:
            while self.running:
                # Check if we've reached the total records limit
                if self.settings.total_records and self.records_generated >= self.settings.total_records:
                    self.logger.info(f"Reached total records limit: {self.settings.total_records}")
                    break
                
                # Generate and publish record
                record = self.generate_record()
                self.publish_record(record)
                
                self.records_generated += 1
                
                # Log progress
                if self.records_generated % 100 == 0:
                    self.logger.info(f"Generated {self.records_generated} records, Errors: {self.errors}")
                
                # Sleep to maintain rate
                time.sleep(interval)
                
        except KeyboardInterrupt:
            self.logger.info("Interrupted by user")
        except Exception as e:
            self.logger.error(f"Generator error: {e}")
        finally:
            self.stop()

    def stop(self):
        """Stop the generator and cleanup."""
        self.running = False
        
        if self.producer:
            # Flush any pending messages
            self.logger.info("Flushing pending messages...")
            self.producer.flush(timeout=5)
            
        self.logger.info(f"Generator stopped - Total records: {self.records_generated}, Errors: {self.errors}")


def main():
    """Main entry point for the test data generator."""
    parser = argparse.ArgumentParser(description="Test Data Generator for Pipeline")
    parser.add_argument("--rate", type=int, help="Records per second to generate")
    parser.add_argument("--pattern", choices=["valid", "invalid", "edge", "mixed"], 
                       help="Pattern of test data to generate")
    parser.add_argument("--total", type=int, help="Total number of records to generate")
    parser.add_argument("--invalid-ratio", type=float, help="Ratio of invalid records in mixed pattern")
    parser.add_argument("--log-level", choices=["DEBUG", "INFO", "WARNING", "ERROR"], 
                       help="Logging level")
    
    args = parser.parse_args()
    
    # Setup logging
    try:
        setup_logging(log_level=args.log_level)
    except Exception as e:
        # Fallback to basic logging
        import logging
        logging.basicConfig(level=args.log_level or "INFO")
    
    # Override settings from command line
    try:
        settings = get_settings().test_data
    except Exception:
        # Fallback to default settings
        settings = TestDataSettings()
    
    if args.rate:
        settings.rate = args.rate
    if args.pattern:
        settings.pattern = args.pattern
    if args.total:
        settings.total_records = args.total
    if args.invalid_ratio:
        settings.invalid_ratio = args.invalid_ratio
    
    # Create and run generator
    generator = TestDataGenerator(settings)
    generator.run()


if __name__ == "__main__":
    main()
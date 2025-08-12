"""Oracle Data Publisher Service.

This service publishes Oracle reference data to Kafka after nightly batch processes.
It's designed to run as a separate service, typically triggered by cron or batch completion.

Key Features:
- Publishes Oracle reference data to compacted Kafka topic
- Supports full refresh and incremental updates
- Handles connection pooling and error recovery
- Provides metrics and logging for monitoring

Architecture:
    Oracle DB → Data Publisher → Kafka Lookup Topic → Pipeline Cache
    
The service publishes to a compacted Kafka topic so that consumers always get
the latest value for each lookup key, enabling efficient cache warming.

Usage:
    # Full refresh after nightly batch
    python -m services.oracle_data_publisher --mode full
    
    # Incremental update 
    python -m services.oracle_data_publisher --mode incremental --since "2024-01-01"
"""

import asyncio
import logging
import argparse
from typing import Dict, List, Any, Optional
from datetime import datetime, timedelta
import json
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

try:
    import cx_Oracle
except ImportError:
    cx_Oracle = None
    print("Warning: cx_Oracle not installed. Oracle connectivity disabled.")

from pipeline.kafka_producer import KafkaProducerAsync
from config.settings import get_settings
from config.logging_config import get_logger


class OracleDataPublisher:
    """
    Service for publishing Oracle reference data to Kafka.
    
    This service extracts reference data from Oracle database and publishes it
    to a compacted Kafka topic for consumption by the NRT pipeline. It supports
    both full refresh and incremental update modes.
    
    The published data structure:
    - Key: lookup_key (e.g., customer_id, product_id)  
    - Value: reference_data (JSON object with all lookup fields)
    
    Attributes:
        oracle_connection: Oracle database connection
        kafka_producer: Kafka producer for publishing data
        lookup_topic: Kafka topic name for lookup data
        batch_size: Number of records to process in each batch
    """
    
    def __init__(self, oracle_config: Dict[str, str], kafka_settings=None):
        """
        Initialize the Oracle Data Publisher.
        
        Args:
            oracle_config: Oracle connection configuration
            kafka_settings: Kafka settings (uses default if not provided)
        """
        self.oracle_config = oracle_config
        self.kafka_settings = kafka_settings or get_settings().kafka
        self.logger = get_logger("oracle_publisher")
        
        # Connection objects
        self.oracle_connection = None
        self.kafka_producer = None
        
        # Configuration
        self.lookup_topic = "lookup-data"  # Compacted topic for reference data
        self.batch_size = 1000
        self.published_count = 0
        self.error_count = 0
        
        # Lookup queries - configured based on your Oracle schema
        self.lookup_queries = {
            "customers": {
                "query": """
                    SELECT customer_id, customer_name, customer_type, segment, 
                           region, status, created_date, last_updated
                    FROM customers 
                    WHERE last_updated >= :since_date
                """,
                "key_field": "customer_id",
                "partition_key": "customer"
            },
            "products": {
                "query": """
                    SELECT product_id, product_name, category, subcategory,
                           price, status, brand, last_updated
                    FROM products
                    WHERE last_updated >= :since_date  
                """,
                "key_field": "product_id",
                "partition_key": "product"
            },
            "locations": {
                "query": """
                    SELECT location_id, location_name, address, city, state,
                           country, region, timezone, last_updated
                    FROM locations
                    WHERE last_updated >= :since_date
                """,
                "key_field": "location_id", 
                "partition_key": "location"
            }
        }

    async def connect_oracle(self):
        """Establish Oracle database connection."""
        if not cx_Oracle:
            raise ImportError("cx_Oracle not installed. Install with: pip install cx_Oracle")
            
        try:
            # Create connection string
            dsn = cx_Oracle.makedsn(
                self.oracle_config["host"],
                self.oracle_config["port"],
                service_name=self.oracle_config["service_name"]
            )
            
            # Create connection pool for better performance
            self.connection_pool = cx_Oracle.create_pool(
                user=self.oracle_config["username"],
                password=self.oracle_config["password"], 
                dsn=dsn,
                min=2,
                max=10,
                increment=1,
                threaded=True
            )
            
            self.logger.info(f"Connected to Oracle: {self.oracle_config['host']}")
            
        except Exception as e:
            self.logger.error(f"Failed to connect to Oracle: {e}")
            raise

    async def connect_kafka(self):
        """Initialize Kafka producer."""
        try:
            self.kafka_producer = KafkaProducerAsync(self.kafka_settings)
            self.logger.info("Connected to Kafka for publishing lookup data")
        except Exception as e:
            self.logger.error(f"Failed to connect to Kafka: {e}")
            raise

    async def publish_full_refresh(self):
        """
        Publish full reference data refresh.
        
        This mode publishes all reference data from Oracle to Kafka.
        Typically run after nightly batch processes complete.
        """
        self.logger.info("Starting full refresh of reference data")
        
        # Use a date far in the past to get all records
        since_date = datetime(2000, 1, 1)
        
        for lookup_name, config in self.lookup_queries.items():
            await self._publish_lookup_data(lookup_name, config, since_date)
        
        self.logger.info(
            f"Full refresh completed - Published: {self.published_count}, "
            f"Errors: {self.error_count}"
        )

    async def publish_incremental_update(self, since_date: datetime):
        """
        Publish incremental reference data update.
        
        This mode publishes only records that have been updated since the
        specified date. Used for periodic updates during the day.
        
        Args:
            since_date: Only publish records updated after this date
        """
        self.logger.info(f"Starting incremental update since {since_date}")
        
        for lookup_name, config in self.lookup_queries.items():
            await self._publish_lookup_data(lookup_name, config, since_date)
        
        self.logger.info(
            f"Incremental update completed - Published: {self.published_count}, "
            f"Errors: {self.error_count}"
        )

    async def _publish_lookup_data(self, lookup_name: str, config: Dict[str, Any], since_date: datetime):
        """
        Publish lookup data for a specific entity type.
        
        Args:
            lookup_name: Name of the lookup entity (customers, products, etc.)
            config: Query configuration
            since_date: Only include records updated after this date
        """
        self.logger.info(f"Publishing {lookup_name} lookup data")
        
        try:
            # Get Oracle connection from pool
            connection = self.connection_pool.acquire()
            cursor = connection.cursor()
            
            # Execute query
            cursor.execute(config["query"], since_date=since_date)
            
            # Get column names
            columns = [desc[0].lower() for desc in cursor.description]
            
            # Process results in batches
            batch = []
            while True:
                rows = cursor.fetchmany(self.batch_size)
                if not rows:
                    break
                
                for row in rows:
                    # Create record dictionary
                    record = dict(zip(columns, row))
                    
                    # Create Kafka message
                    lookup_key = f"{config['partition_key']}_{record[config['key_field']]}"
                    lookup_value = {
                        "lookup_type": lookup_name,
                        "key_field": config["key_field"],
                        "data": record,
                        "published_at": datetime.utcnow().isoformat(),
                        "source": "oracle"
                    }
                    
                    batch.append((lookup_key, lookup_value))
                    
                    # Publish batch when full
                    if len(batch) >= self.batch_size:
                        await self._publish_batch(batch)
                        batch = []
            
            # Publish remaining records
            if batch:
                await self._publish_batch(batch)
            
            cursor.close()
            self.connection_pool.release(connection)
            
            self.logger.info(f"Completed publishing {lookup_name} lookup data")
            
        except Exception as e:
            self.logger.error(f"Error publishing {lookup_name} lookup data: {e}")
            self.error_count += 1
            raise

    async def _publish_batch(self, batch: List[tuple]):
        """
        Publish a batch of lookup records to Kafka.
        
        Args:
            batch: List of (key, value) tuples to publish
        """
        try:
            # Prepare messages for batch publishing
            messages = []
            keys = []
            
            for key, value in batch:
                messages.append(json.dumps(value, default=str))
                keys.append(key)
            
            # Publish to Kafka
            results = await self.kafka_producer.produce_batch_to_topic(
                self.lookup_topic, 
                messages, 
                keys
            )
            
            # Count successful publishes
            successful = sum(1 for r in results if not isinstance(r, Exception))
            failed = len(results) - successful
            
            self.published_count += successful
            self.error_count += failed
            
            if failed > 0:
                self.logger.warning(f"Failed to publish {failed} records in batch")
                
        except Exception as e:
            self.logger.error(f"Failed to publish batch: {e}")
            self.error_count += len(batch)
            raise

    async def cleanup(self):
        """Cleanup connections and resources."""
        if self.kafka_producer:
            await self.kafka_producer.close()
        
        if hasattr(self, 'connection_pool') and self.connection_pool:
            self.connection_pool.close()
        
        self.logger.info("Oracle Data Publisher cleanup completed")

    async def run(self, mode: str = "full", since_hours: int = 24):
        """
        Run the Oracle Data Publisher.
        
        Args:
            mode: "full" for complete refresh, "incremental" for updates only
            since_hours: For incremental mode, hours to look back
        """
        try:
            # Connect to Oracle and Kafka
            await self.connect_oracle()
            await self.connect_kafka()
            
            # Publish data based on mode
            if mode == "full":
                await self.publish_full_refresh()
            elif mode == "incremental":
                since_date = datetime.utcnow() - timedelta(hours=since_hours)
                await self.publish_incremental_update(since_date)
            else:
                raise ValueError(f"Invalid mode: {mode}")
                
        except Exception as e:
            self.logger.error(f"Oracle Data Publisher failed: {e}")
            raise
        finally:
            await self.cleanup()

    def get_stats(self) -> Dict[str, Any]:
        """Get publisher statistics."""
        return {
            "published_count": self.published_count,
            "error_count": self.error_count,
            "lookup_topic": self.lookup_topic,
            "batch_size": self.batch_size
        }


async def main():
    """Main entry point for Oracle Data Publisher."""
    parser = argparse.ArgumentParser(description="Oracle Data Publisher")
    parser.add_argument(
        "--mode", 
        choices=["full", "incremental"], 
        default="full",
        help="Publishing mode (default: full)"
    )
    parser.add_argument(
        "--since-hours",
        type=int,
        default=24,
        help="For incremental mode: hours to look back (default: 24)"
    )
    parser.add_argument(
        "--oracle-host",
        default="localhost",
        help="Oracle host (default: localhost)"
    )
    parser.add_argument(
        "--oracle-port", 
        type=int,
        default=1521,
        help="Oracle port (default: 1521)"
    )
    parser.add_argument(
        "--oracle-service",
        default="ORCL",
        help="Oracle service name (default: ORCL)"
    )
    parser.add_argument(
        "--oracle-user",
        required=True,
        help="Oracle username"
    )
    parser.add_argument(
        "--oracle-password",
        required=True, 
        help="Oracle password"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Oracle configuration
    oracle_config = {
        "host": args.oracle_host,
        "port": args.oracle_port,
        "service_name": args.oracle_service,
        "username": args.oracle_user,
        "password": args.oracle_password
    }
    
    # Create and run publisher
    publisher = OracleDataPublisher(oracle_config)
    
    try:
        await publisher.run(mode=args.mode, since_hours=args.since_hours)
        print(f"Publisher completed successfully: {publisher.get_stats()}")
    except Exception as e:
        print(f"Publisher failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())
#!/usr/bin/env python
"""Simple standalone test data generator."""

import json
import random
import time
import argparse
from datetime import datetime
from confluent_kafka import Producer


def generate_test_record(record_id: int) -> dict:
    """Generate a single test record."""
    return {
        "id": f"rec_{record_id:08d}",
        "timestamp": datetime.utcnow().isoformat(),
        "user_id": f"user_{random.randint(1, 1000):04d}",
        "event_type": random.choice(["click", "view", "purchase", "add_to_cart"]),
        "product_id": f"prod_{random.randint(1, 500):04d}",
        "category": random.choice(["electronics", "clothing", "books", "home"]),
        "price": round(random.uniform(10.0, 1000.0), 2),
        "quantity": random.randint(1, 10),
        "device_type": random.choice(["mobile", "desktop", "tablet"]),
        "browser": random.choice(["chrome", "firefox", "safari", "edge"]),
        "country": random.choice(["US", "UK", "CA", "DE", "FR"])
    }


def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Simple Test Data Generator")
    parser.add_argument("--rate", type=int, default=5, help="Records per second")
    parser.add_argument("--total", type=int, default=50, help="Total records to generate")
    parser.add_argument("--topic", default="data-ingestion", help="Kafka topic")
    parser.add_argument("--bootstrap-servers", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--output", choices=["kafka", "console"], default="console", help="Output destination")
    
    args = parser.parse_args()
    
    print(f"Generating {args.total} records at {args.rate} records/second")
    print(f"Output: {args.output}")
    
    producer = None
    if args.output == "kafka":
        try:
            config = {
                'bootstrap.servers': args.bootstrap_servers,
                'client.id': 'simple-test-generator'
            }
            producer = Producer(config)
            print(f"Connected to Kafka: {args.bootstrap_servers}")
        except Exception as e:
            print(f"Failed to connect to Kafka: {e}")
            print("Falling back to console output")
            args.output = "console"
    
    interval = 1.0 / args.rate
    
    try:
        for i in range(args.total):
            record = generate_test_record(i + 1)
            
            if args.output == "kafka" and producer:
                try:
                    producer.produce(
                        topic=args.topic,
                        key=record["id"].encode('utf-8'),
                        value=json.dumps(record).encode('utf-8')
                    )
                    producer.poll(0)
                    print(f"Sent record {i+1}/{args.total} to Kafka")
                except Exception as e:
                    print(f"Failed to send record {i+1}: {e}")
            else:
                print(f"Record {i+1}/{args.total}: {json.dumps(record, indent=2)}")
            
            if i < args.total - 1:  # Don't sleep after the last record
                time.sleep(interval)
        
        if producer:
            producer.flush(timeout=5)
            print("All records sent successfully!")
        
    except KeyboardInterrupt:
        print("\nInterrupted by user")
    except Exception as e:
        print(f"Error: {e}")


if __name__ == "__main__":
    main()
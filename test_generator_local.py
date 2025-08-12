#!/usr/bin/env python
"""Test the data generator locally without Kafka."""

import os
import sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '.'))

from test_data_generator.generator import TestDataGenerator
from config.settings import TestDataSettings
import json


def main():
    """Test the generator locally."""
    print("Testing Data Pipeline Generator")
    print("=" * 50)
    
    # Create test settings
    settings = TestDataSettings(
        rate=5,
        pattern="mixed",
        total_records=10,
        invalid_ratio=0.2
    )
    
    # Create generator
    generator = TestDataGenerator(settings)
    
    # Test different record types
    print("\n1. Testing Valid Record Generation:")
    valid_record = generator.generate_valid_record()
    print(json.dumps(valid_record, indent=2))
    
    print("\n2. Testing Invalid Record Generation:")
    invalid_record = generator.generate_invalid_record()
    print(json.dumps(invalid_record, indent=2))
    
    print("\n3. Testing Edge Case Record Generation:")
    edge_record = generator.generate_edge_case_record()
    print(json.dumps(edge_record, indent=2))
    
    print("\n4. Testing Mixed Pattern Generation:")
    for i in range(5):
        mixed_record = generator.generate_record()
        print(f"Record {i+1}: {mixed_record['id']} - Event: {mixed_record.get('event_type', 'N/A')}")
    
    print("\n✅ Data generator test completed successfully!")
    print(f"Generator settings: Rate={settings.rate}/sec, Pattern={settings.pattern}")


if __name__ == "__main__":
    main()
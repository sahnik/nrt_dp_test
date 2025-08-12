# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a near real-time data pipeline proof of concept built in Python with the following architecture:
- Kafka ingestion source (Confluent Cloud)
- DAG-based task orchestration for defining dependencies
- Asynchronous record processing
- MongoDB Atlas or Kafka as sink/target
- Pluggable password management (using .env for POC)

## Project Structure

```
/dag_orchestrator/        - Core DAG orchestration engine
  /context.py            - Pipeline context for data passing between tasks
  /task.py               - Abstract Task base class with async execution
  /dag.py                - DAG implementation with NetworkX for dependency management
  /executor.py           - Async DAG executor with parallel task execution
/task_components/         - Reusable task libraries
  /standardization/      - Data standardization library
    /standardizer.py     - Field mapping, type conversion, standardization tasks
  /data_quality/         - Data quality check library  
    /validator.py        - Validation rules, quality checks, duplicate detection
/pipeline/               - Main pipeline execution code
  /main.py              - Main pipeline runner with DAG configuration
  /kafka_consumer.py    - Async Kafka consumer with batch processing
  /kafka_producer.py    - Async Kafka producer with buffering
  /mongodb_sink.py      - MongoDB sink with bulk operations
/test_data_generator/    - Test data generation utility
  /generator.py         - Configurable test data producer
/config/                 - Configuration management
  /settings.py          - Pydantic settings with environment variable support
  /logging_config.py    - Structured logging configuration
/tests/                  - Test suite
```

## Key Development Commands

### Environment Setup
```bash
# Automated setup (recommended)
python setup.py

# Manual setup
python -m venv venv
source venv/bin/activate  # On macOS/Linux
pip install -r requirements.txt
cp .env.example .env  # Edit with your credentials
```

### Running the Pipeline
```bash
# Start main pipeline
python pipeline/main.py

# Generate test data (various options)
python test_data_generator/generator.py --rate 10 --pattern mixed
python test_data_generator/generator.py --rate 1 --pattern valid --total 100

# Alternative generators
python simple_test_generator.py --rate 5 --total 20 --output console  # Simple console output
python test_generator_local.py  # Test generator functionality locally

# Generate data for Kafka (requires running Kafka)
python simple_test_generator.py --rate 10 --total 50 --output kafka --bootstrap-servers your-kafka-server:9092
```

### Testing
```bash
# Run all tests (38 tests pass)
pytest
# OR use the test runner
python run_tests.py

# Run specific test modules
pytest tests/test_dag_orchestrator.py
pytest tests/test_standardization.py 
pytest tests/test_data_quality.py

# Run specific test patterns
python run_tests.py dag          # DAG orchestrator tests
python run_tests.py quality      # Data quality tests
python run_tests.py standardization  # Standardization tests

# Run with coverage
pytest --cov=. --cov-report=html
```

### Code Quality
```bash
# Format code
black .

# Lint
flake8 .

# Type checking
mypy .
```

## Architecture Implementation

### DAG Orchestrator
- Uses NetworkX for dependency graph management
- Supports parallel execution of independent tasks
- Provides PipelineContext for data and metadata passing
- Implements retry logic and error recovery
- Tasks execute asynchronously with proper dependency ordering

### Task Components
**Standardization Tasks:**
- `DataStandardizationTask`: Complete standardization with field mapping, type conversion
- `FieldMappingTask`: Field renaming and mapping
- `DataTypeConversionTask`: Type conversion with error handling

**Data Quality Tasks:**
- `DataQualityTask`: Comprehensive validation with configurable rules
- `DuplicateCheckTask`: Duplicate detection with remove/mark/keep actions
- Validation rules: NotNullRule, RangeRule, PatternRule, TypeRule

**Sink Tasks:**
- `MongoDBSinkTask`: Async MongoDB writer with batch operations and upsert support
- `KafkaSinkTask`: Async Kafka producer for sink topic with JSON serialization

### Data Flow Implementation
1. `BatchKafkaConsumer` ingests records in configurable batches
2. `DataPipeline.process_batch()` creates PipelineContext with batch data
3. `DAGExecutor` runs tasks in dependency order:
   - Field mapping → Type conversion → Standardization → Duplicate check → Quality validation → **Dual Sinks (MongoDB + Kafka)**
4. Context passes DataFrames/records between tasks
5. Clean data goes to **both MongoDB and Kafka sink topic** in parallel
6. Invalid data is logged/tracked with comprehensive error reporting

### Configuration System
- Pydantic-based settings with environment variable support
- Nested configuration: KafkaSettings, MongoDBSettings, PipelineSettings  
- Environment variables use double underscore delimiter: `KAFKA__BOOTSTRAP_SERVERS`
- Hot-reloadable configuration
- Structured logging with JSON/text formats

## Environment Configuration (.env)

### Kafka Settings (Confluent Cloud)
- `KAFKA__BOOTSTRAP_SERVERS` - Kafka cluster endpoints
- `KAFKA__API_KEY` - Confluent Cloud API key for authentication
- `KAFKA__API_SECRET` - Confluent Cloud API secret for authentication
- `KAFKA__TOPIC_SOURCE` - Source topic name for data ingestion
- `KAFKA__TOPIC_SINK` - Sink topic name for processed data output
- `KAFKA__CONSUMER_GROUP` - Consumer group ID for coordinated consumption
- `KAFKA__AUTO_OFFSET_RESET` - Offset reset behavior (earliest/latest)

### MongoDB Settings (MongoDB Atlas)
- `MONGODB__CONNECTION_STRING` - MongoDB Atlas connection URI with SSL parameters
- `MONGODB__DATABASE` - Target database name for processed data
- `MONGODB__COLLECTION` - Target collection name for document storage

### Pipeline Settings
- `PIPELINE__BATCH_SIZE` - Number of records processed per batch (default: 100)
- `PIPELINE__MAX_WORKERS` - Maximum concurrent worker threads (default: 10)
- `PIPELINE__RETRY_COUNT` - Number of retry attempts for failed operations (default: 3)
- `PIPELINE__RETRY_DELAY_SECONDS` - Delay between retry attempts in seconds (default: 5)

### Test Data Generator Settings
- `TEST_DATA__RATE` - Records generated per second (default: 10)
- `TEST_DATA__PATTERN` - Data pattern type: 'valid', 'invalid', 'mixed' (default: mixed)
- `TEST_DATA__TOTAL_RECORDS` - Total records to generate (blank = unlimited)
- `TEST_DATA__INVALID_RATIO` - Ratio of invalid records when pattern=mixed (default: 0.1)

### Logging Settings
- `LOGGING__LEVEL` - Log level: DEBUG, INFO, WARNING, ERROR (default: INFO)
- `LOGGING__FORMAT` - Log format: 'json' or 'text' (default: json)

**Note**: Environment variables use double underscore (`__`) for nested configuration sections.

### Key Libraries Used
- `confluent-kafka==2.3.0` - Kafka client with async support
- `motor==3.3.2` - Async MongoDB driver  
- `networkx==3.2.1` - DAG dependency management
- `pandas==2.1.4` - Data processing
- `pydantic-settings==2.1.0` - Configuration management
- `structlog==24.1.0` - Structured logging

### Error Handling & Monitoring
- Task-level retry with exponential backoff
- Pipeline-level error tracking in PipelineContext
- Configurable error thresholds for data quality
- Comprehensive logging with correlation IDs
- Graceful shutdown on SIGTERM/SIGINT

### Performance Features  
- Async execution throughout the pipeline
- Batch processing for improved throughput
- Connection pooling for MongoDB and Kafka
- Parallel task execution where dependencies allow
- Configurable batch sizes and worker counts
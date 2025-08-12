# Near Real-Time Data Pipeline

A production-ready Python data pipeline that processes streaming data from Kafka through a DAG-based task orchestration system with dual output to MongoDB and Kafka.

## 🏗️ Architecture Overview

This pipeline implements a sophisticated data processing architecture:

```
Kafka Source → DAG Tasks → Dual Sinks (MongoDB + Kafka)
    ↓              ↓            ↓
[data-ingestion] → [Processing] → [persistence + streaming]
```

### Key Components

1. **DAG Orchestrator** - Core engine for task dependency management and parallel execution
2. **Task Components** - Reusable libraries for data standardization and quality checks
3. **Pipeline Runner** - Main execution engine with async Kafka consumer
4. **Dual Sinks** - Parallel output to MongoDB Atlas and Kafka for different use cases

## 🚀 Quick Start

### Prerequisites
- Python 3.8+
- Confluent Cloud account (Kafka)
- MongoDB Atlas account

### Setup
```bash
# Clone and setup environment
python setup.py  # Automated setup

# Or manual setup:
python -m venv venv
source venv/bin/activate  # macOS/Linux
pip install -r requirements.txt
cp .env.example .env  # Edit with your credentials
```

### Configuration
Edit `.env` with your credentials:
```env
# Kafka (Confluent Cloud)
KAFKA__BOOTSTRAP_SERVERS=your-kafka-server:9092
KAFKA__API_KEY=your-api-key
KAFKA__API_SECRET=your-api-secret

# MongoDB (Atlas)
MONGODB__CONNECTION_STRING=mongodb+srv://user:pass@cluster.mongodb.net/?ssl=true
```

### Run the Pipeline
```bash
# Start the main pipeline
python pipeline/main.py

# Generate test data in another terminal
python test_data_generator/generator.py --rate 10 --pattern mixed
```

## 📊 Data Flow

### 1. Data Ingestion
- **Source**: Kafka topic `data-ingestion`
- **Consumer**: Async batch consumer processes records in configurable batches
- **Format**: JSON records with user/transaction data

### 2. DAG Processing Pipeline
The pipeline executes tasks in dependency order:

```
Field Mapping → Type Conversion → Standardization → Duplicate Check → Quality Validation → Dual Sinks
```

#### Task Details:
- **Field Mapping**: Standardizes field names (e.g., `user_id` → `userId`)
- **Type Conversion**: Converts data types with validation
- **Standardization**: Applies business rules and formatting
- **Duplicate Check**: Identifies and handles duplicate records
- **Quality Validation**: Validates against configurable rules (NotNull, Range, Pattern, Type)
- **Dual Sinks**: Parallel writes to MongoDB and Kafka

### 3. Data Output
- **MongoDB**: Persistent storage for analytics and reporting
- **Kafka Sink**: Stream processed data to downstream consumers
- **Parallel Execution**: Both sinks run simultaneously for optimal performance

## 🔧 Core Components

### DAG Orchestrator (`/dag_orchestrator/`)
- **`context.py`** - Pipeline context for passing data between tasks
- **`task.py`** - Abstract base class with retry logic and error handling
- **`dag.py`** - DAG implementation using NetworkX for dependency management
- **`executor.py`** - Async executor for parallel task execution

### Task Components (`/task_components/`)
- **`standardization/`** - Data standardization library with field mapping and type conversion
- **`data_quality/`** - Validation rules and quality checks

### Pipeline (`/pipeline/`)
- **`main.py`** - Main pipeline runner and DAG configuration
- **`kafka_consumer.py`** - Async Kafka consumer with batch processing
- **`kafka_producer.py`** - Async Kafka producer for sink operations
- **`mongodb_sink.py`** - MongoDB sink with bulk operations and error handling
- **`kafka_sink_task.py`** - Kafka sink task for processed data

## 🧪 Testing & Development

### Run Tests
```bash
# Run all tests (38 tests)
pytest

# Run specific test categories
python run_tests.py dag          # DAG orchestrator tests
python run_tests.py quality      # Data quality tests
python run_tests.py standardization  # Standardization tests

# Run with coverage
pytest --cov=. --cov-report=html
```

### Code Quality
```bash
black .      # Format code
flake8 .     # Lint
mypy .       # Type checking
```

### Generate Test Data
```bash
# Various patterns and rates
python test_data_generator/generator.py --rate 10 --pattern mixed
python test_data_generator/generator.py --rate 1 --pattern valid --total 100
```

## ⚙️ Configuration

### Environment Variables
All configuration uses double underscore (`__`) for nested sections:

#### Kafka Settings
- `KAFKA__BOOTSTRAP_SERVERS` - Kafka cluster endpoints
- `KAFKA__API_KEY` / `KAFKA__API_SECRET` - Authentication
- `KAFKA__TOPIC_SOURCE` / `KAFKA__TOPIC_SINK` - Topic names

#### MongoDB Settings
- `MONGODB__CONNECTION_STRING` - Atlas connection URI
- `MONGODB__DATABASE` / `MONGODB__COLLECTION` - Target location

#### Pipeline Settings
- `PIPELINE__BATCH_SIZE` - Records per batch (default: 100)
- `PIPELINE__MAX_WORKERS` - Concurrent workers (default: 10)
- `PIPELINE__RETRY_COUNT` - Retry attempts (default: 3)

See [CLAUDE.md](CLAUDE.md) for complete configuration reference.

## 🛠️ Development Guide

### Adding New Tasks
1. Inherit from `Task` base class in `dag_orchestrator/task.py`
2. Implement `execute()` method with business logic
3. Add to DAG in `pipeline/main.py` with dependencies
4. Write tests in `tests/` directory

### Task Example
```python
from dag_orchestrator.task import Task
from dag_orchestrator.context import PipelineContext

class MyCustomTask(Task):
    async def execute(self, context: PipelineContext) -> PipelineContext:
        # Get data from context
        data = context.get("data")
        
        # Process data
        processed = self.process_data(data)
        
        # Set results back to context
        context.set("processed_data", processed)
        return context
```

### Error Handling
- Tasks automatically retry with exponential backoff
- Errors are logged with structured logging
- Pipeline continues processing valid records
- Failed records are tracked in context

### Monitoring
- Structured JSON logging with correlation IDs
- Task execution metrics and timing
- Error tracking and retry statistics
- Pipeline health monitoring

## 🔍 Troubleshooting

### Common Issues

**MongoDB SSL Certificate Errors:**
```bash
# Solution: Update connection string with SSL parameters
MONGODB__CONNECTION_STRING=mongodb+srv://user:pass@cluster.mongodb.net/?ssl=true&retryWrites=true
```

**Kafka Authentication Errors:**
```bash
# Verify API credentials in .env
KAFKA__API_KEY=your-key
KAFKA__API_SECRET=your-secret
```

**Import Errors:**
```bash
# Ensure virtual environment is activated
source venv/bin/activate
pip install -r requirements.txt
```

## 📈 Performance

- **Throughput**: Processes 1000+ records/second with default settings
- **Latency**: Sub-second processing for most record types
- **Scalability**: Horizontal scaling via multiple consumer instances
- **Reliability**: Built-in retry logic and error recovery

## 🏢 Production Deployment

### Security
- Environment-based configuration
- SSL/TLS for all external connections
- API key authentication for Kafka
- MongoDB Atlas with SSL encryption

### Monitoring
- Structured logging for observability
- Health checks and metrics endpoints
- Error tracking and alerting
- Performance monitoring

### Scaling
- Multiple consumer group instances
- Configurable batch sizes and worker pools
- Connection pooling for databases
- Async processing throughout

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Ensure all tests pass: `pytest`
5. Run code quality checks: `black .`, `flake8 .`
6. Submit a pull request

## 📚 Additional Resources

- [CLAUDE.md](CLAUDE.md) - Detailed technical documentation
- [Confluent Cloud Documentation](https://docs.confluent.io/cloud/)
- [MongoDB Atlas Documentation](https://docs.atlas.mongodb.com/)
- [NetworkX Documentation](https://networkx.org/) - DAG implementation
- [Motor Documentation](https://motor.readthedocs.io/) - Async MongoDB driver

---

**Architecture**: DAG-based async pipeline with dual sinks  
**Languages**: Python 3.8+  
**Key Technologies**: Kafka, MongoDB, NetworkX, asyncio  
**Status**: Production ready with comprehensive test suite
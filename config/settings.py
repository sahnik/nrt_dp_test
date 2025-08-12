"""Configuration management using pydantic-settings."""

from typing import Optional, Literal
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import Field, field_validator


class KafkaSettings(BaseSettings):
    """Kafka configuration settings."""
    
    bootstrap_servers: str = Field(
        default="localhost:9092",
        description="Kafka bootstrap servers"
    )
    api_key: Optional[str] = Field(
        default=None,
        description="Kafka API key for authentication"
    )
    api_secret: Optional[str] = Field(
        default=None,
        description="Kafka API secret for authentication"
    )
    topic_source: str = Field(
        default="data-ingestion",
        description="Source topic for data ingestion"
    )
    topic_sink: str = Field(
        default="data-processed",
        description="Sink topic for processed data"
    )
    consumer_group: str = Field(
        default="pipeline-consumer-group",
        description="Consumer group ID"
    )
    auto_offset_reset: Literal["earliest", "latest"] = Field(
        default="earliest",
        description="Auto offset reset policy"
    )
    enable_auto_commit: bool = Field(
        default=True,
        description="Enable auto commit for consumer"
    )
    session_timeout_ms: int = Field(
        default=6000,
        description="Session timeout in milliseconds"
    )
    max_poll_records: int = Field(
        default=500,
        description="Maximum records to poll at once"
    )

    @field_validator("bootstrap_servers")
    @classmethod
    def validate_bootstrap_servers(cls, v):
        """Validate bootstrap servers format."""
        if not v or not isinstance(v, str):
            raise ValueError("Bootstrap servers must be a non-empty string")
        return v

    def get_consumer_config(self) -> dict:
        """Get Kafka consumer configuration."""
        config = {
            'bootstrap.servers': self.bootstrap_servers,
            'group.id': self.consumer_group,
            'auto.offset.reset': self.auto_offset_reset,
            'enable.auto.commit': self.enable_auto_commit,
            'session.timeout.ms': self.session_timeout_ms,
        }
        
        # Add authentication if provided
        if self.api_key and self.api_secret:
            config.update({
                'security.protocol': 'SASL_SSL',
                'sasl.mechanisms': 'PLAIN',
                'sasl.username': self.api_key,
                'sasl.password': self.api_secret,
            })
        
        return config

    def get_producer_config(self) -> dict:
        """Get Kafka producer configuration."""
        config = {
            'bootstrap.servers': self.bootstrap_servers,
            'acks': 'all',
            'retries': 3,
            'max.in.flight.requests.per.connection': 1,
        }
        
        # Add authentication if provided
        if self.api_key and self.api_secret:
            config.update({
                'security.protocol': 'SASL_SSL',
                'sasl.mechanisms': 'PLAIN',
                'sasl.username': self.api_key,
                'sasl.password': self.api_secret,
            })
        
        return config


class MongoDBSettings(BaseSettings):
    """MongoDB configuration settings."""
    
    connection_string: str = Field(
        default="mongodb://localhost:27017/",
        description="MongoDB connection string"
    )
    database: str = Field(
        default="pipeline_db",
        description="Database name"
    )
    collection: str = Field(
        default="processed_data",
        description="Collection name for processed data"
    )
    max_pool_size: int = Field(
        default=10,
        description="Maximum connection pool size"
    )
    write_concern: int = Field(
        default=1,
        description="Write concern level"
    )
    batch_size: int = Field(
        default=100,
        description="Batch size for bulk operations"
    )

    @field_validator("connection_string")
    @classmethod
    def validate_connection_string(cls, v):
        """Validate MongoDB connection string."""
        if not v or not v.startswith(("mongodb://", "mongodb+srv://")):
            raise ValueError("Invalid MongoDB connection string")
        return v


class PipelineSettings(BaseSettings):
    """Pipeline configuration settings."""
    
    batch_size: int = Field(
        default=100,
        description="Number of records to process in a batch"
    )
    max_workers: int = Field(
        default=10,
        description="Maximum number of concurrent workers"
    )
    retry_count: int = Field(
        default=3,
        description="Number of retry attempts for failed tasks"
    )
    retry_delay_seconds: float = Field(
        default=5.0,
        description="Delay between retry attempts in seconds"
    )
    processing_timeout_seconds: int = Field(
        default=300,
        description="Timeout for processing a batch in seconds"
    )
    enable_metrics: bool = Field(
        default=True,
        description="Enable metrics collection"
    )
    metrics_port: int = Field(
        default=8000,
        description="Port for metrics endpoint"
    )


class TestDataSettings(BaseSettings):
    """Test data generator settings."""
    
    rate: int = Field(
        default=10,
        description="Records per second to generate"
    )
    pattern: Literal["valid", "invalid", "edge", "mixed"] = Field(
        default="mixed",
        description="Pattern of test data to generate"
    )
    total_records: Optional[int] = Field(
        default=None,
        description="Total number of records to generate (None for infinite)"
    )
    invalid_ratio: float = Field(
        default=0.1,
        description="Ratio of invalid records in mixed pattern"
    )


class LoggingSettings(BaseSettings):
    """Logging configuration settings."""
    
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = Field(
        default="INFO",
        description="Logging level"
    )
    format: Literal["json", "text"] = Field(
        default="json",
        description="Log format"
    )
    file_path: Optional[str] = Field(
        default=None,
        description="Log file path (None for stdout only)"
    )
    max_bytes: int = Field(
        default=10485760,  # 10MB
        description="Maximum log file size in bytes"
    )
    backup_count: int = Field(
        default=5,
        description="Number of backup log files to keep"
    )


class Settings(BaseSettings):
    """Main application settings."""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        env_nested_delimiter="__",
        extra="ignore"  # Allow extra fields to be ignored
    )
    
    # Sub-configurations
    kafka: KafkaSettings = Field(default_factory=KafkaSettings)
    mongodb: MongoDBSettings = Field(default_factory=MongoDBSettings)
    pipeline: PipelineSettings = Field(default_factory=PipelineSettings)
    test_data: TestDataSettings = Field(default_factory=TestDataSettings)
    logging: LoggingSettings = Field(default_factory=LoggingSettings)
    
    # Application settings
    app_name: str = Field(
        default="data-pipeline-poc",
        description="Application name"
    )
    environment: Literal["development", "staging", "production"] = Field(
        default="development",
        description="Environment name"
    )
    debug: bool = Field(
        default=False,
        description="Debug mode"
    )

    @classmethod
    def from_env(cls) -> "Settings":
        """
        Create settings from environment variables.
        
        Returns:
            Settings instance
        """
        return cls()

    def to_dict(self) -> dict:
        """
        Convert settings to dictionary.
        
        Returns:
            Dictionary representation of settings
        """
        return self.model_dump()


# Singleton instance
_settings: Optional[Settings] = None


def get_settings() -> Settings:
    """
    Get the singleton settings instance.
    
    Returns:
        Settings instance
    """
    global _settings
    if _settings is None:
        _settings = Settings.from_env()
    return _settings


def reload_settings() -> Settings:
    """
    Reload settings from environment.
    
    Returns:
        New settings instance
    """
    global _settings
    _settings = Settings.from_env()
    return _settings
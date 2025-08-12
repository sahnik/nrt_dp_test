"""MongoDB sink for the pipeline."""

import asyncio
from typing import Dict, Any, List, Optional
from datetime import datetime
import pandas as pd
import logging
from motor.motor_asyncio import AsyncIOMotorClient
from pymongo.errors import BulkWriteError, ConnectionFailure

from config.settings import get_settings, MongoDBSettings
from config.logging_config import get_logger
from dag_orchestrator.task import Task
from dag_orchestrator.context import PipelineContext


class MongoDBSink:
    """
    Asynchronous MongoDB sink for pipeline data output.
    """

    def __init__(self, settings: Optional[MongoDBSettings] = None):
        """
        Initialize the MongoDB sink.
        
        Args:
            settings: MongoDB settings (uses default if not provided)
        """
        self.settings = settings or get_settings().mongodb
        self.logger = get_logger("mongodb_sink")
        self.client = None
        self.database = None
        self.collection = None
        self.documents_written = 0
        self.errors = 0

    async def connect(self):
        """Connect to MongoDB."""
        try:
            # Option 1: Use certifi for certificates
            # import certifi
            # self.client = AsyncIOMotorClient(
            #     self.settings.connection_string,
            #     maxPoolSize=self.settings.max_pool_size,
            #     tls=True,
            #     tlsCAFile=certifi.where()
            # )
            
            # Option 2: Disable SSL verification (TEMPORARY - for testing only)
            self.client = AsyncIOMotorClient(
                self.settings.connection_string,
                maxPoolSize=self.settings.max_pool_size,
                tls=True,
                tlsAllowInvalidCertificates=True
            )
            self.database = self.client[self.settings.database]
            self.collection = self.database[self.settings.collection]
            
            # Test connection
            await self.client.admin.command('ping')
            self.logger.info(f"Connected to MongoDB: {self.settings.database}/{self.settings.collection}")
            
        except ConnectionFailure as e:
            self.logger.error(f"Failed to connect to MongoDB: {e}")
            raise

    async def write(self, document: Dict[str, Any]) -> str:
        """
        Write a single document to MongoDB.
        
        Args:
            document: Document to write
            
        Returns:
            Inserted document ID
        """
        if self.collection is None:
            await self.connect()
        
        try:
            # Add metadata
            document["_inserted_at"] = datetime.utcnow()
            
            # Insert document
            result = await self.collection.insert_one(document)
            self.documents_written += 1
            
            return str(result.inserted_id)
            
        except Exception as e:
            self.logger.error(f"Failed to write document: {e}")
            self.errors += 1
            raise

    async def write_batch(self, documents: List[Dict[str, Any]]) -> List[str]:
        """
        Write a batch of documents to MongoDB.
        
        Args:
            documents: List of documents to write
            
        Returns:
            List of inserted document IDs
        """
        if self.collection is None:
            await self.connect()
        
        if not documents:
            return []
        
        try:
            # Add metadata to each document
            for doc in documents:
                doc["_inserted_at"] = datetime.utcnow()
            
            # Bulk insert
            result = await self.collection.insert_many(
                documents,
                ordered=False  # Continue on errors
            )
            
            self.documents_written += len(result.inserted_ids)
            self.logger.info(f"Wrote batch of {len(result.inserted_ids)} documents")
            
            return [str(id) for id in result.inserted_ids]
            
        except BulkWriteError as e:
            # Some documents may have been written
            write_errors = e.details.get('writeErrors', [])
            self.errors += len(write_errors)
            
            # Get successful writes
            successful = e.details.get('nInserted', 0)
            self.documents_written += successful
            
            self.logger.warning(
                f"Bulk write partial failure: {successful} succeeded, {len(write_errors)} failed"
            )
            
            # Return IDs of successful inserts if available
            if hasattr(e, 'inserted_ids'):
                return [str(id) for id in e.inserted_ids]
            return []
            
        except Exception as e:
            self.logger.error(f"Failed to write batch: {e}")
            self.errors += len(documents)
            raise

    async def upsert(self, document: Dict[str, Any], filter_dict: Dict[str, Any]) -> str:
        """
        Upsert a document (update if exists, insert if not).
        
        Args:
            document: Document to upsert
            filter_dict: Filter to find existing document
            
        Returns:
            Upserted document ID
        """
        if self.collection is None:
            await self.connect()
        
        try:
            # Add/update metadata
            document["_updated_at"] = datetime.utcnow()
            
            # Upsert document
            result = await self.collection.replace_one(
                filter_dict,
                document,
                upsert=True
            )
            
            if result.upserted_id:
                self.documents_written += 1
                return str(result.upserted_id)
            else:
                return str(result.matched_count)
            
        except Exception as e:
            self.logger.error(f"Failed to upsert document: {e}")
            self.errors += 1
            raise

    async def create_index(self, keys: List[tuple], unique: bool = False):
        """
        Create an index on the collection.
        
        Args:
            keys: List of (field, direction) tuples
            unique: Whether the index should be unique
        """
        if self.collection is None:
            await self.connect()
        
        try:
            await self.collection.create_index(keys, unique=unique)
            self.logger.info(f"Created index on {keys}")
        except Exception as e:
            self.logger.error(f"Failed to create index: {e}")
            raise

    async def close(self):
        """Close the MongoDB connection."""
        if self.client:
            self.client.close()
            self.logger.info(
                f"MongoDB connection closed - Documents written: {self.documents_written}, Errors: {self.errors}"
            )

    def get_stats(self) -> Dict[str, Any]:
        """
        Get sink statistics.
        
        Returns:
            Dictionary of sink stats
        """
        return {
            "documents_written": self.documents_written,
            "errors": self.errors,
            "database": self.settings.database,
            "collection": self.settings.collection
        }

    async def __aenter__(self):
        """Async context manager entry."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        await self.close()


class MongoDBSinkTask(Task):
    """
    Task for writing data to MongoDB.
    """

    def __init__(
        self,
        name: str = "mongodb_sink",
        settings: Optional[MongoDBSettings] = None,
        batch_write: bool = True,
        upsert_keys: Optional[List[str]] = None,
        retries: int = 3,
        retry_delay: float = 1.0
    ):
        """
        Initialize the MongoDB sink task.
        
        Args:
            name: Task name
            settings: MongoDB settings
            batch_write: Whether to use batch writing
            upsert_keys: Fields to use for upsert filter
            retries: Number of retry attempts
            retry_delay: Delay between retries
        """
        super().__init__(name, retries, retry_delay)
        self.settings = settings or get_settings().mongodb
        self.batch_write = batch_write
        self.upsert_keys = upsert_keys
        self.sink = None
        self.logger = get_logger(f"task.{name}")

    async def execute(self, context: PipelineContext) -> PipelineContext:
        """
        Execute the MongoDB sink task.
        
        Args:
            context: Pipeline context containing data
            
        Returns:
            Updated context with write results
        """
        # Get data from context
        data = context.get("data")
        
        if data is None:
            self.logger.warning("No data found in context")
            return context
        
        # Initialize sink if not already done
        if self.sink is None:
            self.sink = MongoDBSink(self.settings)
            await self.sink.connect()
        
        try:
            # Convert data to list of documents
            if isinstance(data, pd.DataFrame):
                documents = data.to_dict('records')
            elif isinstance(data, list):
                documents = data
            elif isinstance(data, dict):
                documents = [data]
            else:
                self.logger.error(f"Unsupported data type: {type(data)}")
                context.add_error(self.name, f"Unsupported data type: {type(data)}")
                return context
            
            # Clean documents for MongoDB compatibility
            cleaned_documents = []
            for doc in documents:
                cleaned_doc = {}
                for key, value in doc.items():
                    if pd.isna(value):
                        cleaned_doc[key] = None
                    elif hasattr(value, 'isoformat'):  # datetime objects
                        cleaned_doc[key] = value.isoformat()
                    elif isinstance(value, pd.Timestamp):
                        cleaned_doc[key] = value.isoformat() if not pd.isna(value) else None
                    else:
                        cleaned_doc[key] = value
                cleaned_documents.append(cleaned_doc)
            
            documents = cleaned_documents
            
            # Write to MongoDB
            if self.upsert_keys:
                # Upsert mode
                results = []
                for doc in documents:
                    filter_dict = {key: doc.get(key) for key in self.upsert_keys}
                    result = await self.sink.upsert(doc, filter_dict)
                    results.append(result)
                
                context.add_task_output(self.name, {
                    "documents_upserted": len(results),
                    "mode": "upsert"
                })
                
            elif self.batch_write and len(documents) > 1:
                # Batch write mode
                batch_size = self.settings.batch_size
                all_ids = []
                
                for i in range(0, len(documents), batch_size):
                    batch = documents[i:i+batch_size]
                    ids = await self.sink.write_batch(batch)
                    all_ids.extend(ids)
                
                context.add_task_output(self.name, {
                    "documents_written": len(all_ids),
                    "document_ids": all_ids,
                    "mode": "batch"
                })
                
            else:
                # Single document write
                ids = []
                for doc in documents:
                    doc_id = await self.sink.write(doc)
                    ids.append(doc_id)
                
                context.add_task_output(self.name, {
                    "documents_written": len(ids),
                    "document_ids": ids,
                    "mode": "single"
                })
            
            self.logger.info(f"Successfully wrote {len(documents)} documents to MongoDB")
            
        except Exception as e:
            self.logger.error(f"Failed to write to MongoDB: {e}")
            context.add_error(self.name, str(e))
            raise
        
        return context

    async def cleanup(self):
        """Cleanup resources."""
        if self.sink:
            await self.sink.close()


class MongoDBQueryTask(Task):
    """
    Task for querying data from MongoDB.
    """

    def __init__(
        self,
        name: str = "mongodb_query",
        settings: Optional[MongoDBSettings] = None,
        query: Dict[str, Any] = None,
        projection: Dict[str, int] = None,
        limit: int = None,
        retries: int = 3,
        retry_delay: float = 1.0
    ):
        """
        Initialize the MongoDB query task.
        
        Args:
            name: Task name
            settings: MongoDB settings
            query: MongoDB query filter
            projection: Field projection
            limit: Maximum documents to retrieve
            retries: Number of retry attempts
            retry_delay: Delay between retries
        """
        super().__init__(name, retries, retry_delay)
        self.settings = settings or get_settings().mongodb
        self.query = query or {}
        self.projection = projection
        self.limit = limit
        self.logger = get_logger(f"task.{name}")

    async def execute(self, context: PipelineContext) -> PipelineContext:
        """
        Execute the MongoDB query task.
        
        Args:
            context: Pipeline context
            
        Returns:
            Updated context with query results
        """
        sink = MongoDBSink(self.settings)
        await sink.connect()
        
        try:
            # Build cursor
            cursor = sink.collection.find(self.query, self.projection)
            
            if self.limit:
                cursor = cursor.limit(self.limit)
            
            # Fetch documents
            documents = await cursor.to_list(length=self.limit)
            
            # Convert ObjectId to string
            for doc in documents:
                if "_id" in doc:
                    doc["_id"] = str(doc["_id"])
            
            # Add to context
            context.set("data", documents)
            context.add_task_output(self.name, {
                "documents_retrieved": len(documents),
                "query": self.query
            })
            
            self.logger.info(f"Retrieved {len(documents)} documents from MongoDB")
            
        except Exception as e:
            self.logger.error(f"Failed to query MongoDB: {e}")
            context.add_error(self.name, str(e))
            raise
        
        finally:
            await sink.close()
        
        return context
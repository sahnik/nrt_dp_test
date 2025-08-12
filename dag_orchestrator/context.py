"""Pipeline context for passing data between tasks in the DAG.

This module provides the PipelineContext class which serves as a shared data container
that flows through the DAG execution. Tasks can read data from the context, process it,
and write results back to the context for subsequent tasks to consume.

Key Features:
- Data storage and retrieval between tasks
- Metadata tracking (creation time, task history, etc.)
- Error and warning collection for monitoring
- Task output tracking for debugging and metrics
- Deep copying for parallel task execution

Usage:
    # Initialize with data
    context = PipelineContext(initial_data={"records": batch_records})
    
    # Tasks read and write data
    records = context.get("records")
    context.set("processed_records", cleaned_records)
    
    # Track task outputs and errors
    context.add_task_output("validation", {"passed": 95, "failed": 5})
    context.add_error("validation", "Failed to validate 5 records")
"""

from typing import Any, Dict, Optional
from datetime import datetime
import copy


class PipelineContext:
    """
    Context object that flows through the DAG pipeline.
    
    This class acts as a shared data container that carries information between
    tasks in the DAG. Each task can read from and write to the context, enabling
    data flow through the processing pipeline.
    
    The context maintains three main data structures:
    1. _data: The main data payload (DataFrames, records, etc.)
    2. _metadata: Pipeline metadata (timestamps, history, errors)
    3. _task_outputs: Individual task results for monitoring
    
    Attributes:
        _data (Dict[str, Any]): Main data storage (records, DataFrames, etc.)
        _metadata (Dict[str, Any]): Pipeline metadata and execution history
        _task_outputs (Dict[str, Any]): Task-specific outputs for monitoring
    """

    def __init__(self, initial_data: Optional[Dict[str, Any]] = None):
        """
        Initialize the pipeline context.
        
        Args:
            initial_data: Initial data to populate the context
        """
        self._data = initial_data or {}
        self._metadata = {
            "created_at": datetime.utcnow(),
            "task_history": [],
            "errors": [],
            "warnings": []
        }
        self._task_outputs = {}

    def get(self, key: str, default: Any = None) -> Any:
        """
        Retrieve data from the context by key.
        
        This is the primary method tasks use to read data that was stored
        by previous tasks in the pipeline. Common keys include "data" for
        input records, "clean_data" for validated records, etc.
        
        Args:
            key: The data key to retrieve (e.g., "data", "processed_records")
            default: Default value to return if key doesn't exist
            
        Returns:
            The stored value for the key, or default if not found
            
        Example:
            records = context.get("data")  # Get input records
            clean_data = context.get("clean_data", [])  # Get with default
        """
        return self._data.get(key, default)

    def set(self, key: str, value: Any) -> None:
        """
        Store data in the context for use by subsequent tasks.
        
        This is the primary method tasks use to store processed data
        for consumption by downstream tasks. Data can be DataFrames,
        lists of records, validation results, or any processed output.
        
        Args:
            key: The data key to store (e.g., "standardized_data", "validation_results")
            value: The data to store (DataFrames, lists, dicts, etc.)
            
        Example:
            context.set("processed_data", cleaned_records)
            context.set("validation_summary", {"passed": 95, "failed": 5})
        """
        self._data[key] = value

    def update(self, data: Dict[str, Any]) -> None:
        """
        Update the context with multiple key-value pairs.
        
        Args:
            data: Dictionary of data to update
        """
        self._data.update(data)

    def add_task_output(self, task_name: str, output: Any) -> None:
        """
        Store the output from a specific task.
        
        Args:
            task_name: Name of the task
            output: Output from the task
        """
        self._task_outputs[task_name] = output
        self._metadata["task_history"].append({
            "task": task_name,
            "timestamp": datetime.utcnow(),
            "status": "completed"
        })

    def get_task_output(self, task_name: str) -> Any:
        """
        Retrieve the output from a specific task.
        
        Args:
            task_name: Name of the task
            
        Returns:
            The output from the specified task
        """
        return self._task_outputs.get(task_name)

    def add_error(self, task_name: str, error: str) -> None:
        """
        Add an error to the context.
        
        Args:
            task_name: Task where the error occurred
            error: Error message
        """
        self._metadata["errors"].append({
            "task": task_name,
            "error": error,
            "timestamp": datetime.utcnow()
        })

    def add_warning(self, task_name: str, warning: str) -> None:
        """
        Add a warning to the context.
        
        Args:
            task_name: Task where the warning occurred
            warning: Warning message
        """
        self._metadata["warnings"].append({
            "task": task_name,
            "warning": warning,
            "timestamp": datetime.utcnow()
        })

    def has_errors(self) -> bool:
        """Check if the context has any errors."""
        return len(self._metadata["errors"]) > 0

    def get_errors(self) -> list:
        """Get all errors from the context."""
        return self._metadata["errors"]

    def get_metadata(self) -> Dict[str, Any]:
        """Get the metadata dictionary."""
        return self._metadata

    def clone(self) -> "PipelineContext":
        """
        Create a deep copy of the context.
        
        Returns:
            A new PipelineContext instance with copied data
        """
        new_context = PipelineContext()
        new_context._data = copy.deepcopy(self._data)
        new_context._metadata = copy.deepcopy(self._metadata)
        new_context._task_outputs = copy.deepcopy(self._task_outputs)
        return new_context

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert the context to a dictionary.
        
        Returns:
            Dictionary representation of the context
        """
        return {
            "data": self._data,
            "metadata": self._metadata,
            "task_outputs": self._task_outputs
        }

    def __repr__(self) -> str:
        """String representation of the context."""
        return f"PipelineContext(data_keys={list(self._data.keys())}, tasks_completed={len(self._task_outputs)})"
"""Abstract base class for DAG tasks.

This module provides the base Task class that all pipeline tasks must inherit from.
It includes retry logic, error handling, status tracking, and execution timing.

Key Features:
- Abstract base class enforcing execute() method implementation
- Automatic retry logic with exponential backoff
- Task status tracking (PENDING, RUNNING, SUCCESS, FAILED, SKIPPED)
- Execution timing and error tracking
- Integration with PipelineContext for data flow

Usage:
    class MyTask(Task):
        async def execute(self, context: PipelineContext) -> PipelineContext:
            # Implement task logic here
            data = context.get("input_data")
            processed = self.process(data)
            context.set("output_data", processed)
            return context

All tasks in the pipeline inherit from this base class, ensuring consistent
behavior for error handling, retries, and status management.
"""

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional, List
from datetime import datetime
import asyncio
import logging
from enum import Enum

from .context import PipelineContext


class TaskStatus(Enum):
    """Task execution status."""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


class Task(ABC):
    """
    Abstract base class for all pipeline tasks.
    
    This class provides the foundation for all tasks in the DAG pipeline.
    It handles common concerns like retry logic, error tracking, timing,
    and status management, allowing concrete task implementations to focus
    on their specific business logic.
    
    Key Responsibilities:
    1. Execute task logic with automatic retries
    2. Track task status and execution timing
    3. Handle errors with proper logging
    4. Integrate with PipelineContext for data flow
    5. Provide cleanup mechanisms for resources
    
    All concrete tasks must implement the execute() method which contains
    the core business logic for that specific processing step.
    
    Attributes:
        name (str): Unique identifier for the task
        status (TaskStatus): Current execution status
        retries (int): Maximum number of retry attempts
        retry_delay (float): Base delay between retries (exponential backoff)
        start_time (datetime): When execution started
        end_time (datetime): When execution completed
        error (str): Last error message if task failed
    """

    def __init__(self, name: str, retries: int = 3, retry_delay: float = 1.0):
        """
        Initialize the task.
        
        Args:
            name: Unique name for the task
            retries: Number of retry attempts on failure
            retry_delay: Delay in seconds between retries
        """
        self.name = name
        self.retries = retries
        self.retry_delay = retry_delay
        self.status = TaskStatus.PENDING
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.error: Optional[str] = None
        self.dependencies: List[str] = []
        self.logger = logging.getLogger(f"task.{name}")

    @abstractmethod
    async def execute(self, context: PipelineContext) -> PipelineContext:
        """
        Execute the task logic.
        
        Args:
            context: The pipeline context containing data and metadata
            
        Returns:
            Updated pipeline context
        """
        pass

    async def run(self, context: PipelineContext) -> PipelineContext:
        """
        Run the task with retry logic and error handling.
        
        Args:
            context: The pipeline context
            
        Returns:
            Updated pipeline context
        """
        self.status = TaskStatus.RUNNING
        self.start_time = datetime.utcnow()
        self.logger.info(f"Starting task: {self.name}")

        attempt = 0
        last_error = None

        while attempt <= self.retries:
            try:
                # Execute the task
                result_context = await self.execute(context)
                
                # Mark as successful
                self.status = TaskStatus.SUCCESS
                self.end_time = datetime.utcnow()
                
                # Add task output to context
                result_context.add_task_output(self.name, {
                    "status": self.status.value,
                    "duration": (self.end_time - self.start_time).total_seconds()
                })
                
                self.logger.info(f"Task completed successfully: {self.name}")
                return result_context

            except Exception as e:
                last_error = str(e)
                self.logger.error(f"Task {self.name} failed (attempt {attempt + 1}/{self.retries + 1}): {e}")
                
                if attempt < self.retries:
                    await asyncio.sleep(self.retry_delay * (attempt + 1))  # Exponential backoff
                    attempt += 1
                else:
                    # All retries exhausted
                    self.status = TaskStatus.FAILED
                    self.end_time = datetime.utcnow()
                    self.error = last_error
                    
                    # Add error to context
                    context.add_error(self.name, last_error)
                    context.add_task_output(self.name, {
                        "status": self.status.value,
                        "error": last_error,
                        "duration": (self.end_time - self.start_time).total_seconds()
                    })
                    
                    raise

        return context

    def add_dependency(self, task_name: str) -> None:
        """
        Add a dependency to this task.
        
        Args:
            task_name: Name of the task this task depends on
        """
        if task_name not in self.dependencies:
            self.dependencies.append(task_name)

    def remove_dependency(self, task_name: str) -> None:
        """
        Remove a dependency from this task.
        
        Args:
            task_name: Name of the task to remove from dependencies
        """
        if task_name in self.dependencies:
            self.dependencies.remove(task_name)

    def get_dependencies(self) -> List[str]:
        """Get the list of task dependencies."""
        return self.dependencies.copy()

    def reset(self) -> None:
        """Reset the task state."""
        self.status = TaskStatus.PENDING
        self.start_time = None
        self.end_time = None
        self.error = None

    def __repr__(self) -> str:
        """String representation of the task."""
        return f"Task(name={self.name}, status={self.status.value}, dependencies={self.dependencies})"


class ConditionalTask(Task):
    """
    A task that executes based on a condition.
    """

    def __init__(self, name: str, condition_func, retries: int = 3, retry_delay: float = 1.0):
        """
        Initialize the conditional task.
        
        Args:
            name: Task name
            condition_func: Function that takes context and returns bool
            retries: Number of retry attempts
            retry_delay: Delay between retries
        """
        super().__init__(name, retries, retry_delay)
        self.condition_func = condition_func

    async def run(self, context: PipelineContext) -> PipelineContext:
        """
        Run the task only if the condition is met.
        
        Args:
            context: The pipeline context
            
        Returns:
            Updated pipeline context
        """
        if not self.condition_func(context):
            self.status = TaskStatus.SKIPPED
            self.logger.info(f"Skipping task {self.name} - condition not met")
            context.add_task_output(self.name, {
                "status": self.status.value,
                "reason": "condition_not_met"
            })
            return context
        
        return await super().run(context)
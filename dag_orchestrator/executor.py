"""Asynchronous executor for running DAG tasks."""

import asyncio
from typing import Dict, Set, Optional, List
from datetime import datetime
import logging

from .dag import DAG
from .task import Task, TaskStatus
from .context import PipelineContext


class DAGExecutor:
    """
    Asynchronous executor for running DAG tasks with parallel execution support.
    """

    def __init__(self, max_workers: int = 10):
        """
        Initialize the DAG executor.
        
        Args:
            max_workers: Maximum number of concurrent task executions
        """
        self.max_workers = max_workers
        self.logger = logging.getLogger("dag.executor")
        self._running_tasks: Set[str] = set()
        self._completed_tasks: Set[str] = set()
        self._failed_tasks: Set[str] = set()
        self._task_futures: Dict[str, asyncio.Task] = {}

    async def execute(self, dag: DAG, context: PipelineContext) -> PipelineContext:
        """
        Execute all tasks in the DAG following dependency order.
        
        Args:
            dag: The DAG to execute
            context: Initial pipeline context
            
        Returns:
            Final pipeline context after all tasks complete
        """
        self.logger.info(f"Starting DAG execution: {dag.name}")
        
        # Validate DAG
        if not dag.validate():
            raise ValueError("Invalid DAG - contains cycles")
        
        # Reset state
        self._running_tasks.clear()
        self._completed_tasks.clear()
        self._failed_tasks.clear()
        self._task_futures.clear()
        dag.reset()
        
        # Create a semaphore to limit concurrent executions
        semaphore = asyncio.Semaphore(self.max_workers)
        
        # Start execution
        start_time = datetime.utcnow()
        
        try:
            # Execute tasks in parallel where possible
            await self._execute_parallel(dag, context, semaphore)
            
            # Check if all tasks completed successfully
            if self._failed_tasks:
                self.logger.error(f"DAG execution failed. Failed tasks: {self._failed_tasks}")
                context.add_error("dag_executor", f"Failed tasks: {list(self._failed_tasks)}")
            else:
                self.logger.info(f"DAG execution completed successfully")
            
        except Exception as e:
            self.logger.error(f"DAG execution error: {e}")
            context.add_error("dag_executor", str(e))
            raise
        
        finally:
            end_time = datetime.utcnow()
            duration = (end_time - start_time).total_seconds()
            self.logger.info(f"DAG execution finished in {duration:.2f} seconds")
            
            # Add execution summary to context
            context.set("dag_execution_summary", {
                "dag_name": dag.name,
                "total_tasks": len(dag.tasks),
                "completed_tasks": len(self._completed_tasks),
                "failed_tasks": len(self._failed_tasks),
                "duration_seconds": duration
            })
        
        return context

    async def _execute_parallel(self, dag: DAG, context: PipelineContext, semaphore: asyncio.Semaphore) -> None:
        """
        Execute tasks in parallel respecting dependencies.
        
        Args:
            dag: The DAG to execute
            context: Pipeline context
            semaphore: Semaphore for limiting concurrent executions
        """
        # Continue until all tasks are processed
        while len(self._completed_tasks) + len(self._failed_tasks) < len(dag.tasks):
            # Get tasks that are ready to run
            ready_tasks = dag.get_ready_tasks(self._completed_tasks)
            
            # Filter out tasks that are already running or failed
            ready_tasks = [
                task for task in ready_tasks 
                if task not in self._running_tasks and task not in self._failed_tasks
            ]
            
            if not ready_tasks and not self._running_tasks:
                # No tasks to run and nothing running - check for issues
                unprocessed = set(dag.tasks.keys()) - self._completed_tasks - self._failed_tasks
                if unprocessed:
                    self.logger.error(f"Deadlock detected. Unprocessed tasks: {unprocessed}")
                    raise RuntimeError(f"DAG execution deadlock. Unprocessed tasks: {unprocessed}")
                break
            
            # Start ready tasks
            for task_name in ready_tasks:
                task = dag.get_task(task_name)
                if task is not None:
                    # Create task execution coroutine
                    task_future = asyncio.create_task(
                        self._execute_task(task, context, semaphore)
                    )
                    self._task_futures[task_name] = task_future
                    self._running_tasks.add(task_name)
                else:
                    self.logger.error(f"Task {task_name} not found in DAG")
                    self._failed_tasks.add(task_name)
            
            # Wait for at least one task to complete if any are running
            if self._running_tasks:
                # Wait for any task to complete
                done, pending = await asyncio.wait(
                    self._task_futures.values(),
                    return_when=asyncio.FIRST_COMPLETED
                )
                
                # Process completed tasks
                for future in done:
                    # Find which task completed
                    for task_name, task_future in list(self._task_futures.items()):
                        if task_future == future:
                            self._running_tasks.discard(task_name)
                            del self._task_futures[task_name]
                            
                            # Check if task succeeded or failed
                            task = dag.get_task(task_name)
                            if task and task.status == TaskStatus.SUCCESS:
                                self._completed_tasks.add(task_name)
                                self.logger.info(f"Task completed: {task_name}")
                            else:
                                self._failed_tasks.add(task_name)
                                self.logger.error(f"Task failed: {task_name}")
                                
                                # Check if we should stop on failure
                                if self._should_stop_on_failure(dag, task_name):
                                    self.logger.warning(f"Stopping execution due to failed task: {task_name}")
                                    # Cancel remaining tasks
                                    await self._cancel_running_tasks()
                                    return
            
            # Small delay to prevent busy waiting
            await asyncio.sleep(0.01)

    async def _execute_task(self, task: Task, context: PipelineContext, semaphore: asyncio.Semaphore) -> None:
        """
        Execute a single task with semaphore control.
        
        Args:
            task: Task to execute
            context: Pipeline context
            semaphore: Semaphore for limiting concurrent executions
        """
        async with semaphore:
            try:
                self.logger.debug(f"Executing task: {task.name}")
                await task.run(context)
            except Exception as e:
                self.logger.error(f"Task execution error in {task.name}: {e}")
                # Error is already handled in task.run, just log here

    def _should_stop_on_failure(self, dag: DAG, failed_task: str) -> bool:
        """
        Determine if execution should stop due to a task failure.
        
        Args:
            dag: The DAG being executed
            failed_task: Name of the failed task
            
        Returns:
            True if execution should stop
        """
        # Check if any uncompleted tasks depend on the failed task
        dependents = dag.get_dependents(failed_task)
        uncompleted_dependents = dependents - self._completed_tasks - self._failed_tasks
        
        if uncompleted_dependents:
            self.logger.warning(f"Tasks depending on failed task {failed_task}: {uncompleted_dependents}")
            return True
        
        return False

    async def _cancel_running_tasks(self) -> None:
        """Cancel all running tasks."""
        for task_name, future in self._task_futures.items():
            if not future.done():
                self.logger.info(f"Cancelling task: {task_name}")
                future.cancel()
        
        # Wait for cancellations to complete
        if self._task_futures:
            await asyncio.gather(*self._task_futures.values(), return_exceptions=True)


class ParallelDAGExecutor(DAGExecutor):
    """
    Enhanced executor with optimized parallel execution strategies.
    """

    async def execute(self, dag: DAG, context: PipelineContext) -> PipelineContext:
        """
        Execute DAG with optimized parallel execution.
        
        Args:
            dag: The DAG to execute
            context: Initial pipeline context
            
        Returns:
            Final pipeline context
        """
        self.logger.info(f"Starting parallel DAG execution: {dag.name}")
        
        # Get parallel execution groups
        parallel_groups = dag.get_parallel_groups()
        self.logger.info(f"Identified {len(parallel_groups)} parallel execution groups")
        
        # Execute each group
        for i, group in enumerate(parallel_groups, 1):
            self.logger.info(f"Executing parallel group {i}: {group}")
            
            # Execute all tasks in the group concurrently
            tasks = [dag.get_task(task_name) for task_name in group if dag.get_task(task_name)]
            
            # Run tasks in parallel
            results = await asyncio.gather(
                *[task.run(context) for task in tasks],
                return_exceptions=True
            )
            
            # Check for failures
            for task, result in zip(tasks, results):
                if isinstance(result, Exception):
                    self._failed_tasks.add(task.name)
                    self.logger.error(f"Task {task.name} failed: {result}")
                    
                    # Stop if critical task failed
                    if self._should_stop_on_failure(dag, task.name):
                        self.logger.error(f"Stopping execution due to critical task failure: {task.name}")
                        return context
                else:
                    self._completed_tasks.add(task.name)
        
        return context
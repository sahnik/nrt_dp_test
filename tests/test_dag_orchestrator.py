"""Tests for DAG orchestrator components."""

import pytest
import asyncio
from datetime import datetime

import sys
sys.path.append('..')

from dag_orchestrator.context import PipelineContext
from dag_orchestrator.task import Task, TaskStatus
from dag_orchestrator.dag import DAG
from dag_orchestrator.executor import DAGExecutor


class SimpleTask(Task):
    """Simple test task."""
    
    def __init__(self, name: str, delay: float = 0.1, should_fail: bool = False):
        super().__init__(name)
        self.delay = delay
        self.should_fail = should_fail
        self.executed = False
    
    async def execute(self, context: PipelineContext) -> PipelineContext:
        """Execute the test task."""
        await asyncio.sleep(self.delay)
        self.executed = True
        
        if self.should_fail:
            raise ValueError(f"Task {self.name} failed intentionally")
        
        # Add some data to context
        context.set(f"{self.name}_result", f"Result from {self.name}")
        return context


class TestPipelineContext:
    """Test PipelineContext functionality."""
    
    def test_context_creation(self):
        """Test context creation."""
        context = PipelineContext()
        assert isinstance(context, PipelineContext)
        assert context.get("nonexistent") is None
        assert context.get("nonexistent", "default") == "default"
    
    def test_context_data_operations(self):
        """Test context data operations."""
        context = PipelineContext({"initial": "value"})
        
        # Test get/set
        assert context.get("initial") == "value"
        context.set("new_key", "new_value")
        assert context.get("new_key") == "new_value"
        
        # Test update
        context.update({"key1": "val1", "key2": "val2"})
        assert context.get("key1") == "val1"
        assert context.get("key2") == "val2"
    
    def test_task_outputs(self):
        """Test task output tracking."""
        context = PipelineContext()
        
        # Add task output
        context.add_task_output("task1", {"result": "success"})
        assert context.get_task_output("task1") == {"result": "success"}
        
        # Check task history
        metadata = context.get_metadata()
        assert len(metadata["task_history"]) == 1
        assert metadata["task_history"][0]["task"] == "task1"
        assert metadata["task_history"][0]["status"] == "completed"
    
    def test_error_handling(self):
        """Test error handling in context."""
        context = PipelineContext()
        
        # Add error
        context.add_error("task1", "Test error")
        assert context.has_errors()
        
        errors = context.get_errors()
        assert len(errors) == 1
        assert errors[0]["task"] == "task1"
        assert errors[0]["error"] == "Test error"
    
    def test_context_clone(self):
        """Test context cloning."""
        context = PipelineContext({"data": [1, 2, 3]})
        context.add_task_output("task1", {"result": "test"})
        
        cloned = context.clone()
        assert cloned.get("data") == [1, 2, 3]
        assert cloned.get_task_output("task1") == {"result": "test"}
        
        # Modify original - clone should be unaffected
        context.set("data", [4, 5, 6])
        assert cloned.get("data") == [1, 2, 3]


class TestDAG:
    """Test DAG functionality."""
    
    def test_dag_creation(self):
        """Test DAG creation."""
        dag = DAG("test_dag")
        assert dag.name == "test_dag"
        assert len(dag.tasks) == 0
    
    def test_add_tasks(self):
        """Test adding tasks to DAG."""
        dag = DAG("test_dag")
        
        task1 = SimpleTask("task1")
        task2 = SimpleTask("task2")
        
        dag.add_task(task1)
        dag.add_task(task2)
        
        assert len(dag.tasks) == 2
        assert dag.get_task("task1") == task1
        assert dag.get_task("task2") == task2
    
    def test_add_dependencies(self):
        """Test adding dependencies between tasks."""
        dag = DAG("test_dag")
        
        task1 = SimpleTask("task1")
        task2 = SimpleTask("task2")
        task3 = SimpleTask("task3")
        
        dag.add_task(task1)
        dag.add_task(task2)
        dag.add_task(task3)
        
        dag.add_dependency("task1", "task2")
        dag.add_dependency("task2", "task3")
        
        assert "task1" in dag.get_dependencies("task2")
        assert "task2" in dag.get_dependencies("task3")
        assert "task2" in dag.get_dependents("task1")
        assert "task3" in dag.get_dependents("task2")
    
    def test_execution_order(self):
        """Test getting execution order."""
        dag = DAG("test_dag")
        
        task1 = SimpleTask("task1")
        task2 = SimpleTask("task2")
        task3 = SimpleTask("task3")
        
        dag.add_task(task1)
        dag.add_task(task2)
        dag.add_task(task3)
        
        dag.add_dependency("task1", "task2")
        dag.add_dependency("task2", "task3")
        
        order = dag.get_execution_order()
        assert order == ["task1", "task2", "task3"]
    
    def test_parallel_groups(self):
        """Test getting parallel execution groups."""
        dag = DAG("test_dag")
        
        # Create a DAG with parallel tasks
        tasks = [SimpleTask(f"task{i}") for i in range(5)]
        for task in tasks:
            dag.add_task(task)
        
        # task0 -> task1, task2
        # task1, task2 -> task3
        # task3 -> task4
        dag.add_dependency("task0", "task1")
        dag.add_dependency("task0", "task2")
        dag.add_dependency("task1", "task3")
        dag.add_dependency("task2", "task3")
        dag.add_dependency("task3", "task4")
        
        groups = dag.get_parallel_groups()
        assert groups[0] == ["task0"]
        assert set(groups[1]) == {"task1", "task2"}
        assert groups[2] == ["task3"]
        assert groups[3] == ["task4"]


@pytest.mark.asyncio
class TestTaskExecution:
    """Test task execution."""
    
    async def test_simple_task_execution(self):
        """Test simple task execution."""
        task = SimpleTask("test_task")
        context = PipelineContext()
        
        result_context = await task.run(context)
        
        assert task.executed
        assert task.status == TaskStatus.SUCCESS
        assert result_context.get("test_task_result") == "Result from test_task"
    
    async def test_task_failure(self):
        """Test task failure handling."""
        task = SimpleTask("failing_task", should_fail=True)
        context = PipelineContext()
        
        with pytest.raises(ValueError):
            await task.run(context)
        
        assert task.status == TaskStatus.FAILED
        assert task.error is not None
        assert context.has_errors()
    
    async def test_task_retry(self):
        """Test task retry mechanism."""
        # Create a task that fails twice then succeeds
        class FlakyTask(Task):
            def __init__(self, name: str):
                super().__init__(name, retries=3)
                self.attempt_count = 0
            
            async def execute(self, context: PipelineContext) -> PipelineContext:
                self.attempt_count += 1
                if self.attempt_count < 3:
                    raise ValueError("Temporary failure")
                context.set("attempts", self.attempt_count)
                return context
        
        task = FlakyTask("flaky_task")
        context = PipelineContext()
        
        result_context = await task.run(context)
        
        assert task.attempt_count == 3
        assert task.status == TaskStatus.SUCCESS
        assert result_context.get("attempts") == 3


@pytest.mark.asyncio
class TestDAGExecutor:
    """Test DAG executor."""
    
    async def test_sequential_execution(self):
        """Test sequential task execution."""
        dag = DAG("test_dag")
        
        task1 = SimpleTask("task1", delay=0.01)
        task2 = SimpleTask("task2", delay=0.01)
        task3 = SimpleTask("task3", delay=0.01)
        
        dag.add_task(task1)
        dag.add_task(task2)
        dag.add_task(task3)
        
        dag.add_dependency("task1", "task2")
        dag.add_dependency("task2", "task3")
        
        executor = DAGExecutor()
        context = PipelineContext()
        
        start_time = asyncio.get_event_loop().time()
        result_context = await executor.execute(dag, context)
        end_time = asyncio.get_event_loop().time()
        
        # All tasks should have executed
        assert task1.executed
        assert task2.executed
        assert task3.executed
        
        # Results should be in context
        assert result_context.get("task1_result") == "Result from task1"
        assert result_context.get("task2_result") == "Result from task2"
        assert result_context.get("task3_result") == "Result from task3"
        
        # Should take at least 0.03 seconds (sequential)
        assert end_time - start_time >= 0.025
    
    async def test_parallel_execution(self):
        """Test parallel task execution."""
        dag = DAG("test_dag")
        
        task1 = SimpleTask("task1", delay=0.02)
        task2 = SimpleTask("task2", delay=0.02)
        task3 = SimpleTask("task3", delay=0.02)
        
        dag.add_task(task1)
        dag.add_task(task2)
        dag.add_task(task3)
        
        # No dependencies - all tasks can run in parallel
        
        executor = DAGExecutor(max_workers=3)
        context = PipelineContext()
        
        start_time = asyncio.get_event_loop().time()
        result_context = await executor.execute(dag, context)
        end_time = asyncio.get_event_loop().time()
        
        # All tasks should have executed
        assert task1.executed
        assert task2.executed
        assert task3.executed
        
        # Should take less than 0.05 seconds (parallel)
        assert end_time - start_time < 0.05
    
    async def test_dag_failure_handling(self):
        """Test DAG failure handling."""
        dag = DAG("test_dag")
        
        task1 = SimpleTask("task1")
        task2 = SimpleTask("task2", should_fail=True)  # This will fail
        task3 = SimpleTask("task3")
        
        dag.add_task(task1)
        dag.add_task(task2)
        dag.add_task(task3)
        
        dag.add_dependency("task1", "task2")
        dag.add_dependency("task2", "task3")  # task3 depends on failing task2
        
        executor = DAGExecutor()
        context = PipelineContext()
        
        # Execution should complete even with failures
        result_context = await executor.execute(dag, context)
        
        assert task1.executed  # Should succeed
        assert task2.status == TaskStatus.FAILED  # Should fail
        assert not task3.executed  # Should not execute due to dependency failure
        
        assert result_context.has_errors()


if __name__ == "__main__":
    pytest.main([__file__])
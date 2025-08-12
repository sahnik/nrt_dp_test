"""DAG (Directed Acyclic Graph) implementation for task orchestration."""

from typing import Dict, List, Set, Optional
import networkx as nx
from networkx.algorithms.dag import topological_sort, is_directed_acyclic_graph
import logging

from .task import Task, TaskStatus
from .context import PipelineContext


class DAG:
    """
    Directed Acyclic Graph for managing task dependencies and execution order.
    """

    def __init__(self, name: str):
        """
        Initialize the DAG.
        
        Args:
            name: Name of the DAG
        """
        self.name = name
        self.graph = nx.DiGraph()
        self.tasks: Dict[str, Task] = {}
        self.logger = logging.getLogger(f"dag.{name}")

    def add_task(self, task: Task) -> None:
        """
        Add a task to the DAG.
        
        Args:
            task: Task instance to add
        """
        if task.name in self.tasks:
            raise ValueError(f"Task {task.name} already exists in DAG")
        
        self.tasks[task.name] = task
        self.graph.add_node(task.name)
        self.logger.debug(f"Added task {task.name} to DAG")

    def add_dependency(self, upstream_task: str, downstream_task: str) -> None:
        """
        Add a dependency between two tasks.
        
        Args:
            upstream_task: Task that must complete first
            downstream_task: Task that depends on upstream_task
        """
        if upstream_task not in self.tasks:
            raise ValueError(f"Task {upstream_task} not found in DAG")
        if downstream_task not in self.tasks:
            raise ValueError(f"Task {downstream_task} not found in DAG")
        
        self.graph.add_edge(upstream_task, downstream_task)
        self.tasks[downstream_task].add_dependency(upstream_task)
        self.logger.debug(f"Added dependency: {upstream_task} -> {downstream_task}")

    def set_dependencies(self, task_name: str, dependencies: List[str]) -> None:
        """
        Set multiple dependencies for a task.
        
        Args:
            task_name: Name of the dependent task
            dependencies: List of upstream task names
        """
        for dependency in dependencies:
            self.add_dependency(dependency, task_name)

    def validate(self) -> bool:
        """
        Validate that the DAG is acyclic.
        
        Returns:
            True if the graph is a valid DAG
        """
        if not is_directed_acyclic_graph(self.graph):
            self.logger.error("Graph contains cycles - not a valid DAG")
            return False
        return True

    def get_execution_order(self) -> List[str]:
        """
        Get the topological order of tasks for execution.
        
        Returns:
            List of task names in execution order
        """
        if not self.validate():
            raise ValueError("Cannot determine execution order - DAG contains cycles")
        
        return list(topological_sort(self.graph))

    def get_parallel_groups(self) -> List[List[str]]:
        """
        Get groups of tasks that can be executed in parallel.
        
        Returns:
            List of task groups, where each group can run in parallel
        """
        if not self.validate():
            raise ValueError("Cannot determine parallel groups - DAG contains cycles")
        
        # Calculate levels using topological generations
        generations = list(nx.topological_generations(self.graph))
        return [[task for task in generation] for generation in generations]

    def get_task(self, task_name: str) -> Optional[Task]:
        """
        Get a task by name.
        
        Args:
            task_name: Name of the task
            
        Returns:
            Task instance or None if not found
        """
        return self.tasks.get(task_name)

    def get_dependencies(self, task_name: str) -> Set[str]:
        """
        Get all dependencies for a task.
        
        Args:
            task_name: Name of the task
            
        Returns:
            Set of dependency task names
        """
        if task_name not in self.tasks:
            return set()
        
        return set(self.graph.predecessors(task_name))

    def get_dependents(self, task_name: str) -> Set[str]:
        """
        Get all tasks that depend on a given task.
        
        Args:
            task_name: Name of the task
            
        Returns:
            Set of dependent task names
        """
        if task_name not in self.tasks:
            return set()
        
        return set(self.graph.successors(task_name))

    def get_ready_tasks(self, completed_tasks: Set[str]) -> List[str]:
        """
        Get tasks that are ready to execute based on completed tasks.
        
        Args:
            completed_tasks: Set of task names that have completed
            
        Returns:
            List of task names ready for execution
        """
        ready_tasks = []
        
        for task_name, task in self.tasks.items():
            if task.status != TaskStatus.PENDING:
                continue
            
            dependencies = self.get_dependencies(task_name)
            if dependencies.issubset(completed_tasks):
                ready_tasks.append(task_name)
        
        return ready_tasks

    def reset(self) -> None:
        """Reset all tasks in the DAG to pending state."""
        for task in self.tasks.values():
            task.reset()

    def visualize(self) -> str:
        """
        Generate a text representation of the DAG structure.
        
        Returns:
            String representation of the DAG
        """
        lines = [f"DAG: {self.name}"]
        lines.append("=" * 40)
        
        # Show tasks and their dependencies
        for task_name in self.get_execution_order():
            task = self.tasks[task_name]
            deps = self.get_dependencies(task_name)
            
            if deps:
                deps_str = ", ".join(deps)
                lines.append(f"{task_name} <- [{deps_str}]")
            else:
                lines.append(f"{task_name} (no dependencies)")
        
        # Show parallel groups
        lines.append("")
        lines.append("Parallel Execution Groups:")
        lines.append("-" * 40)
        
        for i, group in enumerate(self.get_parallel_groups(), 1):
            lines.append(f"Group {i}: {', '.join(group)}")
        
        return "\n".join(lines)

    def to_dict(self) -> Dict:
        """
        Convert DAG to dictionary representation.
        
        Returns:
            Dictionary representation of the DAG
        """
        return {
            "name": self.name,
            "tasks": list(self.tasks.keys()),
            "edges": list(self.graph.edges()),
            "parallel_groups": self.get_parallel_groups()
        }

    def __repr__(self) -> str:
        """String representation of the DAG."""
        return f"DAG(name={self.name}, tasks={len(self.tasks)}, edges={self.graph.number_of_edges()})"
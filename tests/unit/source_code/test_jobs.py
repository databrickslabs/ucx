from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs

from databricks.labs.ucx.source_code.files import FileLoader, LocalFileResolver
from databricks.labs.ucx.source_code.graph import DependencyGraph, DependencyResolver
from databricks.labs.ucx.source_code.jobs import WorkflowTaskContainer


def test_workflow_task_container_build_dependency_graph_no_dependency_problems(mock_path_lookup):
    """No dependency problems"""
    ws = WorkspaceClient()
    task = jobs.Task(task_key="test")

    file_resolver = LocalFileResolver(FileLoader())
    dependency_resolver = DependencyResolver([file_resolver], mock_path_lookup)
    maybe = dependency_resolver.resolve_local_file(mock_path_lookup, Path("leaf1.py.txt"))
    graph = DependencyGraph(maybe.dependency, None, None, dependency_resolver, mock_path_lookup)

    workflow_task_container = WorkflowTaskContainer(ws, task)
    dependency_problems = workflow_task_container.build_dependency_graph(graph)

    assert len(dependency_problems) == 0

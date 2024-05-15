from pathlib import Path
from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import compute, jobs

from databricks.labs.ucx.source_code.files import FileLoader
from databricks.labs.ucx.source_code.graph import Dependency, DependencyGraph, DependencyResolver
from databricks.labs.ucx.source_code.jobs import WorkflowTaskContainer
from databricks.labs.ucx.source_code.site_packages import PipInstaller


@pytest.fixture
def graph(mock_path_lookup) -> DependencyGraph:
    dependency = Dependency(FileLoader(), Path("test"))
    installer = PipInstaller()
    dependency_resolver = DependencyResolver([], mock_path_lookup)
    graph = DependencyGraph(dependency, None, installer, dependency_resolver, mock_path_lookup)
    return graph


def test_workflow_task_container_build_dependency_graph_empty_task(mock_path_lookup, graph):
    """No dependency problems with empty task"""
    ws = create_autospec(WorkspaceClient)
    task = jobs.Task(task_key="test")

    workflow_task_container = WorkflowTaskContainer(ws, task)
    dependency_problems = workflow_task_container.build_dependency_graph(graph)

    assert len(dependency_problems) == 0
    ws.assert_not_called()


def test_workflow_task_container_build_dependency_graph_pytest_pypi_library(mock_path_lookup, graph):
    """Install pytest PyPi library"""
    ws = create_autospec(WorkspaceClient)
    libraries = [compute.Library(pypi=compute.PythonPyPiLibrary(package="pytest"))]
    task = jobs.Task(task_key="test", libraries=libraries)

    workflow_task_container = WorkflowTaskContainer(ws, task)
    dependency_problems = workflow_task_container.build_dependency_graph(graph)

    assert len(dependency_problems) == 0
    assert graph.path_lookup.resolve(Path("pytest")).exists()
    ws.assert_not_called()

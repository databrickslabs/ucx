from pathlib import Path
from unittest.mock import create_autospec

import pytest
from databricks.labs.ucx.source_code.python_libraries import PipResolver
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import compute, jobs

from databricks.labs.ucx.source_code.files import FileLoader
from databricks.labs.ucx.source_code.graph import Dependency, DependencyGraph, DependencyResolver
from databricks.labs.ucx.source_code.jobs import WorkflowTaskContainer
from databricks.labs.ucx.source_code.notebooks.loaders import NotebookResolver, NotebookLoader


@pytest.fixture
def graph(mock_path_lookup) -> DependencyGraph:
    dependency = Dependency(FileLoader(), Path("test"))
    dependency_resolver = DependencyResolver([PipResolver()], NotebookResolver(NotebookLoader()), [], mock_path_lookup)
    dependency_graph = DependencyGraph(dependency, None, dependency_resolver, mock_path_lookup)
    return dependency_graph


def test_workflow_task_container_builds_dependency_graph_not_yet_implemented(mock_path_lookup, graph):
    # Goal of test is to raise test coverage, remove after implementing
    ws = create_autospec(WorkspaceClient)
    library = compute.Library(jar="library.jar", egg="library.egg", whl="libary.whl", requirements="requirements.txt")
    task = jobs.Task(task_key="test", libraries=[library], existing_cluster_id="id")

    workflow_task_container = WorkflowTaskContainer(ws, task)
    problems = workflow_task_container.build_dependency_graph(graph)

    assert len(problems) == 5
    assert all(problem.code == "not-yet-implemented" for problem in problems)
    ws.assert_not_called()


def test_workflow_task_container_builds_dependency_graph_empty_task(mock_path_lookup, graph):
    ws = create_autospec(WorkspaceClient)
    task = jobs.Task(task_key="test")

    workflow_task_container = WorkflowTaskContainer(ws, task)
    problems = workflow_task_container.build_dependency_graph(graph)

    assert len(problems) == 0
    ws.assert_not_called()


def test_workflow_task_container_builds_dependency_graph_pytest_pypi_library(mock_path_lookup, graph):
    ws = create_autospec(WorkspaceClient)
    libraries = [compute.Library(pypi=compute.PythonPyPiLibrary(package="pytest"))]
    task = jobs.Task(task_key="test", libraries=libraries)

    workflow_task_container = WorkflowTaskContainer(ws, task)
    problems = workflow_task_container.build_dependency_graph(graph)

    assert len(problems) == 0
    assert graph.path_lookup.resolve(Path("pytest")).exists()
    ws.assert_not_called()


def test_workflow_task_container_builds_dependency_graph_unknown_pypi_library(mock_path_lookup, graph):
    ws = create_autospec(WorkspaceClient)
    libraries = [compute.Library(pypi=compute.PythonPyPiLibrary(package="unknown-library-name"))]
    task = jobs.Task(task_key="test", libraries=libraries)

    workflow_task_container = WorkflowTaskContainer(ws, task)
    problems = workflow_task_container.build_dependency_graph(graph)

    assert len(problems) == 1
    assert problems[0].code == "library-install-failed"
    assert problems[0].message.startswith("Failed to install unknown-library-name")
    assert mock_path_lookup.resolve(Path("unknown-library-name")) is None
    ws.assert_not_called()

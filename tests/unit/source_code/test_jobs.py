import logging
from pathlib import Path
from unittest.mock import create_autospec

import pytest
from databricks.labs.ucx.source_code.python_libraries import PipResolver
from databricks.labs.ucx.source_code.whitelist import Whitelist
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import compute, jobs

from databricks.labs.ucx.source_code.files import FileLoader, ImportFileResolver
from databricks.labs.ucx.source_code.graph import Dependency, DependencyGraph, DependencyResolver
from databricks.labs.ucx.source_code.jobs import JobProblem, WorkflowLinter, WorkflowTaskContainer
from databricks.labs.ucx.source_code.notebooks.loaders import NotebookResolver, NotebookLoader


def test_job_problem_as_message():
    expected_message = "UNKNOWN:-1 [library-not-found] Library not found: lib"

    problem = JobProblem(
        1234, "test-job", "test-task", "UNKNOWN", "library-not-found", "Library not found: lib", -1, -1, -1, -1
    )

    assert problem.as_message() == expected_message


@pytest.fixture
def dependency_resolver(mock_path_lookup) -> DependencyResolver:
    file_loader = FileLoader()
    whitelist = Whitelist()
    resolver = DependencyResolver(
        [PipResolver(file_loader, whitelist)],
        NotebookResolver(NotebookLoader()),
        ImportFileResolver(file_loader, whitelist),
        mock_path_lookup,
    )
    return resolver


@pytest.fixture
def graph(mock_path_lookup, dependency_resolver) -> DependencyGraph:
    dependency = Dependency(FileLoader(), Path("test"))
    dependency_graph = DependencyGraph(dependency, None, dependency_resolver, mock_path_lookup)
    return dependency_graph


def test_workflow_task_container_builds_dependency_graph_not_yet_implemented(mock_path_lookup, graph):
    # Goal of test is to raise test coverage, remove after implementing
    ws = create_autospec(WorkspaceClient)
    library = compute.Library(jar="library.jar", egg="library.egg", whl="library.whl", requirements="requirements.txt")
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
    libraries = [compute.Library(pypi=compute.PythonPyPiLibrary(package="demo-egg"))]  # installs pkgdir
    task = jobs.Task(task_key="test", libraries=libraries)

    workflow_task_container = WorkflowTaskContainer(ws, task)
    problems = workflow_task_container.build_dependency_graph(graph)

    assert len(problems) == 0
    assert graph.path_lookup.resolve(Path("pkgdir")).exists()
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


def test_workflow_linter_lint_job_logs_problems(dependency_resolver, mock_path_lookup, empty_index, caplog):
    expected_message = "Found job problems:\nUNKNOWN:-1 [library-install-failed] Failed to install unknown-library"

    ws = create_autospec(WorkspaceClient)
    linter = WorkflowLinter(ws, dependency_resolver, mock_path_lookup, empty_index)

    libraries = [compute.Library(pypi=compute.PythonPyPiLibrary(package="unknown-library-name"))]
    task = jobs.Task(task_key="test-task", libraries=libraries)
    settings = jobs.JobSettings(name="test-job", tasks=[task])
    job = jobs.Job(job_id=1234, settings=settings)

    ws.jobs.get.return_value = job

    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.source_code.jobs"):
        linter.lint_job(1234)

    assert any(message.startswith(expected_message) for message in caplog.messages)

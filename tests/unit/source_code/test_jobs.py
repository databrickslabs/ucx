import io
import logging
from pathlib import Path
from unittest.mock import create_autospec

import pytest
from databricks.sdk.service.jobs import Job
from databricks.sdk.service.pipelines import NotebookLibrary, GetPipelineResponse, PipelineLibrary, FileLibrary

from databricks.labs.ucx.source_code.python_libraries import PythonLibraryResolver
from databricks.labs.ucx.source_code.known import Whitelist
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import compute, jobs, pipelines
from databricks.sdk.service.workspace import ExportFormat

from databricks.labs.ucx.source_code.linters.files import FileLoader, ImportFileResolver
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
        PythonLibraryResolver(whitelist),
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
    library = compute.Library(jar="library.jar")
    task = jobs.Task(task_key="test", libraries=[library], existing_cluster_id="id")

    workflow_task_container = WorkflowTaskContainer(ws, task, Job())
    problems = workflow_task_container.build_dependency_graph(graph)

    assert len(problems) == 1
    assert all(problem.code == "not-yet-implemented" for problem in problems)
    ws.assert_not_called()


def test_workflow_task_container_builds_dependency_graph_empty_task(mock_path_lookup, graph):
    ws = create_autospec(WorkspaceClient)
    task = jobs.Task(task_key="test")

    workflow_task_container = WorkflowTaskContainer(ws, task, Job())
    problems = workflow_task_container.build_dependency_graph(graph)

    assert len(problems) == 0
    ws.assert_not_called()


def test_workflow_task_container_builds_dependency_graph_pytest_pypi_library(mock_path_lookup, graph):
    ws = create_autospec(WorkspaceClient)
    libraries = [compute.Library(pypi=compute.PythonPyPiLibrary(package="demo-egg"))]  # installs pkgdir
    task = jobs.Task(task_key="test", libraries=libraries)

    workflow_task_container = WorkflowTaskContainer(ws, task, Job())
    problems = workflow_task_container.build_dependency_graph(graph)

    assert len(problems) == 0
    assert graph.path_lookup.resolve(Path("pkgdir")).exists()
    ws.assert_not_called()


def test_workflow_task_container_builds_dependency_graph_unknown_pypi_library(mock_path_lookup, graph):
    ws = create_autospec(WorkspaceClient)
    libraries = [compute.Library(pypi=compute.PythonPyPiLibrary(package="unknown-library-name"))]
    task = jobs.Task(task_key="test", libraries=libraries)

    workflow_task_container = WorkflowTaskContainer(ws, task, Job())
    problems = workflow_task_container.build_dependency_graph(graph)

    assert len(problems) == 1
    assert problems[0].code == "library-install-failed"
    assert problems[0].message.startswith("Failed to install unknown-library-name")
    assert mock_path_lookup.resolve(Path("unknown-library-name")) is None
    ws.assert_not_called()


def test_workflow_task_container_builds_dependency_graph_for_python_wheel(mock_path_lookup, graph):
    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.return_value = io.BytesIO(b"test")

    libraries = [compute.Library(whl="test.whl")]
    task = jobs.Task(task_key="test", libraries=libraries)

    workflow_task_container = WorkflowTaskContainer(ws, task, Job())
    problems = workflow_task_container.build_dependency_graph(graph)

    assert len(problems) == 1
    assert problems[0].code == "library-install-failed"
    assert problems[0].message.startswith("Failed to install")
    assert mock_path_lookup.resolve(Path("test")) is None
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


def test_workflow_task_container_builds_dependency_graph_for_requirements_txt(mock_path_lookup, graph):
    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.return_value = io.BytesIO(b"test")

    libraries = [compute.Library(requirements="requirements.txt")]
    task = jobs.Task(task_key="test", libraries=libraries)

    workflow_task_container = WorkflowTaskContainer(ws, task, Job())
    problems = workflow_task_container.build_dependency_graph(graph)

    assert len(problems) == 1
    assert problems[0].code == "library-install-failed"
    assert problems[0].message.startswith("Failed to install")
    assert mock_path_lookup.resolve(Path("test")) is None
    ws.assert_not_called()


def test_workflow_task_container_build_dependency_graph_warns_about_reference_to_other_requirements(
    mock_path_lookup, graph, caplog
):
    expected_message = "References to other requirements file is not supported: -r other-requirements.txt"

    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.return_value = io.BytesIO(b"-r other-requirements.txt")

    libraries = [compute.Library(requirements="requirements.txt")]
    task = jobs.Task(task_key="test", libraries=libraries)

    workflow_task_container = WorkflowTaskContainer(ws, task, Job())

    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.source_code.jobs"):
        workflow_task_container.build_dependency_graph(graph)

    assert expected_message in caplog.messages
    ws.workspace.download.assert_called_once_with("requirements.txt", format=ExportFormat.AUTO)


def test_workflow_task_container_build_dependency_graph_warns_about_reference_to_constrains(
    mock_path_lookup, graph, caplog
):
    expected_message = "References to constrains file is not supported: -c constrains.txt"

    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.return_value = io.BytesIO(b"-c constrains.txt")

    libraries = [compute.Library(requirements="requirements.txt")]
    task = jobs.Task(task_key="test", libraries=libraries)

    workflow_task_container = WorkflowTaskContainer(ws, task, Job())

    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.source_code.jobs"):
        workflow_task_container.build_dependency_graph(graph)

    assert expected_message in caplog.messages
    ws.workspace.download.assert_called_once_with("requirements.txt", format=ExportFormat.AUTO)


def test_workflow_task_container_with_existing_cluster_builds_dependency_graph_pytest_pypi_library(
    mock_path_lookup, graph
):
    ws = create_autospec(WorkspaceClient)
    libraries = []
    existing_cluster_id = "TEST_CLUSTER_ID"
    task = jobs.Task(task_key="test", libraries=libraries, existing_cluster_id=existing_cluster_id)
    libraries_api = create_autospec(compute.LibrariesAPI)
    libraries_api.cluster_status.return_value = [
        compute.LibraryFullStatus(
            is_library_for_all_clusters=False,
            library=compute.Library(
                cran=None,
                egg=None,
                jar=None,
                maven=None,
                pypi=compute.PythonPyPiLibrary(package='pandas', repo=None),
                requirements=None,
                whl=None,
            ),
            messages=None,
            status="<LibraryInstallStatus.PENDING: 'PENDING'>",
        )
    ]

    workflow_task_container = WorkflowTaskContainer(ws, task, Job())
    problems = workflow_task_container.build_dependency_graph(graph)
    assert len(problems) == 0
    ws.assert_not_called()


def test_workflow_task_container_builds_dependency_graph_with_unknown_egg_library(mock_path_lookup, graph):
    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.return_value = io.BytesIO(b"test")

    unknown_library = "/path/to/unknown/library.egg"
    libraries = [compute.Library(egg=unknown_library)]
    task = jobs.Task(task_key="test", libraries=libraries)

    workflow_task_container = WorkflowTaskContainer(ws, task, Job())
    problems = workflow_task_container.build_dependency_graph(graph)

    assert len(problems) == 1
    assert problems[0].code == "library-install-failed"
    assert problems[0].message.startswith("Failed to install")
    assert graph.path_lookup.resolve(Path(unknown_library)) is None
    ws.workspace.download.assert_called_once_with(unknown_library, format=ExportFormat.AUTO)


def test_workflow_task_container_builds_dependency_graph_with_known_egg_library(mock_path_lookup, graph):
    ws = create_autospec(WorkspaceClient)

    egg_file = Path(__file__).parent / "samples/distribution/dist/thingy-0.0.1-py3.10.egg"
    with egg_file.open("rb") as f:
        ws.workspace.download.return_value = io.BytesIO(f.read())

    libraries = [compute.Library(egg=egg_file.as_posix())]
    task = jobs.Task(task_key="test", libraries=libraries)

    workflow_task_container = WorkflowTaskContainer(ws, task, Job())
    problems = workflow_task_container.build_dependency_graph(graph)

    assert len(problems) == 0
    assert graph.path_lookup.resolve(Path("thingy")) is not None
    ws.workspace.download.assert_called_once_with(egg_file.as_posix(), format=ExportFormat.AUTO)


def test_workflow_task_container_builds_dependency_graph_with_missing_distribution_in_python_wheel_task(
    mock_path_lookup,
    graph,
):
    ws = create_autospec(WorkspaceClient)
    python_wheel_task = jobs.PythonWheelTask(package_name="databricks_labs_ucx", entry_point="runtime")
    task = jobs.Task(task_key="test", python_wheel_task=python_wheel_task)

    workflow_task_container = WorkflowTaskContainer(ws, task, Job())
    problems = workflow_task_container.build_dependency_graph(graph)

    assert len(problems) == 1
    assert problems[0].code == "distribution-not-found"
    assert problems[0].message == "Could not find distribution for databricks_labs_ucx"
    ws.assert_not_called()


def test_workflow_task_container_builds_dependency_graph_with_missing_entrypoint_in_python_wheel_task(graph):
    ws = create_autospec(WorkspaceClient)

    whl_file = Path(__file__).parent / "samples/distribution/dist/thingy-0.0.1-py2.py3-none-any.whl"
    with whl_file.open("rb") as f:
        ws.workspace.download.return_value = io.BytesIO(f.read())

    python_wheel_task = jobs.PythonWheelTask(package_name="thingy", entry_point="non_existing_entrypoint")
    libraries = [compute.Library(whl=whl_file.as_posix())]
    task = jobs.Task(task_key="test", libraries=libraries, python_wheel_task=python_wheel_task)

    workflow_task_container = WorkflowTaskContainer(ws, task, Job())
    problems = workflow_task_container.build_dependency_graph(graph)

    assert len(problems) == 1
    assert problems[0].code == "distribution-entry-point-not-found"
    assert problems[0].message == "Could not find distribution entry point for thingy.non_existing_entrypoint"
    ws.workspace.download.assert_called_once_with(whl_file.as_posix(), format=ExportFormat.AUTO)


def test_workflow_task_container_builds_dependency_graph_for_python_wheel_task(graph):
    ws = create_autospec(WorkspaceClient)

    whl_file = Path(__file__).parent / "samples/distribution/dist/thingy-0.0.1-py2.py3-none-any.whl"
    with whl_file.open("rb") as f:
        ws.workspace.download.return_value = io.BytesIO(f.read())

    python_wheel_task = jobs.PythonWheelTask(package_name="thingy", entry_point="runtime")
    libraries = [compute.Library(whl=whl_file.as_posix())]
    task = jobs.Task(task_key="test", libraries=libraries, python_wheel_task=python_wheel_task)

    workflow_task_container = WorkflowTaskContainer(ws, task, Job())
    problems = workflow_task_container.build_dependency_graph(graph)

    assert len(problems) == 0
    ws.workspace.download.assert_called_once_with(whl_file.as_posix(), format=ExportFormat.AUTO)


def test_workflow_linter_dlt_pipeline_task(graph):
    ws = create_autospec(WorkspaceClient)
    pipeline = ws.pipelines.create(continous=False, name="test-pipeline")
    task = jobs.Task(task_key="test", pipeline_task=jobs.PipelineTask(pipeline_id=pipeline.pipeline_id))

    # no spec in GetPipelineResponse
    ws.pipelines.get.return_value = GetPipelineResponse(
        pipeline_id=pipeline.pipeline_id,
        name="test-pipeline",
    )

    workflow_task_container = WorkflowTaskContainer(ws, task, Job())
    problems = workflow_task_container.build_dependency_graph(graph)
    assert len(problems) == 0

    # no libraries in GetPipelineResponse.spec
    ws.pipelines.get.return_value = GetPipelineResponse(
        pipeline_id=pipeline.pipeline_id,
        name="test-pipeline",
        spec=pipelines.PipelineSpec(continuous=False),
    )

    workflow_task_container = WorkflowTaskContainer(ws, task, Job())
    problems = workflow_task_container.build_dependency_graph(graph)
    assert len(problems) == 0

    ws.pipelines.get.return_value = GetPipelineResponse(
        pipeline_id=pipeline.pipeline_id,
        name="test-pipeline",
        spec=pipelines.PipelineSpec(
            libraries=[
                PipelineLibrary(
                    jar="some.jar",
                    maven=compute.MavenLibrary(coordinates="com.example:example:1.0.0"),
                    notebook=NotebookLibrary(path="test.py"),
                    file=FileLibrary(path="test.txt"),
                )
            ]
        ),
    )
    workflow_task_container = WorkflowTaskContainer(ws, task, Job())
    problems = workflow_task_container.build_dependency_graph(graph)
    assert len(problems) == 4
    ws.assert_not_called()


def test_xxx(graph):
    ws = create_autospec(WorkspaceClient)
    notebook_task = jobs.NotebookTask(
        notebook_path="test",
        base_parameters={"a": "b", "c": "dbfs:/mnt/foo"},
    )
    task = jobs.Task(
        task_key="test",
        job_cluster_key="main",
        notebook_task=notebook_task,
    )
    job = Job(
        settings=jobs.JobSettings(
            job_clusters=[
                jobs.JobCluster(
                    job_cluster_key="main",
                    new_cluster=compute.ClusterSpec(
                        spark_version="15.2.x-photon-scala2.12",
                        node_type_id="Standard_F4s",
                        num_workers=2,
                        data_security_mode=compute.DataSecurityMode.LEGACY_TABLE_ACL,
                        spark_conf={"spark.databricks.cluster.profile": "singleNode"},
                    ),
                ),
            ],
        ),
    )

    workflow_task_container = WorkflowTaskContainer(ws, task, job)
    _ = workflow_task_container.build_dependency_graph(graph)

    assert workflow_task_container.named_parameters == {'a': 'b', 'c': 'dbfs:/mnt/foo'}
    assert workflow_task_container.data_security_mode == compute.DataSecurityMode.LEGACY_TABLE_ACL
    assert workflow_task_container.runtime_version == (15, 2)
    assert workflow_task_container.spark_conf == {"spark.databricks.cluster.profile": "singleNode"}

    ws.assert_not_called()

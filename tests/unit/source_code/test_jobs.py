import io
import logging
import textwrap
from pathlib import Path
from unittest.mock import create_autospec

import pytest
from databricks.labs.lsql.backends import MockBackend
from databricks.sdk.service.compute import LibraryInstallStatus
from databricks.sdk.service.jobs import Job, SparkPythonTask
from databricks.sdk.service.pipelines import NotebookLibrary, GetPipelineResponse, PipelineLibrary, FileLibrary

from databricks.labs.blueprint.paths import DBFSPath, WorkspacePath
from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.directfs_access import DirectFsAccessCrawler
from databricks.labs.ucx.source_code.python_libraries import PythonLibraryResolver
from databricks.labs.ucx.source_code.known import KnownList
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service import compute, jobs, pipelines
from databricks.sdk.service.workspace import ExportFormat, ObjectInfo, Language

from databricks.labs.ucx.source_code.linters.files import FileLoader, ImportFileResolver
from databricks.labs.ucx.source_code.graph import (
    Dependency,
    DependencyGraph,
    DependencyResolver,
)
from databricks.labs.ucx.source_code.jobs import JobProblem, WorkflowLinter, WorkflowTaskContainer, LintingWalker
from databricks.labs.ucx.source_code.notebooks.loaders import NotebookResolver, NotebookLoader
from databricks.labs.ucx.source_code.used_table import UsedTablesCrawler


def test_job_problem_as_message() -> None:
    expected_message = "UNKNOWN:-1 [library-not-found] Library not found: lib"

    problem = JobProblem(
        1234, "test-job", "test-task", "UNKNOWN", "library-not-found", "Library not found: lib", -1, -1, -1, -1
    )

    assert problem.as_message() == expected_message


@pytest.fixture
def dependency_resolver(mock_path_lookup) -> DependencyResolver:
    file_loader = FileLoader()
    allow_list = KnownList()
    import_file_resolver = ImportFileResolver(file_loader, allow_list)
    resolver = DependencyResolver(
        PythonLibraryResolver(allow_list),
        NotebookResolver(NotebookLoader()),
        import_file_resolver,
        import_file_resolver,
        mock_path_lookup,
    )
    return resolver


@pytest.fixture
def graph(mock_path_lookup, dependency_resolver) -> DependencyGraph:
    dependency = Dependency(FileLoader(), Path("test"))
    dependency_graph = DependencyGraph(dependency, None, dependency_resolver, mock_path_lookup, CurrentSessionState())
    return dependency_graph


def test_workflow_task_container_builds_dependency_graph_not_yet_implemented(mock_path_lookup, graph) -> None:
    # Goal of test is to raise test coverage, remove after implementing
    ws = create_autospec(WorkspaceClient)
    library = compute.Library(jar="library.jar")
    task = jobs.Task(task_key="test", libraries=[library], existing_cluster_id="id")

    workflow_task_container = WorkflowTaskContainer(ws, task, Job())
    problems = workflow_task_container.build_dependency_graph(graph)

    assert len(problems) == 1
    assert all(problem.code == "not-yet-implemented" for problem in problems)
    ws.assert_not_called()


def test_workflow_task_container_builds_dependency_graph_empty_task(mock_path_lookup, graph) -> None:
    ws = create_autospec(WorkspaceClient)
    task = jobs.Task(task_key="test")

    workflow_task_container = WorkflowTaskContainer(ws, task, Job())
    problems = workflow_task_container.build_dependency_graph(graph)

    assert len(problems) == 0
    ws.assert_not_called()


def test_workflow_task_container_builds_dependency_graph_pytest_pypi_library(mock_path_lookup, graph) -> None:
    ws = create_autospec(WorkspaceClient)
    libraries = [compute.Library(pypi=compute.PythonPyPiLibrary(package="demo-egg"))]  # installs pkgdir
    task = jobs.Task(task_key="test", libraries=libraries)

    workflow_task_container = WorkflowTaskContainer(ws, task, Job())
    problems = workflow_task_container.build_dependency_graph(graph)

    assert len(problems) == 0
    assert graph.path_lookup.resolve(Path("pkgdir")).exists()
    ws.assert_not_called()


def test_workflow_task_container_builds_dependency_graph_unknown_pypi_library(mock_path_lookup, graph) -> None:
    ws = create_autospec(WorkspaceClient)
    libraries = [compute.Library(pypi=compute.PythonPyPiLibrary(package="unknown-library-name"))]
    task = jobs.Task(task_key="test", libraries=libraries)

    workflow_task_container = WorkflowTaskContainer(ws, task, Job())
    problems = workflow_task_container.build_dependency_graph(graph)

    assert len(problems) == 1
    assert problems[0].code == "library-install-failed"
    assert problems[0].message.startswith("'pip --disable-pip-version-check install unknown-library-name")
    assert mock_path_lookup.resolve(Path("unknown-library-name")) is None
    ws.assert_not_called()


@pytest.fixture(scope="function")
def ws_pkg(request: pytest.FixtureRequest) -> WorkspaceClient:
    ws = create_autospec(WorkspaceClient)  # pylint: disable=mock-no-usage
    data = io.BytesIO(b"unimportant")
    match request.param:
        case "wspath":
            ws.workspace.download.return_value = data
        case "dbfs":
            ws.dbfs.open.return_value = data
    return ws


@pytest.mark.parametrize(
    "ws_pkg, library, registered",
    (
        (
            "wspath",
            compute.Library(whl="/path/to/some/some_library-py3-none-any.whl"),
            "some_library-py3-none-any.whl",
        ),
        (
            "wspath",
            compute.Library(egg="/path/to/some/some_library-py3.10.egg"),
            "some_library-py3.10.egg",
        ),
        (
            "dbfs",
            compute.Library(whl="dbfs:/path/to/some/some_library-py3-none-any.whl"),
            "some_library-py3-none-any.whl",
        ),
        (
            "dbfs",
            compute.Library(egg="dbfs:/path/to/some/some_library-py3.10.egg"),
            "some_library-py3.10.egg",
        ),
    ),
    indirect=("ws_pkg",),
)
def test_builds_dependency_graph_for_local_package(
    ws_pkg: WorkspaceClient, library: compute.Library, registered: str
) -> None:
    """Check that the (local) libraries for a task are registered with the dependency graph."""
    dep_graph = create_autospec(DependencyGraph)

    task = jobs.Task(task_key="test", libraries=[library])

    workflow_task_container = WorkflowTaskContainer(ws_pkg, task, Job())
    _ = workflow_task_container.build_dependency_graph(dep_graph)

    dep_graph.register_library.assert_called_once()
    registered_libraries = tuple(library for args in dep_graph.register_library.call_args_list for library in args[0])
    registered_names = tuple(Path(library).name for library in registered_libraries)
    assert registered_names == (registered,)


@pytest.fixture(scope="function")
def ws_requirements(request: pytest.FixtureRequest) -> WorkspaceClient:
    ws = create_autospec(WorkspaceClient)  # pylint: disable=mock-no-usage
    data = io.BytesIO(
        textwrap.dedent(
            """\
            some_library==0.10.0
            databricks-cli==0.17.5
            """
        ).encode("utf-8")
    )
    match request.param:
        case "wspath":
            ws.workspace.download.return_value = data
        case "dbfs":
            ws.dbfs.open.return_value = data
    return ws


@pytest.mark.parametrize(
    "ws_requirements, path",
    (
        ("wspath", "/path/to/requirements.txt"),
        ("dbfs", "dbfs:/path/to/requirements.txt"),
    ),
    indirect=("ws_requirements",),
)
def test_builds_dependency_graph_for_requirements(ws_requirements: WorkspaceClient, path: str) -> None:
    """Check that the task dependencies listed in a requirements.txt are registered with the dependency graph."""
    dep_graph = create_autospec(DependencyGraph)  # pylint: disable=mock-no-usage

    library = compute.Library(requirements=path)
    task = jobs.Task(task_key="test", libraries=[library])

    workflow_task_container = WorkflowTaskContainer(ws_requirements, task, Job())
    _ = workflow_task_container.build_dependency_graph(dep_graph)

    registered_libraries = tuple(library for args in dep_graph.register_library.call_args_list for library in args[0])
    assert registered_libraries == ("some_library==0.10.0", "databricks-cli==0.17.5")


@pytest.mark.parametrize(
    "python_file,expected_cls,expected_path",
    (
        ("/path/to/file.py", WorkspacePath, "/path/to/file.py"),
        ("dbfs:/path/to/another/file.py", DBFSPath, "/path/to/another/file.py"),
    ),
)
def test_workflow_task_container_builds_dependency_graph_spark_python_task(
    python_file: str, expected_cls, expected_path: str
) -> None:
    """Verify that spark python tasks from the workspace are registered with the graph."""
    ws = create_autospec(WorkspaceClient)  # pylint: disable=mock-no-usage
    dep_graph = create_autospec(DependencyGraph)  # pylint: disable=mock-no-usage

    task = jobs.Task(task_key="test", spark_python_task=SparkPythonTask(python_file=python_file))
    workflow_task_container = WorkflowTaskContainer(ws, task, Job())
    _ = workflow_task_container.build_dependency_graph(dep_graph)

    expected_path_instance = expected_cls(ws, expected_path)

    registered_notebooks = []
    for call in dep_graph.register_file.call_args_list:
        registered_notebooks.append(call.args[0])
    assert registered_notebooks == [expected_path_instance]


def test_workflow_linter_lint_job_logs_problems(dependency_resolver, mock_path_lookup, empty_index, caplog) -> None:
    expected_message = "Found job problems:\nUNKNOWN:-1 [library-install-failed] 'pip --disable-pip-version-check install unknown-library"

    ws = create_autospec(WorkspaceClient)
    directfs_crawler = create_autospec(DirectFsAccessCrawler)
    used_tables_crawler = create_autospec(UsedTablesCrawler)
    linter = WorkflowLinter(
        ws, dependency_resolver, mock_path_lookup, empty_index, directfs_crawler, used_tables_crawler
    )

    libraries = [compute.Library(pypi=compute.PythonPyPiLibrary(package="unknown-library-name"))]
    task = jobs.Task(task_key="test-task", libraries=libraries)
    settings = jobs.JobSettings(name="test-job", tasks=[task])
    job = jobs.Job(job_id=1234, settings=settings)

    ws.jobs.get.return_value = job
    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.source_code.jobs"):
        linter.lint_job(1234)

    directfs_crawler.assert_not_called()
    used_tables_crawler.assert_not_called()
    assert any(message.startswith(expected_message) for message in caplog.messages)


def test_workflow_task_container_builds_dependency_graph_for_requirements_txt(mock_path_lookup, graph) -> None:
    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.return_value = io.BytesIO(b"test")

    libraries = [compute.Library(requirements="/path/to/requirements.txt")]
    task = jobs.Task(task_key="test", libraries=libraries)

    workflow_task_container = WorkflowTaskContainer(ws, task, Job())
    problems = workflow_task_container.build_dependency_graph(graph)

    assert len(problems) == 1
    assert problems[0].code == "library-install-failed"
    assert problems[0].message.startswith("'pip --disable-pip-version-check install test")
    assert mock_path_lookup.resolve(Path("test")) is None
    ws.assert_not_called()


def test_workflow_task_container_build_dependency_graph_warns_about_reference_to_other_requirements(
    mock_path_lookup, graph, caplog
) -> None:
    expected_message = "Reference to other requirements file is not supported: -r other-requirements.txt"

    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.return_value = io.BytesIO(b"-r other-requirements.txt")

    libraries = [compute.Library(requirements="/path/to/requirements.txt")]
    task = jobs.Task(task_key="test", libraries=libraries)

    workflow_task_container = WorkflowTaskContainer(ws, task, Job())

    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.source_code.jobs"):
        workflow_task_container.build_dependency_graph(graph)

    assert expected_message in caplog.messages
    ws.workspace.download.assert_called_once_with("/path/to/requirements.txt", format=ExportFormat.AUTO)


def test_workflow_task_container_build_dependency_graph_warns_about_reference_to_constraints(
    mock_path_lookup, graph, caplog
) -> None:
    expected_message = "Reference to constraints file is not supported: -c constraints.txt"

    ws = create_autospec(WorkspaceClient)
    ws.workspace.download.return_value = io.BytesIO(b"-c constraints.txt")

    libraries = [compute.Library(requirements="/path/to/requirements.txt")]
    task = jobs.Task(task_key="test", libraries=libraries)

    workflow_task_container = WorkflowTaskContainer(ws, task, Job())

    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.source_code.jobs"):
        workflow_task_container.build_dependency_graph(graph)

    assert expected_message in caplog.messages
    ws.workspace.download.assert_called_once_with("/path/to/requirements.txt", format=ExportFormat.AUTO)


def test_workflow_task_container_with_existing_cluster_builds_dependency_graph_pytest_pypi_library(
    mock_path_lookup, graph
) -> None:
    ws = create_autospec(WorkspaceClient)
    libraries: list[compute.Library] = []
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
            status=LibraryInstallStatus.PENDING,
        )
    ]

    workflow_task_container = WorkflowTaskContainer(ws, task, Job())
    problems = workflow_task_container.build_dependency_graph(graph)
    assert len(problems) == 0
    ws.assert_not_called()


def test_workflow_task_container_builds_dependency_graph_with_unknown_egg_library(mock_path_lookup, graph) -> None:
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


def test_workflow_task_container_builds_dependency_graph_with_known_egg_library(mock_path_lookup, graph) -> None:
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
) -> None:
    ws = create_autospec(WorkspaceClient)
    python_wheel_task = jobs.PythonWheelTask(package_name="databricks_labs_ucx", entry_point="runtime")
    task = jobs.Task(task_key="test", python_wheel_task=python_wheel_task)

    workflow_task_container = WorkflowTaskContainer(ws, task, Job())
    problems = workflow_task_container.build_dependency_graph(graph)

    assert len(problems) == 1
    assert problems[0].code == "distribution-not-found"
    assert problems[0].message == "Could not find distribution for databricks_labs_ucx"
    ws.assert_not_called()


def test_workflow_task_container_builds_dependency_graph_with_missing_entrypoint_in_python_wheel_task(graph) -> None:
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


def test_workflow_task_container_builds_dependency_graph_for_python_wheel_task(graph) -> None:
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


def test_workflow_linter_dlt_pipeline_task(graph) -> None:
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
                    notebook=NotebookLibrary(path="/path/to/test.py"),
                    file=FileLibrary(path="test.txt"),
                )
            ]
        ),
    )
    ws.workspace.get_status.side_effect = NotFound("Simulated workspace file not found.")
    workflow_task_container = WorkflowTaskContainer(ws, task, Job())
    problems = workflow_task_container.build_dependency_graph(graph)
    assert len(problems) == 4
    ws.assert_not_called()


def test_xxx(graph) -> None:
    ws = create_autospec(WorkspaceClient)
    ws.workspace.get_status.side_effect = NotFound("Simulated workspace file not found.")
    notebook_task = jobs.NotebookTask(
        notebook_path="/path/to/test",
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


def test_linting_walker_populates_paths(dependency_resolver, mock_path_lookup, migration_index) -> None:
    path = mock_path_lookup.resolve(Path("functional/values_across_cells.py"))
    root = Dependency(NotebookLoader(), path)
    xgraph = DependencyGraph(root, None, dependency_resolver, mock_path_lookup, CurrentSessionState())
    walker = LintingWalker(xgraph, set(), mock_path_lookup, "key", CurrentSessionState(), migration_index)
    advices = 0
    for advice in walker:
        advices += 1
        assert "UNKNOWN" not in advice.path.as_posix()
    assert advices


def test_workflow_linter_refresh_report(dependency_resolver, mock_path_lookup, migration_index) -> None:
    ws = create_autospec(WorkspaceClient)
    ws.workspace.get_status.return_value = ObjectInfo(object_id=123, language=Language.PYTHON)
    some_things = mock_path_lookup.resolve(Path("functional/zoo.py"))
    assert some_things is not None
    ws.workspace.download.return_value = some_things.read_bytes()
    notebook_task = jobs.NotebookTask(
        notebook_path=some_things.absolute().as_posix(),
        base_parameters={"a": "b", "c": "dbfs:/mnt/foo"},
    )
    task = jobs.Task(
        task_key="test",
        job_cluster_key="main",
        notebook_task=notebook_task,
    )
    settings = jobs.JobSettings(
        tasks=[task],
        name='some',
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
    )
    ws.jobs.list.return_value = [Job(job_id=1), Job(job_id=2, settings=settings)]
    ws.jobs.get.return_value = Job(job_id=2, settings=settings)

    sql_backend = MockBackend()
    directfs_crawler = DirectFsAccessCrawler.for_paths(sql_backend, "test")
    used_tables_crawler = UsedTablesCrawler.for_paths(sql_backend, "test")
    linter = WorkflowLinter(
        ws,
        dependency_resolver,
        mock_path_lookup,
        migration_index,
        directfs_crawler,
        used_tables_crawler,
        [1],
    )
    linter.refresh_report(sql_backend, 'test')

    sql_backend.has_rows_written_for('test.workflow_problems')
    sql_backend.has_rows_written_for('hive_metastore.test.used_tables_in_paths')
    sql_backend.has_rows_written_for('hive_metastore.test.directfs_in_paths')

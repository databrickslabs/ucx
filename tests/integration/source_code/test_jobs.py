import io
import logging
import shutil
from collections.abc import Callable
from dataclasses import replace
from datetime import timedelta
from io import StringIO
from pathlib import Path
from unittest.mock import create_autospec

import pytest
from databricks.labs.blueprint.paths import DBFSPath, WorkspacePath
from databricks.labs.blueprint.tui import Prompts
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service.compute import Library, PythonPyPiLibrary
from databricks.sdk.service.pipelines import NotebookLibrary
from databricks.sdk.service.workspace import ImportFormat, Language

from databricks.labs.ucx.hive_metastore.migration_status import MigrationIndex
from databricks.labs.ucx.mixins.fixtures import get_purge_suffix, factory
from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.graph import Dependency
from databricks.labs.ucx.source_code.known import UNKNOWN, KnownList
from databricks.labs.ucx.source_code.linters.files import LocalCodeLinter, FileLoader, FolderLoader
from databricks.labs.ucx.source_code.linters.context import LinterContext
from databricks.labs.ucx.source_code.notebooks.loaders import NotebookLoader
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.sdk.service import jobs, compute, pipelines

from tests.unit.source_code.test_graph import _TestDependencyGraph


@retried(on=[NotFound], timeout=timedelta(minutes=5))
def test_running_real_workflow_linter_job(installation_ctx, make_notebook, make_directory, make_job):
    # Deprecated file system path in call to: /mnt/things/e/f/g
    lint_problem = b"display(spark.read.csv('/mnt/things/e/f/g'))"
    notebook = make_notebook(path=f"{make_directory()}/notebook.ipynb", content=lint_problem)
    job = make_job(notebook_path=notebook)
    ctx = installation_ctx.replace(config_transform=lambda wc: replace(wc, include_job_ids=[job.job_id]))
    ctx.workspace_installation.run()
    ctx.deployed_workflows.run_workflow("experimental-workflow-linter")
    ctx.deployed_workflows.validate_step("experimental-workflow-linter")
    cursor = ctx.sql_backend.fetch(f"SELECT COUNT(*) AS count FROM {ctx.inventory_database}.workflow_problems")
    result = next(cursor)
    if result['count'] == 0:
        installation_ctx.deployed_workflows.relay_logs("experimental-workflow-linter")
        assert False, "No workflow problems found"


@retried(on=[NotFound], timeout=timedelta(minutes=2))
def test_linter_from_context(simple_ctx, make_job, make_notebook):
    # This code is essentially the same as in test_running_real_workflow_linter_job,
    # but it's executed on the caller side and is easier to debug.
    # ensure we have at least 1 job that fails
    notebook_path = make_notebook(content=io.BytesIO(b"import xyz"))
    job = make_job(notebook_path=notebook_path)
    simple_ctx.config.include_job_ids = [job.job_id]
    simple_ctx.workflow_linter.refresh_report(simple_ctx.sql_backend, simple_ctx.inventory_database)

    cursor = simple_ctx.sql_backend.fetch(
        f"SELECT COUNT(*) AS count FROM {simple_ctx.inventory_database}.workflow_problems"
    )
    result = next(cursor)
    assert result['count'] > 0


def test_job_linter_no_problems(simple_ctx, make_job):
    j = make_job()

    problems = simple_ctx.workflow_linter.lint_job(j.job_id)

    assert len(problems) == 0


def test_job_task_linter_library_not_installed_cluster(
    simple_ctx, make_job, make_random, make_cluster, make_notebook, make_directory
):
    created_cluster = make_cluster(single_node=True)
    entrypoint = make_directory()

    notebook = make_notebook(path=f"{entrypoint}/notebook.ipynb", content=b"import greenlet")

    task = jobs.Task(
        task_key=make_random(4),
        description=make_random(4),
        existing_cluster_id=created_cluster.cluster_id,
        notebook_task=jobs.NotebookTask(
            notebook_path=str(notebook),
        ),
    )
    j = make_job(tasks=[task])

    problems = simple_ctx.workflow_linter.lint_job(j.job_id)
    assert len([problem for problem in problems if problem.message == "Could not locate import: greenlet"]) == 1


def test_job_task_linter_library_installed_cluster(
    simple_ctx, ws, make_job, make_random, make_cluster, make_notebook, make_directory
):
    created_cluster = make_cluster(single_node=True)
    libraries_api = ws.libraries
    libraries_api.install(created_cluster.cluster_id, [Library(pypi=PythonPyPiLibrary("greenlet"))])
    entrypoint = make_directory()

    notebook = make_notebook(path=f"{entrypoint}/notebook.ipynb", content=b"import doesnotexist;import greenlet")

    task = jobs.Task(
        task_key=make_random(4),
        description=make_random(4),
        existing_cluster_id=created_cluster.cluster_id,
        notebook_task=jobs.NotebookTask(
            notebook_path=str(notebook),
        ),
    )
    j = make_job(tasks=[task])

    problems = simple_ctx.workflow_linter.lint_job(j.job_id)
    assert len([problem for problem in problems if problem.message == "Could not locate import: greenlet"]) == 0


def test_job_linter_some_notebook_graph_with_problems(simple_ctx, ws, make_job, make_notebook, make_random, caplog):
    expected_messages = {
        'second_notebook:3 [dbfs-usage] Deprecated file system path: /mnt/something',
        'second_notebook:3 [implicit-dbfs-usage] The use of default dbfs: references is deprecated: /mnt/something',
        'some_file.py:0 [dbfs-usage] Deprecated file system path: /mnt/foo/bar',
        'some_file.py:0 [implicit-dbfs-usage] The use of default dbfs: references is deprecated: /mnt/foo/bar',
    }

    entrypoint = WorkspacePath(ws, f"~/linter-{make_random(4)}-{get_purge_suffix()}").expanduser()
    entrypoint.mkdir()

    main_notebook = entrypoint / 'main'
    make_notebook(path=main_notebook, content=b'%run ./second_notebook')
    j = make_job(notebook_path=main_notebook)

    make_notebook(
        path=entrypoint / 'second_notebook',
        content=b"""import some_file
print('hello world')
display(spark.read.parquet("/mnt/something"))
""",
    )

    some_file = entrypoint / 'some_file.py'
    some_file.write_text('display(spark.read.parquet("/mnt/foo/bar"))')

    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.source_code.jobs"):
        problems = simple_ctx.workflow_linter.lint_job(j.job_id)

    messages = {replace(p, path=Path(p.path).relative_to(entrypoint)).as_message() for p in problems}
    assert messages == expected_messages

    last_messages = caplog.messages[-1].split("\n")
    assert all(any(message.endswith(expected) for message in last_messages) for expected in expected_messages)


def test_workflow_linter_lints_job_with_import_pypi_library(
    simple_ctx,
    ws,
    make_job,
    make_notebook,
    make_random,
):
    entrypoint = WorkspacePath(ws, f"~/linter-{make_random(4)}-{get_purge_suffix()}").expanduser()
    entrypoint.mkdir()

    simple_ctx = simple_ctx.replace(
        path_lookup=PathLookup(Path("/non/existing/path"), []),  # Avoid finding the pytest you are running
    )

    notebook = entrypoint / "notebook.ipynb"
    make_notebook(path=notebook, content=b"import greenlet")

    job_without_pytest_library = make_job(notebook_path=notebook)
    problems = simple_ctx.workflow_linter.lint_job(job_without_pytest_library.job_id)

    assert len([problem for problem in problems if problem.message == "Could not locate import: greenlet"]) > 0

    library = compute.Library(pypi=compute.PythonPyPiLibrary(package="greenlet"))
    job_with_pytest_library = make_job(notebook_path=notebook, libraries=[library])

    problems = simple_ctx.workflow_linter.lint_job(job_with_pytest_library.job_id)

    assert len([problem for problem in problems if problem.message == "Could not locate import: greenlet"]) == 0


def test_lint_local_code(simple_ctx):
    # no need to connect
    session_state = CurrentSessionState()
    linter_context = LinterContext(MigrationIndex([]), session_state)
    light_ctx = simple_ctx
    ucx_path = Path(__file__).parent.parent.parent.parent
    path_to_scan = Path(ucx_path, "src")
    # TODO: LocalCheckoutContext has to move into GlobalContext because of this hack
    linter = LocalCodeLinter(
        light_ctx.file_loader,
        light_ctx.folder_loader,
        light_ctx.path_lookup,
        session_state,
        light_ctx.dependency_resolver,
        lambda: linter_context,
    )
    problems = linter.lint(Prompts(), path_to_scan, StringIO())
    assert len(problems) > 0


@pytest.mark.parametrize("order", [[0, 1, 2], [0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [2, 1, 0]])
def test_graph_computes_magic_run_route_recursively_in_parent_folder(simple_ctx, order):
    # order in which we consider files influences the algorithm so we check all order
    parent_local_path = (
        Path(__file__).parent.parent.parent / "unit" / "source_code" / "samples" / "parent-child-context"
    )
    client: WorkspaceClient = simple_ctx.workspace_client
    parent_ws_path = WorkspacePath(client, "/parent-child-context")
    parent_ws_path.mkdir()
    all_names = ["grand_parent", "parent", "child"]
    all_ws_paths = list(WorkspacePath(client, parent_ws_path / name) for name in all_names)
    for i, name in enumerate(all_names):
        ws_path = all_ws_paths[i]
        if not ws_path.exists():
            file_path = parent_local_path / f"{name}.py"
            # use intermediate string because WorkspacePath does not yet support BOMs
            content = file_path.read_text("utf-8")
            # workspace notebooks don't have extensions
            content = content.replace(".py", "")
            data = content.encode("utf-8")
            client.workspace.upload(
                ws_path.as_posix(), data, format=ImportFormat.SOURCE, overwrite=True, language=Language.PYTHON
            )

    class ScrambledFolderPath(WorkspacePath):

        def iterdir(self):
            scrambled = [all_ws_paths[order[0]], all_ws_paths[order[1]], all_ws_paths[order[2]]]
            yield from scrambled

    dependency = Dependency(FolderLoader(NotebookLoader(), FileLoader()), ScrambledFolderPath(client, parent_ws_path))
    root_graph = _TestDependencyGraph(
        dependency, None, simple_ctx.dependency_resolver, simple_ctx.path_lookup, CurrentSessionState()
    )
    container = dependency.load(simple_ctx.path_lookup)
    container.build_dependency_graph(root_graph)
    roots = root_graph.root_dependencies
    assert len(roots) == 1
    assert all_ws_paths[0] in [dep.path for dep in roots]
    route = root_graph.compute_route(all_ws_paths[0], all_ws_paths[2])
    assert [dep.path for dep in route] == all_ws_paths


@pytest.fixture
def make_dbfs_directory(ws: WorkspaceClient, make_random: Callable[[int], str]):
    def create() -> DBFSPath:
        path = DBFSPath(ws, f"~/sdk-{make_random(4)}-{get_purge_suffix()}").expanduser()
        path.mkdir()
        return path

    yield from factory("dbfs-directory", create, lambda p: p.rmdir(recursive=True))


@pytest.fixture
def make_workspace_directory(ws: WorkspaceClient, make_random: Callable[[int], str]):
    def create() -> WorkspacePath:
        path = WorkspacePath(ws, f"~/sdk-{make_random(4)}-{get_purge_suffix()}").expanduser()
        path.mkdir()
        return path

    yield from factory("workspace-directory", create, lambda p: p.rmdir(recursive=True))


def test_workflow_linter_lints_job_with_workspace_requirements_dependency(
    simple_ctx,
    ws,
    make_job,
    make_notebook,
    make_directory,
    make_workspace_directory,
):
    # A requirement that can definitely not be found.
    requirements = "a_package_that_does_not_exist\n"

    # Notebook code: yaml is part of DBR, and shouldn't trigger an error but the other module will.
    python_code = "import yaml\nimport module_that_does_not_exist\n"

    remote_requirements_path = make_workspace_directory() / "requirements.txt"
    remote_requirements_path.write_text(requirements)
    library = compute.Library(requirements=remote_requirements_path.as_posix())

    entrypoint = make_directory()
    notebook = make_notebook(path=f"{entrypoint}/notebook.ipynb", content=python_code.encode("utf-8"))
    job_with_pytest_library = make_job(notebook_path=notebook, libraries=[library])

    problems = simple_ctx.workflow_linter.lint_job(job_with_pytest_library.job_id)
    messages = tuple(problem.message for problem in problems)
    expected_messages = (
        "ERROR: Could not find a version that satisfies the requirement a_package_that_does_not_exist",
        "Could not locate import: module_that_does_not_exist",
    )
    unexpected_messages = ("Could not locate import: yaml",)
    assert len(problems) == 2
    assert all(any(expected in message for message in messages) for expected in expected_messages)
    assert all(not any(unexpected in message for message in messages) for unexpected in unexpected_messages)


def test_workflow_linter_lints_job_with_dbfs_requirements_dependency(
    simple_ctx,
    ws,
    make_job,
    make_notebook,
    make_directory,
    make_dbfs_directory,
):
    # A requirement that can definitely not be found.
    requirements = "a_package_that_does_not_exist\n"

    # Notebook code: yaml is part of DBR, and shouldn't trigger an error but the other module will.
    python_code = "import yaml\nimport module_that_does_not_exist\n"

    remote_requirements_path = make_dbfs_directory() / "requirements.txt"
    remote_requirements_path.write_text(requirements)
    library = compute.Library(requirements=f"dbfs:{remote_requirements_path.as_posix()}")

    entrypoint = make_directory()
    notebook = make_notebook(path=f"{entrypoint}/notebook.ipynb", content=python_code.encode("utf-8"))
    job_with_pytest_library = make_job(notebook_path=notebook, libraries=[library])

    problems = simple_ctx.workflow_linter.lint_job(job_with_pytest_library.job_id)
    messages = tuple(problem.message for problem in problems)
    expected_messages = (
        "ERROR: Could not find a version that satisfies the requirement a_package_that_does_not_exist",
        "Could not locate import: module_that_does_not_exist",
    )
    unexpected_messages = ("Could not locate import: yaml",)
    assert len(problems) == 2
    assert all(any(expected in message for message in messages) for expected in expected_messages)
    assert all(not any(unexpected in message for message in messages) for unexpected in unexpected_messages)


def test_workflow_linter_lints_job_with_workspace_egg_dependency(
    simple_ctx,
    ws,
    make_job,
    make_notebook,
    make_directory,
    make_workspace_directory,
):
    expected_problem_message = "Could not locate import: thingy"
    egg_file = Path(__file__).parent / "../../unit/source_code/samples/distribution/dist/thingy-0.0.1-py3.10.egg"

    remote_egg_path = make_workspace_directory() / egg_file.name
    with egg_file.open("rb") as src, remote_egg_path.open("wb") as dst:
        shutil.copyfileobj(src, dst)
    library = compute.Library(egg=remote_egg_path.as_posix())

    entrypoint = make_directory()
    notebook = make_notebook(path=f"{entrypoint}/notebook.ipynb", content=b"import thingy\n")
    job_with_egg_dependency = make_job(notebook_path=notebook, libraries=[library])

    problems = simple_ctx.workflow_linter.lint_job(job_with_egg_dependency.job_id)

    assert not [problem for problem in problems if problem.message == expected_problem_message]


def test_workflow_linter_lints_job_with_dbfs_egg_dependency(
    simple_ctx,
    ws,
    make_job,
    make_notebook,
    make_directory,
    make_dbfs_directory,
):
    expected_problem_message = "Could not locate import: thingy"
    egg_file = Path(__file__).parent / "../../unit/source_code/samples/distribution/dist/thingy-0.0.1-py3.10.egg"

    remote_egg_path = make_dbfs_directory() / egg_file.name
    with egg_file.open("rb") as src, remote_egg_path.open("wb") as dst:
        shutil.copyfileobj(src, dst)
    library = compute.Library(egg=f"dbfs:{remote_egg_path.as_posix()}")

    entrypoint = make_directory()
    notebook = make_notebook(path=f"{entrypoint}/notebook.ipynb", content=b"import thingy\n")
    job_with_egg_dependency = make_job(notebook_path=notebook, libraries=[library])

    problems = simple_ctx.workflow_linter.lint_job(job_with_egg_dependency.job_id)

    assert not [problem for problem in problems if problem.message == expected_problem_message]


def test_workflow_linter_lints_job_with_missing_library(simple_ctx, make_job, make_notebook, make_directory):
    expected_problem_message = "Could not locate import: databricks.labs.ucx"
    allow_list = create_autospec(KnownList)  # databricks is in default list
    allow_list.module_compatibility.return_value = UNKNOWN

    simple_ctx = simple_ctx.replace(
        allow_list=allow_list,
        path_lookup=PathLookup(Path("/non/existing/path"), []),  # Avoid finding current project
    )

    notebook = make_notebook(path=f"{make_directory()}/notebook.ipynb", content=b"import databricks.labs.ucx")
    job_without_ucx_library = make_job(notebook_path=notebook)

    problems = simple_ctx.workflow_linter.lint_job(job_without_ucx_library.job_id)

    assert len([problem for problem in problems if problem.message == expected_problem_message]) > 0
    allow_list.module_compatibility.assert_called_once_with("databricks.labs.ucx")


def test_workflow_linter_lints_job_with_wheel_dependency(simple_ctx, make_job, make_notebook, make_directory):
    expected_problem_message = "Could not locate import: databricks.labs.ucx"

    simple_ctx = simple_ctx.replace(
        allow_list=KnownList(),  # databricks is in default list
        path_lookup=PathLookup(Path("/non/existing/path"), []),  # Avoid finding current project
    )

    simple_ctx.workspace_installation.run()  # Creates ucx wheel
    wheels = [file for file in simple_ctx.installation.files() if file.path.endswith(".whl")]
    library = compute.Library(whl=wheels[0].path)

    notebook = make_notebook(path=f"{make_directory()}/notebook.ipynb", content=b"import databricks.labs.ucx")
    job_with_ucx_library = make_job(notebook_path=notebook, libraries=[library])

    problems = simple_ctx.workflow_linter.lint_job(job_with_ucx_library.job_id)

    assert len([problem for problem in problems if problem.message == expected_problem_message]) == 0


def test_job_spark_python_task_linter_happy_path(
    simple_ctx,
    make_job,
    make_random,
    make_cluster,
    make_notebook,
    make_directory,
):
    entrypoint = make_directory()

    make_notebook(path=f"{entrypoint}/notebook.py", content=b"import greenlet")

    new_cluster = make_cluster(single_node=True)
    task = jobs.Task(
        task_key=make_random(4),
        spark_python_task=jobs.SparkPythonTask(
            python_file=f"{entrypoint}/notebook.py",
        ),
        existing_cluster_id=new_cluster.cluster_id,
        libraries=[compute.Library(pypi=compute.PythonPyPiLibrary(package="greenlet"))],
    )
    j = make_job(tasks=[task])

    problems = simple_ctx.workflow_linter.lint_job(j.job_id)
    assert len([problem for problem in problems if problem.message == "Could not locate import: greenlet"]) == 0


def test_job_spark_python_task_linter_unhappy_path(
    simple_ctx, make_job, make_random, make_cluster, make_notebook, make_directory
):
    entrypoint = make_directory()

    make_notebook(path=f"{entrypoint}/notebook.py", content=b"import greenlet")

    new_cluster = make_cluster(single_node=True)
    task = jobs.Task(
        task_key=make_random(4),
        spark_python_task=jobs.SparkPythonTask(
            python_file=f"{entrypoint}/notebook.py",
        ),
        existing_cluster_id=new_cluster.cluster_id,
    )
    j = make_job(tasks=[task])

    problems = simple_ctx.workflow_linter.lint_job(j.job_id)
    assert len([problem for problem in problems if problem.message == "Could not locate import: greenlet"]) == 1


def test_workflow_linter_lints_python_wheel_task(simple_ctx, ws, make_job, make_random):
    allow_list = create_autospec(KnownList)  # databricks is in default list
    allow_list.module_compatibility.return_value = UNKNOWN
    allow_list.distribution_compatibility.return_value = UNKNOWN

    simple_ctx = simple_ctx.replace(
        allow_list=allow_list,
        path_lookup=PathLookup(Path("/non/existing/path"), []),  # Avoid finding current project
    )

    simple_ctx.workspace_installation.run()  # Creates ucx wheel
    wheels = [file for file in simple_ctx.installation.files() if file.path.endswith(".whl")]
    library = compute.Library(whl=wheels[0].path)

    python_wheel_task = jobs.PythonWheelTask("databricks_labs_ucx", "runtime")
    task = jobs.Task(
        task_key=make_random(4),
        python_wheel_task=python_wheel_task,
        new_cluster=compute.ClusterSpec(
            num_workers=1,
            node_type_id=ws.clusters.select_node_type(local_disk=True, min_memory_gb=16),
            spark_version=ws.clusters.select_spark_version(latest=True),
        ),
        timeout_seconds=0,
        libraries=[library],
    )
    job_with_ucx_library = make_job(tasks=[task])

    problems = simple_ctx.workflow_linter.lint_job(job_with_ucx_library.job_id)

    assert len([problem for problem in problems if problem.code == "library-dist-info-not-found"]) == 0
    assert len([problem for problem in problems if problem.code == "library-entrypoint-not-found"]) == 0
    allow_list.distribution_compatibility.assert_called_once()


def test_job_spark_python_task_workspace_linter_happy_path(
    simple_ctx,
    make_job,
    make_random,
    make_cluster,
    make_workspace_directory,
) -> None:
    pyspark_job_path = make_workspace_directory() / "spark_job.py"
    pyspark_job_path.write_text("import greenlet\n")

    new_cluster = make_cluster(single_node=True)
    task = jobs.Task(
        task_key=make_random(4),
        spark_python_task=jobs.SparkPythonTask(python_file=pyspark_job_path.as_posix()),
        existing_cluster_id=new_cluster.cluster_id,
        libraries=[compute.Library(pypi=compute.PythonPyPiLibrary(package="greenlet"))],
    )
    j = make_job(tasks=[task])

    problems = simple_ctx.workflow_linter.lint_job(j.job_id)
    assert not [problem for problem in problems if problem.message == "Could not locate import: greenlet"]


def test_job_spark_python_task_dbfs_linter_happy_path(
    simple_ctx,
    make_job,
    make_random,
    make_cluster,
    make_dbfs_directory,
) -> None:
    pyspark_job_path = make_dbfs_directory() / "spark_job.py"
    pyspark_job_path.write_text("import greenlet\n")

    new_cluster = make_cluster(single_node=True)
    task = jobs.Task(
        task_key=make_random(4),
        spark_python_task=jobs.SparkPythonTask(python_file=f"dbfs:{pyspark_job_path.as_posix()}"),
        existing_cluster_id=new_cluster.cluster_id,
        libraries=[compute.Library(pypi=compute.PythonPyPiLibrary(package="greenlet"))],
    )
    j = make_job(tasks=[task])

    problems = simple_ctx.workflow_linter.lint_job(j.job_id)
    assert not [problem for problem in problems if problem.message == "Could not locate import: greenlet"]


def test_job_spark_python_task_linter_notebook_handling(
    simple_ctx,
    make_job,
    make_random,
    make_cluster,
    make_dbfs_directory,
) -> None:
    """Spark Python tasks are simple python files; verify that we treat them as such and not as notebooks."""

    # Use DBFS instead of Workspace paths because the Workspace modifies things if it detects files are notebooks.
    job_dir = make_dbfs_directory()
    local_notebook_path = Path(__file__).parent / "notebook_fake_python.py"
    dbfs_notebook_path = job_dir / local_notebook_path.name
    with local_notebook_path.open("rb") as src, dbfs_notebook_path.open("wb") as dst:
        shutil.copyfileobj(src, dst)

    new_cluster = make_cluster(single_node=True)
    task = jobs.Task(
        task_key=make_random(4),
        spark_python_task=jobs.SparkPythonTask(python_file=f"dbfs:{dbfs_notebook_path.as_posix()}"),
        existing_cluster_id=new_cluster.cluster_id,
    )
    j = make_job(tasks=[task])

    problems = simple_ctx.workflow_linter.lint_job(j.job_id)
    # The notebook being linted has 'import greenlet' in a cell that should be ignored, but will trigger this problem if processed.
    assert not [problem for problem in problems if problem.message == "Could not locate import: greenlet"]


def test_job_dlt_task_linter_unhappy_path(
    simple_ctx,
    make_job,
    make_random,
    make_notebook,
    make_directory,
    make_pipeline,
):
    entrypoint = make_directory()
    make_notebook(path=f"{entrypoint}/notebook.py", content=b"import greenlet")
    dlt_pipeline = make_pipeline(
        libraries=[pipelines.PipelineLibrary(notebook=NotebookLibrary(path=f"{entrypoint}/notebook.py"))]
    )

    task = jobs.Task(
        task_key=make_random(4),
        pipeline_task=jobs.PipelineTask(pipeline_id=dlt_pipeline.pipeline_id),
    )
    j = make_job(tasks=[task])

    problems = simple_ctx.workflow_linter.lint_job(j.job_id)
    assert len([problem for problem in problems if problem.message == "Could not locate import: greenlet"]) == 1


def test_job_dlt_task_linter_happy_path(
    simple_ctx,
    make_job,
    make_random,
    make_notebook,
    make_directory,
    make_pipeline,
):
    entrypoint = make_directory()
    make_notebook(path=f"{entrypoint}/notebook.py", content=b"import greenlet")
    dlt_pipeline = make_pipeline(
        libraries=[pipelines.PipelineLibrary(notebook=NotebookLibrary(path=f"{entrypoint}/notebook.py"))]
    )

    task = jobs.Task(
        task_key=make_random(4),
        pipeline_task=jobs.PipelineTask(pipeline_id=dlt_pipeline.pipeline_id),
        libraries=[compute.Library(pypi=compute.PythonPyPiLibrary(package="greenlet"))],
    )
    j = make_job(tasks=[task])

    problems = simple_ctx.workflow_linter.lint_job(j.job_id)
    assert len([problem for problem in problems if problem.message == "Could not locate import: greenlet"]) == 0


def test_job_dependency_problem_egg_dbr14plus(make_job, make_directory, make_notebook, make_random, simple_ctx, ws):
    egg_file = Path(__file__).parent / "../../unit/source_code/samples/distribution/dist/thingy-0.0.1-py3.10.egg"
    task_spark_conf = None
    entrypoint = make_directory()
    notebook_path = make_notebook()
    remote_egg_file = f"{entrypoint}/{egg_file.name}"
    with egg_file.open("rb") as f:
        ws.workspace.upload(remote_egg_file, f.read(), format=ImportFormat.AUTO)
    library = compute.Library(egg=remote_egg_file)
    task = jobs.Task(
        task_key=make_random(4),
        description=make_random(4),
        new_cluster=compute.ClusterSpec(
            num_workers=1,
            node_type_id=ws.clusters.select_node_type(local_disk=True, min_memory_gb=16),
            spark_version=ws.clusters.select_spark_version(latest=True),
            spark_conf=task_spark_conf,
        ),
        libraries=[library],
        notebook_task=jobs.NotebookTask(notebook_path=str(notebook_path)),
    )

    j = make_job(tasks=[task])

    problems = simple_ctx.workflow_linter.lint_job(j.job_id)
    assert (
        len(
            [
                problem
                for problem in problems
                if problem.message == "Installing eggs is no longer supported on Databricks 14.0 or higher"
            ]
        )
        == 1
    )

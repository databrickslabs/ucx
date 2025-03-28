import logging
import shutil
from collections.abc import Callable
from dataclasses import replace
from datetime import timedelta, datetime, timezone
from pathlib import Path
from unittest.mock import create_autospec

import pytest
from databricks.labs.blueprint.paths import DBFSPath, WorkspacePath
from databricks.labs.pytester.fixtures.baseline import factory
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service.compute import Library, PythonPyPiLibrary
from databricks.sdk.service.pipelines import NotebookLibrary
from databricks.sdk.service.workspace import ImportFormat, Language

from databricks.labs.ucx.source_code.base import CurrentSessionState, LineageAtom
from databricks.labs.ucx.source_code.directfs_access import DirectFsAccess
from databricks.labs.ucx.source_code.graph import Dependency
from databricks.labs.ucx.source_code.known import UNKNOWN, KnownList
from databricks.labs.ucx.source_code.folders import FolderLoader
from databricks.labs.ucx.source_code.files import FileLoader
from databricks.labs.ucx.source_code.notebooks.loaders import NotebookLoader
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.sdk.service import jobs, compute, pipelines

from tests.unit.source_code.test_graph import _TestDependencyGraph


@retried(on=[NotFound], timeout=timedelta(minutes=2))
def test_linter_from_context(simple_ctx, make_job) -> None:
    # This code is similar to test_running_real_workflow_linter_job, but it's executed on the caller side and is easier
    # to debug.
    # Ensure we have at least 1 job that fails: "Deprecated file system path in call to: /mnt/things/e/f/g"
    job = make_job(content="spark.read.table('a_table').write.csv('/mnt/things/e/f/g')\n")
    simple_ctx.config.include_job_ids = [job.job_id]
    simple_ctx.workflow_linter.refresh_report(simple_ctx.sql_backend, simple_ctx.inventory_database)

    # Verify that the 'problems' table has content.
    cursor = simple_ctx.sql_backend.fetch(
        f"SELECT COUNT(*) AS count FROM {simple_ctx.inventory_database}.workflow_problems"
    )
    result = next(cursor)
    assert result['count'] > 0

    # Verify that the other data produced snapshot can be loaded.
    dfsa_records = simple_ctx.directfs_access_crawler_for_paths.snapshot()
    used_table_records = simple_ctx.used_tables_crawler_for_paths.snapshot()
    assert dfsa_records and used_table_records


def test_job_linter_no_problems(simple_ctx, make_job) -> None:
    j = make_job()

    problems, *_ = simple_ctx.workflow_linter.lint_job(j.job_id)

    assert len(problems) == 0


def test_job_task_linter_library_not_installed_cluster(simple_ctx, make_job) -> None:
    job = make_job(content="import library_not_found\n")

    problems, *_ = simple_ctx.workflow_linter.lint_job(job.job_id)

    assert (
        len([problem for problem in problems if problem.message == "Could not locate import: library_not_found"]) == 1
    )


def test_job_task_linter_library_installed_cluster(
    simple_ctx,
    ws,
    make_job,
    make_random,
    make_cluster,
    make_notebook,
) -> None:
    created_cluster = make_cluster(single_node=True)
    ws.libraries.install(created_cluster.cluster_id, [Library(pypi=PythonPyPiLibrary("dbt-core==1.8.7"))])

    notebook = make_notebook(content=b"import doesnotexist;import dbt")

    task = jobs.Task(
        task_key=make_random(4),
        description=make_random(4),
        existing_cluster_id=created_cluster.cluster_id,
        notebook_task=jobs.NotebookTask(notebook_path=str(notebook)),
    )
    j = make_job(tasks=[task])

    problems, *_ = simple_ctx.workflow_linter.lint_job(j.job_id)

    assert next(problem for problem in problems if problem.message == "Could not locate import: doesnotexist")
    assert not next((problem for problem in problems if problem.message == "Could not locate import: dbt"), None)


def test_job_linter_some_notebook_graph_with_problems(
    simple_ctx,
    ws,
    make_job,
    make_notebook,
    make_random,
    caplog,
    watchdog_purge_suffix,
) -> None:
    expected_messages = {
        'some_file.py:0 [direct-filesystem-access] The use of direct filesystem references is deprecated: /mnt/foo/bar',
        'second_notebook:3 [direct-filesystem-access] The use of direct filesystem references is deprecated: /mnt/something',
    }

    entrypoint = WorkspacePath(ws, f"~/linter-{make_random(4)}-{watchdog_purge_suffix}").expanduser()
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

    (entrypoint / 'some_file.py').write_text('display(spark.read.parquet("/mnt/foo/bar"))')

    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.source_code.jobs"):
        problems, dfsas, _ = simple_ctx.workflow_linter.lint_job(j.job_id)

    root = Path(entrypoint.as_posix())
    messages = {replace(p, path=Path(p.path).relative_to(root)).as_message() for p in problems}
    assert messages == expected_messages

    last_messages = caplog.messages[-1].split("\n")
    assert all(any(message.endswith(expected) for message in last_messages) for expected in expected_messages)

    assert len(dfsas) == 2
    task_keys = set(f"{j.job_id}/{task.task_key}" for task in j.settings.tasks)
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    for dfsa in dfsas:
        assert dfsa.source_id != DirectFsAccess.UNKNOWN
        assert len(dfsa.source_lineage)
        assert dfsa.source_timestamp > yesterday
        assert dfsa.assessment_start_timestamp > yesterday
        assert dfsa.assessment_end_timestamp > yesterday
        assert dfsa.source_lineage[0] == LineageAtom(
            object_type="WORKFLOW", object_id=str(j.job_id), other={"name": j.settings.name}
        )
        assert dfsa.source_lineage[1].object_type == "TASK"
        assert dfsa.source_lineage[1].object_id in task_keys


def test_workflow_linter_lints_job_with_import_pypi_library(simple_ctx, make_job) -> None:
    content = "import dbt"
    problem_message = "Could not locate import: dbt"
    job_without_library = make_job(content=content)

    problems, *_ = simple_ctx.workflow_linter.lint_job(job_without_library.job_id)

    assert len([problem for problem in problems if problem.message == problem_message]) == 1

    library = compute.Library(pypi=compute.PythonPyPiLibrary(package="dbt-core==1.8.7"))
    job_with_library = make_job(content=content, libraries=[library])

    problems, *_ = simple_ctx.workflow_linter.lint_job(job_with_library.job_id)

    assert len([problem for problem in problems if problem.message == problem_message]) == 0


@pytest.mark.parametrize("order", [[0, 1, 2], [0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [2, 1, 0]])
def test_graph_computes_magic_run_route_recursively_in_parent_folder(simple_ctx, order) -> None:
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
    assert container
    container.build_dependency_graph(root_graph)
    roots = root_graph.root_dependencies
    assert len(roots) == 1
    assert all_ws_paths[0] in [dep.path for dep in roots]
    route = root_graph.compute_route(all_ws_paths[0], all_ws_paths[2])
    assert [dep.path for dep in route] == all_ws_paths


@pytest.fixture
def make_dbfs_directory(ws: WorkspaceClient, make_random: Callable[[int], str], watchdog_purge_suffix):
    def create() -> DBFSPath:
        path = DBFSPath(ws, f"~/sdk-{make_random(4)}-{watchdog_purge_suffix}").expanduser()
        path.mkdir()
        return path

    yield from factory("dbfs-directory", create, lambda p: p.rmdir(recursive=True))


def test_workflow_linter_lints_job_with_workspace_requirements_dependency(
    simple_ctx,
    make_job,
    make_notebook,
    make_directory,
) -> None:
    # A requirement that can definitely not be found.
    requirements = "a_package_that_does_not_exist\n"

    # Notebook code: yaml is part of DBR, and shouldn't trigger an error but the other module will.
    python_code = "import yaml\nimport module_that_does_not_exist\n"

    remote_requirements_path = make_directory() / "requirements.txt"
    remote_requirements_path.write_text(requirements)
    library = compute.Library(requirements=remote_requirements_path.as_posix())

    entrypoint = make_directory()
    notebook = make_notebook(path=f"{entrypoint}/notebook.ipynb", content=python_code.encode("utf-8"))
    job_with_pytest_library = make_job(notebook_path=notebook, libraries=[library])

    problems, *_ = simple_ctx.workflow_linter.lint_job(job_with_pytest_library.job_id)

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
    make_job,
    make_notebook,
    make_directory,
    make_dbfs_directory,
) -> None:
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

    problems, *_ = simple_ctx.workflow_linter.lint_job(job_with_pytest_library.job_id)

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
    make_job,
    make_notebook,
    make_directory,
) -> None:
    expected_problem_message = "Could not locate import: thingy"
    egg_file = Path(__file__).parent / "../../unit/source_code/samples/distribution/dist/thingy-0.0.1-py3.10.egg"

    remote_egg_path = make_directory() / egg_file.name
    with egg_file.open("rb") as src, remote_egg_path.open("wb") as dst:
        shutil.copyfileobj(src, dst)
    library = compute.Library(egg=remote_egg_path.as_posix())

    entrypoint = make_directory()
    notebook = make_notebook(path=f"{entrypoint}/notebook.ipynb", content=b"import thingy\n")
    job_with_egg_dependency = make_job(notebook_path=notebook, libraries=[library])

    problems, *_ = simple_ctx.workflow_linter.lint_job(job_with_egg_dependency.job_id)

    assert not [problem for problem in problems if problem.message == expected_problem_message]


def test_workflow_linter_lints_job_with_dbfs_egg_dependency(
    simple_ctx,
    make_job,
    make_notebook,
    make_directory,
    make_dbfs_directory,
) -> None:
    expected_problem_message = "Could not locate import: thingy"
    egg_file = Path(__file__).parent / "../../unit/source_code/samples/distribution/dist/thingy-0.0.1-py3.10.egg"

    remote_egg_path = make_dbfs_directory() / egg_file.name
    with egg_file.open("rb") as src, remote_egg_path.open("wb") as dst:
        shutil.copyfileobj(src, dst)
    library = compute.Library(egg=f"dbfs:{remote_egg_path.as_posix()}")

    entrypoint = make_directory()
    notebook = make_notebook(path=f"{entrypoint}/notebook.ipynb", content=b"import thingy\n")
    job_with_egg_dependency = make_job(notebook_path=notebook, libraries=[library])

    problems, *_ = simple_ctx.workflow_linter.lint_job(job_with_egg_dependency.job_id)

    assert not [problem for problem in problems if problem.message == expected_problem_message]


def test_workflow_linter_lints_job_with_missing_library(simple_ctx, make_job, make_notebook, make_directory) -> None:
    expected_problem_message = "Could not locate import: databricks.labs.ucx"
    allow_list = create_autospec(KnownList)  # databricks is in default list
    allow_list.module_compatibility.return_value = UNKNOWN

    simple_ctx = simple_ctx.replace(
        allow_list=allow_list,
        path_lookup=PathLookup(Path("/non/existing/path"), []),  # Avoid finding current project
    )

    notebook = make_notebook(path=f"{make_directory()}/notebook.ipynb", content=b"import databricks.labs.ucx")
    job_without_ucx_library = make_job(notebook_path=notebook)

    problems, *_ = simple_ctx.workflow_linter.lint_job(job_without_ucx_library.job_id)

    assert len([problem for problem in problems if problem.message == expected_problem_message]) > 0
    allow_list.module_compatibility.assert_called_once_with("databricks.labs.ucx")


def test_workflow_linter_lints_job_with_wheel_dependency(simple_ctx, make_job, make_notebook, make_directory) -> None:
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

    problems, *_ = simple_ctx.workflow_linter.lint_job(job_with_ucx_library.job_id)

    assert len([problem for problem in problems if problem.message == expected_problem_message]) == 0


def test_job_spark_python_task_linter_happy_path(simple_ctx, make_job) -> None:
    extra_library_for_module = compute.Library(pypi=compute.PythonPyPiLibrary(package="dbt-core==1.8.7"))
    job = make_job(content="import dbt\n", task_type=jobs.SparkPythonTask, libraries=[extra_library_for_module])

    problems, *_ = simple_ctx.workflow_linter.lint_job(job.job_id)

    assert len([problem for problem in problems if problem.message == "Could not locate import: dbt"]) == 0


def test_job_spark_python_task_linter_unhappy_path(simple_ctx, make_job) -> None:
    """The imported dependency is not defined."""
    job = make_job(content="import dbt", task_type=jobs.SparkPythonTask)

    problems, *_ = simple_ctx.workflow_linter.lint_job(job.job_id)

    assert len([problem for problem in problems if problem.message == "Could not locate import: dbt"]) == 1


def test_workflow_linter_lints_python_wheel_task(simple_ctx, ws, make_job, make_random) -> None:
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

    problems, *_ = simple_ctx.workflow_linter.lint_job(job_with_ucx_library.job_id)

    assert len([problem for problem in problems if problem.code == "library-dist-info-not-found"]) == 0
    assert len([problem for problem in problems if problem.code == "library-entrypoint-not-found"]) == 0
    allow_list.distribution_compatibility.assert_called_once()


def test_job_spark_python_task_workspace_linter_happy_path(simple_ctx, make_job) -> None:
    extra_library_for_module = compute.Library(pypi=compute.PythonPyPiLibrary(package="dbt-core==1.8.7"))
    job = make_job(content="import dbt\n", libraries=[extra_library_for_module])

    problems, *_ = simple_ctx.workflow_linter.lint_job(job.job_id)

    assert not [problem for problem in problems if problem.message == "Could not locate import: dbt"]


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
        spark_python_task=jobs.SparkPythonTask(python_file=f"dbfs:{pyspark_job_path}"),
        existing_cluster_id=new_cluster.cluster_id,
        libraries=[compute.Library(pypi=compute.PythonPyPiLibrary(package="greenlet"))],
    )
    j = make_job(tasks=[task])

    problems, *_ = simple_ctx.workflow_linter.lint_job(j.job_id)

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

    problems, *_ = simple_ctx.workflow_linter.lint_job(j.job_id)

    # The notebook being linted has 'import greenlet' in a cell that should be ignored, but will trigger this problem if processed.
    assert not [problem for problem in problems if problem.message == "Could not locate import: greenlet"]


def test_job_dlt_task_linter_unhappy_path(
    simple_ctx,
    make_job,
    make_random,
    make_notebook,
    make_directory,
    make_pipeline,
) -> None:
    notebook_path = make_directory() / "notebook.py"
    make_notebook(path=notebook_path, content=b"import library_not_found")
    dlt_pipeline = make_pipeline(
        libraries=[pipelines.PipelineLibrary(notebook=NotebookLibrary(path=str(notebook_path)))]
    )

    task = jobs.Task(
        task_key=make_random(4),
        pipeline_task=jobs.PipelineTask(pipeline_id=dlt_pipeline.pipeline_id),
    )
    j = make_job(tasks=[task])

    problems, *_ = simple_ctx.workflow_linter.lint_job(j.job_id)

    assert (
        len([problem for problem in problems if problem.message == "Could not locate import: library_not_found"]) == 1
    )

    # Pipeline id does not exist
    task = jobs.Task(
        task_key=make_random(4),
        pipeline_task=jobs.PipelineTask(pipeline_id="1234"),
    )
    j = make_job(tasks=[task])

    problems, *_ = simple_ctx.workflow_linter.lint_job(j.job_id)
    assert len([problem for problem in problems if problem.message == "Could not find pipeline: 1234"]) == 1


def test_job_dlt_task_linter_happy_path(
    simple_ctx,
    make_job,
    make_random,
    make_notebook,
    make_directory,
    make_pipeline,
) -> None:
    notebook_path = make_directory() / "notebook.py"
    make_notebook(path=notebook_path, content=b"import dbt")
    dlt_pipeline = make_pipeline(
        libraries=[pipelines.PipelineLibrary(notebook=NotebookLibrary(path=str(notebook_path)))]
    )

    task = jobs.Task(
        task_key=make_random(4),
        pipeline_task=jobs.PipelineTask(pipeline_id=dlt_pipeline.pipeline_id),
        libraries=[compute.Library(pypi=compute.PythonPyPiLibrary(package="dbt-core==1.8.7"))],
    )
    j = make_job(tasks=[task])

    problems, *_ = simple_ctx.workflow_linter.lint_job(j.job_id)

    assert len([problem for problem in problems if problem.message == "Could not locate import: dbt"]) == 0


def test_job_dependency_problem_egg_dbr14plus(make_job, make_directory, simple_ctx, ws) -> None:
    egg_file = Path(__file__).parent / "../../unit/source_code/samples/distribution/dist/thingy-0.0.1-py3.10.egg"
    entrypoint = make_directory()
    remote_egg_file = f"{entrypoint}/{egg_file.name}"
    with egg_file.open("rb") as f:
        ws.workspace.upload(remote_egg_file, f.read(), format=ImportFormat.AUTO)
    library = compute.Library(egg=remote_egg_file)

    j = make_job(libraries=[library])

    problems, *_ = simple_ctx.workflow_linter.lint_job(j.job_id)
    actual = []
    for problem in problems:
        if problem.message == "Installing eggs is no longer supported on Databricks 14.0 or higher":
            actual.append(problem)
    assert len(actual) == 1

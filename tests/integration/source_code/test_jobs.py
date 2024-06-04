import io
import logging
from dataclasses import replace
from datetime import timedelta
from io import StringIO
from pathlib import Path
from unittest.mock import create_autospec

from databricks.labs.blueprint.tui import Prompts
from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service.compute import Library, PythonPyPiLibrary
from databricks.sdk.service.pipelines import NotebookLibrary
from databricks.sdk.service.workspace import ImportFormat

from databricks.labs.ucx.hive_metastore.migration_status import MigrationIndex
from databricks.labs.ucx.source_code.known import UNKNOWN, Whitelist
from databricks.labs.ucx.source_code.linters.files import LocalCodeLinter
from databricks.labs.ucx.source_code.linters.context import LinterContext
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.sdk.service import jobs, compute, pipelines
from databricks.labs.ucx.mixins.wspath import WorkspacePath


@retried(on=[NotFound], timeout=timedelta(minutes=2))
def test_running_real_workflow_linter_job(installation_ctx, make_notebook, make_directory, make_job):
    # Deprecated file system path in call to: /mnt/things/e/f/g
    lint_problem = b"display(spark.read.csv('/mnt/things/e/f/g'))"
    notebook = make_notebook(path=f"{make_directory()}/notebook.ipynb", content=lint_problem)
    make_job(notebook_path=notebook)

    ctx = installation_ctx
    ctx.workspace_installation.run()
    ctx.deployed_workflows.run_workflow("experimental-workflow-linter")
    ctx.deployed_workflows.validate_step("experimental-workflow-linter")
    cursor = ctx.sql_backend.fetch(f"SELECT COUNT(*) AS count FROM {ctx.inventory_database}.workflow_problems")
    result = next(cursor)
    if result['count'] == 0:
        ctx.deployed_workflows.relay_logs("experimental-workflow-linter")
        assert False, "No workflow problems found"


@retried(on=[NotFound], timeout=timedelta(minutes=2))
def test_linter_from_context(simple_ctx, make_job, make_notebook):
    # This code is essentially the same as in test_running_real_workflow_linter_job,
    # but it's executed on the caller side and is easier to debug.
    # ensure we have at least 1 job that fails
    notebook_path = make_notebook(content=io.BytesIO(b"import xyz"))
    make_job(notebook_path=notebook_path)
    simple_ctx.workflow_linter.refresh_report(simple_ctx.sql_backend, simple_ctx.inventory_database)

    cursor = simple_ctx.sql_backend.fetch(
        f"SELECT COUNT(*) AS count FROM {simple_ctx.inventory_database}.workflow_problems"
    )
    result = next(cursor)
    assert result['count'] > 0


def test_job_linter_no_problems(simple_ctx, ws, make_job):
    j = make_job()

    problems = simple_ctx.workflow_linter.lint_job(j.job_id)

    assert len(problems) == 0


def test_job_task_linter_library_not_installed_cluster(
    simple_ctx, ws, make_job, make_random, make_cluster, make_notebook, make_directory
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
        "second_notebook:4 [direct-filesystem-access] The use of default dbfs: references is deprecated: /mnt/something",
        "some_file.py:1 [direct-filesystem-access] The use of default dbfs: references is deprecated: /mnt/foo/bar",
        "some_file.py:1 [dbfs-usage] Deprecated file system path in call to: /mnt/foo/bar",
        "second_notebook:4 [dbfs-usage] Deprecated file system path in call to: /mnt/something",
    }

    entrypoint = WorkspacePath(ws, f"~/linter-{make_random(4)}").expanduser()
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
    entrypoint = WorkspacePath(ws, f"~/linter-{make_random(4)}").expanduser()
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
    linter_context = LinterContext(MigrationIndex([]))
    light_ctx = simple_ctx
    ucx_path = Path(__file__).parent.parent.parent.parent
    path_to_scan = Path(ucx_path, "src")
    # TODO: LocalCheckoutContext has to move into GlobalContext because of this hack
    linter = LocalCodeLinter(
        light_ctx.file_loader,
        light_ctx.folder_loader,
        light_ctx.path_lookup,
        light_ctx.dependency_resolver,
        lambda: linter_context,
    )
    problems = linter.lint(Prompts(), path_to_scan, StringIO())
    assert len(problems) > 0


def test_workflow_linter_lints_job_with_requirements_dependency(
    simple_ctx,
    ws,
    make_job,
    make_notebook,
    make_random,
    make_directory,
    tmp_path,
):
    expected_problem_message = "Could not locate import: yaml"

    simple_ctx = simple_ctx.replace(
        path_lookup=PathLookup(Path("/non/existing/path"), []),  # Avoid finding the yaml locally
    )

    entrypoint = make_directory()

    requirements_file = f"{entrypoint}/requirements.txt"
    ws.workspace.upload(requirements_file, io.BytesIO(b"pyyaml"), format=ImportFormat.AUTO)
    library = compute.Library(requirements=requirements_file)

    notebook = make_notebook(path=f"{entrypoint}/notebook.ipynb", content=b"import yaml")
    job_with_pytest_library = make_job(notebook_path=notebook, libraries=[library])

    problems = simple_ctx.workflow_linter.lint_job(job_with_pytest_library.job_id)

    assert len([problem for problem in problems if problem.message == expected_problem_message]) == 0


def test_workflow_linter_lints_job_with_egg_dependency(
    simple_ctx,
    ws,
    make_job,
    make_notebook,
    make_directory,
):
    expected_problem_message = "Could not locate import: thingy"
    egg_file = Path(__file__).parent / "../../unit/source_code/samples/distribution/dist/thingy-0.0.1-py3.10.egg"

    entrypoint = make_directory()

    remote_egg_file = f"{entrypoint}/{egg_file.name}"
    with egg_file.open("rb") as f:
        ws.workspace.upload(remote_egg_file, f.read(), format=ImportFormat.AUTO)
    library = compute.Library(egg=remote_egg_file)

    notebook = make_notebook(path=f"{entrypoint}/notebook.ipynb", content=b"import thingy")
    job_with_egg_dependency = make_job(notebook_path=notebook, libraries=[library])

    problems = simple_ctx.workflow_linter.lint_job(job_with_egg_dependency.job_id)

    assert len([problem for problem in problems if problem.message == expected_problem_message]) == 0


def test_workflow_linter_lints_job_with_missing_library(
    simple_ctx,
    ws,
    make_job,
    make_notebook,
    make_random,
    make_directory,
):
    expected_problem_message = "Could not locate import: databricks.labs.ucx"
    whitelist = create_autospec(Whitelist)  # databricks is in default list
    whitelist.module_compatibility.return_value = UNKNOWN

    simple_ctx = simple_ctx.replace(
        whitelist=whitelist,
        path_lookup=PathLookup(Path("/non/existing/path"), []),  # Avoid finding current project
    )

    notebook = make_notebook(path=f"{make_directory()}/notebook.ipynb", content=b"import databricks.labs.ucx")
    job_without_ucx_library = make_job(notebook_path=notebook)

    problems = simple_ctx.workflow_linter.lint_job(job_without_ucx_library.job_id)

    assert len([problem for problem in problems if problem.message == expected_problem_message]) > 0
    whitelist.module_compatibility.assert_called_once_with("databricks.labs.ucx")


def test_workflow_linter_lints_job_with_wheel_dependency(
    simple_ctx,
    ws,
    make_job,
    make_notebook,
    make_random,
    make_directory,
):
    expected_problem_message = "Could not locate import: databricks.labs.ucx"

    simple_ctx = simple_ctx.replace(
        whitelist=Whitelist(),  # databricks is in default list
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
    simple_ctx, ws, make_job, make_random, make_cluster, make_notebook, make_directory
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
    simple_ctx, ws, make_job, make_random, make_cluster, make_notebook, make_directory
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
    whitelist = create_autospec(Whitelist)  # databricks is in default list
    whitelist.module_compatibility.return_value = UNKNOWN
    whitelist.distribution_compatibility.return_value = UNKNOWN

    simple_ctx = simple_ctx.replace(
        whitelist=whitelist,
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
    whitelist.distribution_compatibility.assert_called_once_with(Path(wheels[0].path).name)


def test_job_dlt_task_linter_unhappy_path(
    simple_ctx, ws, make_job, make_random, make_cluster, make_notebook, make_directory, make_pipeline
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

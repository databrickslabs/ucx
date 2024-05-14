import io
import logging
from dataclasses import replace
from datetime import timedelta
from io import StringIO
from pathlib import Path

from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service.workspace import ImportFormat

from databricks.labs.blueprint.tui import Prompts

from databricks.labs.ucx.hive_metastore.migration_status import MigrationIndex
from databricks.sdk.service.compute import Library, PythonPyPiLibrary
from databricks.labs.ucx.assessment import jobs

from databricks.labs.ucx.mixins.wspath import WorkspacePath
from databricks.labs.ucx.source_code.files import LocalCodeLinter
from databricks.labs.ucx.source_code.languages import Languages
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.whitelist import Whitelist
from databricks.sdk.service import jobs, compute



@retried(on=[NotFound], timeout=timedelta(minutes=2))
def test_running_real_workflow_linter_job(installation_ctx):
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


def test_job_task_linter_no_problems(simple_ctx, ws, make_job, make_random, make_cluster, make_notebook):
    created_cluster = make_cluster(single_node=True)

    task = jobs.Task(
        task_key=make_random(4),
        description=make_random(4),
        existing_cluster_id=created_cluster.cluster_id,
        notebook_task=jobs.NotebookTask(notebook_path=str(make_notebook())),
        timeout_seconds=0,
        libraries=[Library(pypi=PythonPyPiLibrary("pandas"))],
    )
    j = make_job(tasks=[task])

    problems = simple_ctx.workflow_linter.lint_job(j.job_id)

    assert len(problems) == 1


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
    make_notebook(path=notebook, content=b"import pytest")

    job_without_pytest_library = make_job(notebook_path=notebook)
    problems = simple_ctx.workflow_linter.lint_job(job_without_pytest_library.job_id)

    assert len([problem for problem in problems if problem.message == "Could not locate import: pytest"]) > 0

    library = compute.Library(pypi=compute.PythonPyPiLibrary(package="pytest"))
    job_with_pytest_library = make_job(notebook_path=notebook, libraries=[library])

    problems = simple_ctx.workflow_linter.lint_job(job_with_pytest_library.job_id)

    assert len([problem for problem in problems if problem.message == "Could not locate import: pytest"]) == 0


def test_lint_local_code(simple_ctx):
    # no need to connect
    light_ctx = simple_ctx.replace(languages=Languages(MigrationIndex([])))
    ucx_path = Path(__file__).parent.parent.parent.parent
    path_to_scan = Path(ucx_path, "src")
    linter = LocalCodeLinter(
        light_ctx.file_loader,
        light_ctx.folder_loader,
        light_ctx.path_lookup,
        light_ctx.dependency_resolver,
        lambda: light_ctx.languages,
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

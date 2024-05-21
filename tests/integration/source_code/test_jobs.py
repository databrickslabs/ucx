import logging
from dataclasses import replace
from pathlib import Path

import pytest
from databricks.sdk.service import compute

from databricks.labs.ucx.mixins.wspath import WorkspacePath
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.whitelist import Whitelist


def test_running_real_workflow_linter_job(installation_ctx):
    ctx = installation_ctx
    ctx.workspace_installation.run()
    ctx.deployed_workflows.run_workflow("experimental-workflow-linter")
    ctx.deployed_workflows.validate_step("experimental-workflow-linter")
    cursor = ctx.sql_backend.fetch(f"SELECT COUNT(*) AS count FROM {ctx.inventory_database}.workflow_problems")
    result = next(cursor)
    assert result['count'] > 0


@pytest.fixture
def simple_ctx(installation_ctx, sql_backend, ws):
    return installation_ctx.replace(
        sql_backend=sql_backend,
        workspace_client=ws,
        connect=ws.config,
    )


def test_linter_from_context(simple_ctx):
    # This code is essentially the same as in test_running_real_workflow_linter_job,
    # but it's executed on the caller side and is easier to debug.
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


def test_job_linter_some_notebook_graph_with_problems(simple_ctx, ws, make_job, make_notebook, make_random, caplog):
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
    assert messages == {
        'second_notebook:4 [direct-filesystem-access] The use of default dbfs: references is deprecated: /mnt/something',
        'some_file.py:1 [direct-filesystem-access] The use of default dbfs: references is deprecated: /mnt/foo/bar',
        'some_file.py:1 [dbfs-usage] Deprecated file system path in call to: /mnt/foo/bar',
        'second_notebook:4 [dbfs-usage] Deprecated file system path in call to: /mnt/something',
    }
    assert all(any(message.endswith(expected) for message in caplog.messages[-1].split("\n")) for expected in messages)


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
        whitelist=Whitelist(use_defaults=False),  # pytest is in default list
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

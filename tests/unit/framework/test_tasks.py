import logging
import shutil
from pathlib import Path
from unittest.mock import create_autospec

import pytest
from databricks.labs.blueprint.installation import MockInstallation
from databricks.labs.lsql.backends import RuntimeBackend
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.framework.tasks import (
    Task,
    TaskLogger,
    parse_args,
    remove_extra_indentation,
    run_task,
    task,
)


def test_replace_pydoc():
    doc = remove_extra_indentation(
        """Test1
        Test2
    Test3"""
    )
    assert (
        doc
        == """Test1
    Test2
Test3"""
    )


def test_task_cloud():
    ws = create_autospec(WorkspaceClient)
    ws.config.is_aws = True
    ws.config.is_azure = False
    ws.config.is_gcp = False

    tasks = [
        Task(task_id=0, workflow="wl_1", name="n3", doc="d3", fn=lambda: None, cloud="aws"),
        Task(task_id=1, workflow="wl_2", name="n2", doc="d2", fn=lambda: None, cloud="azure"),
        Task(task_id=2, workflow="wl_1", name="n1", doc="d1", fn=lambda: None, cloud="gcp"),
    ]

    filter_tasks = sorted([t.name for t in tasks if t.cloud_compatible(ws.config)])
    assert filter_tasks == ["n3"]


def test_task_logger(tmp_path):
    app_logger = logging.getLogger("databricks.labs.ucx.foo")
    databricks_logger = logging.getLogger("databricks.sdk.core")
    with TaskLogger(tmp_path, "assessment", "123", "crawl-tables", "234") as task_logger:
        app_logger.info(f"log file is {task_logger.log_file}")
        databricks_logger.debug("something from sdk")
    contents = _log_contents(tmp_path)
    assert len(contents) == 2
    assert "log file is" in contents["logs/assessment/run-234/crawl-tables.log"]
    assert "something from sdk" in contents["logs/assessment/run-234/crawl-tables.log"]
    assert "[run #234](/#job/123/run/234)" in contents["logs/assessment/run-234/README.md"]


def test_task_failure(tmp_path):
    with pytest.raises(ValueError):
        with TaskLogger(tmp_path, "assessment", "123", "crawl-tables", "234"):
            raise ValueError("some value not found")
    contents = _log_contents(tmp_path)
    assert len(contents) == 2
    # CLI debug info present
    assert "databricks workspace export" in contents["logs/assessment/run-234/crawl-tables.log"]
    # log file name present
    assert "logs/assessment/run-234/crawl-tables.log" in contents["logs/assessment/run-234/crawl-tables.log"]
    # traceback present
    assert 'raise ValueError("some value not found")' in contents["logs/assessment/run-234/crawl-tables.log"]


def _log_contents(tmp_path):
    contents = {}
    for path in tmp_path.glob("**/*"):
        if path.is_dir():
            continue
        contents[path.relative_to(tmp_path).as_posix()] = path.read_text()
    return contents


def test_parse_args():
    args = parse_args("--config=foo", "--task=test")
    assert args["config"] == "foo"
    assert args["task"] == "test"
    with pytest.raises(KeyError):
        parse_args("--foo=bar")


def test_run_task(capsys):
    # mock a task function to be tested
    @task("migrate-tables", job_cluster="migration_sync")
    def mock_migrate_external_tables_sync(cfg, workspace_client, sql_backend, installation):
        """This mock task of migrate-tables"""
        return f"Hello, World! {cfg} {workspace_client} {sql_backend} {installation}"

    args = parse_args("--config=foo", "--task=mock_migrate_external_tables_sync", "--parent_run_id=abc", "--job_id=123")
    cfg = WorkspaceConfig("test_db", log_level="INFO")

    # test the task function is called
    log_path = run_task(
        args, Path("foo"), cfg, create_autospec(WorkspaceClient), create_autospec(RuntimeBackend), MockInstallation()
    )
    # clean up the log folder created by TaskLogger
    log_root_dir = Path(log_path).parents[-2]
    shutil.rmtree(log_root_dir)

    assert "This mock task of migrate-tables" in capsys.readouterr().out

    # test KeyError if task not found
    with pytest.raises(KeyError):
        run_task(
            parse_args("--config=foo", "--task=not_found"),
            Path("foo"),
            cfg,
            create_autospec(WorkspaceClient),
            create_autospec(RuntimeBackend),
            MockInstallation(),
        )

import logging
from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.framework.tasks import (
    Task,
    TaskLogger,
    remove_extra_indentation,
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
        app_logger.info(f"log file is {task_logger._log_file}")
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

import logging

import pytest

from databricks.labs.ucx.framework.tasks import TaskLogger


def test_task_logger(tmp_path):
    app_logger = logging.getLogger("databricks.labs.ucx.foo")
    databricks_logger = logging.getLogger("databricks.sdk.core")
    with TaskLogger(tmp_path, "assessment", "123", "crawl-tables", "234") as task_logger:
        app_logger.info(f"log file is {task_logger._log_file}")
        databricks_logger.debug("something from sdk")
    contents = _log_contents(tmp_path)
    assert 2 == len(contents)
    assert "log file is" in contents["logs/assessment/run-234/crawl-tables.log"]
    assert "something from sdk" in contents["logs/assessment/run-234/crawl-tables.log"]
    assert "[run #234](/#job/123/run/234)" in contents["logs/assessment/run-234/README.md"]


def test_task_failure(tmp_path):
    with pytest.raises(ValueError):
        with TaskLogger(tmp_path, "assessment", "123", "crawl-tables", "234"):
            raise ValueError("some value not found")
    contents = _log_contents(tmp_path)
    assert 2 == len(contents)
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

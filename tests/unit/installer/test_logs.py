import logging
import re
from pathlib import Path

import pytest
from databricks.labs.lsql.backends import MockBackend

from databricks.labs.ucx.framework.tasks import TaskLogger
from databricks.labs.ucx.installer import logs
from databricks.labs.ucx.installer.logs import LogRecord, LogsCrawler


MULTILINE_LOG_MESSAGE = (
    "GET /api/2.0/preview/scim/v2/Groups?attributes=id,displayName,meta,roles,entitlements&startIndex=1&count=100\n"
    "< 200 OK\n"
    "< {\n"
    '<   "Resources": [\n'
    "<     {\n"
    '<       "displayName": "sdk-5cGx,"\n'
    '<       "id": "7325451755795",\n'
    '<       "meta": {\n'
    '<         "resourceType": "WorkspaceGroup"\n'
    "<       }\n"
    "<     }\n"
)
LOG_RECORDS = [
    LogRecord(40, "Message."),
    LogRecord(20, "Other message."),
    LogRecord(30, "Warning message."),
    LogRecord(50, "Watch out!"),
    LogRecord(10, MULTILINE_LOG_MESSAGE.rstrip()),
]


@pytest.fixture()
def log_path(tmp_path: Path) -> Path:
    logger = logging.getLogger("databricks")
    with TaskLogger(
        tmp_path,
        workflow="test",
        workflow_id="123",
        task_name="log-crawler",
        workflow_run_id="abc",
        log_level=logging.DEBUG,
    ) as task_logger:
        for log_record in LOG_RECORDS:
            logger.log(log_record.level, log_record.msg)
        yield task_logger.log_file


def test_parse_logs(log_path: Path) -> None:
    log_records = list(logs.parse_logs(log_path))
    assert log_records == LOG_RECORDS


def test_parse_logs_file_does_not_exists(tmp_path: Path) -> None:
    non_existing_log_path = tmp_path / "does_not_exists.log"
    try:
        list(logs.parse_logs(non_existing_log_path))
    except FileNotFoundError:
        assert False
    else:
        assert True


def test_logs_processor(log_path: Path):
    backend = MockBackend()
    log_processor = LogsCrawler(backend, "default", log_path)
    snapshot = log_processor.snapshot()
    assert all(log_record in snapshot for log_record in LOG_RECORDS)

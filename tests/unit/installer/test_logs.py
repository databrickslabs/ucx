import logging
import re
from pathlib import Path

import pytest
from databricks.labs.lsql.backends import MockBackend

from databricks.labs.ucx.framework.tasks import TaskLogger
from databricks.labs.ucx.installer import logs
from databricks.labs.ucx.installer.logs import PartialLogRecord, LogsRecorder


COMPONENT = "databricks.logs"
TASK_NAME = "parse_logs"
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
    "<     }"
)
PARTIAL_LOG_RECORDS = [
    PartialLogRecord("00:00", "ERROR", COMPONENT, "Message."),
    PartialLogRecord("00:00", "INFO", COMPONENT, "Other message."),
    PartialLogRecord("00:00", "WARNING", COMPONENT, "Warning message."),
    PartialLogRecord("00:00", "CRITICAL", COMPONENT, "Watch out!"),
    PartialLogRecord("00:00", "DEBUG", COMPONENT, MULTILINE_LOG_MESSAGE),
    PartialLogRecord("00:00", "WARNING", COMPONENT, "Last message"),
]


@pytest.fixture()
def log_path(tmp_path: Path) -> Path:
    logger = logging.getLogger(COMPONENT)
    with TaskLogger(
        tmp_path,
        workflow="test",
        workflow_id="123",
        task_name=TASK_NAME,
        workflow_run_id="abc",
        log_level=logging.DEBUG,
    ) as task_logger:
        for log_record in PARTIAL_LOG_RECORDS:
            logger.log(logging.getLevelName(log_record.level), log_record.message)
        yield task_logger.log_file


def test_get_task_names_at_runtime_one_test_task(log_path: Path) -> None:
    task_names = logs._get_task_names_at_runtime(log_path.parent)
    assert len(task_names) == 1
    assert task_names[0] == TASK_NAME


@pytest.mark.parametrize("attribute", ["level", "component", "message"])
def test_parse_logs_attributes(log_path: Path, attribute: str) -> None:
    expected_partial_log_records = [
        getattr(partial_log_record, attribute)
        for partial_log_record in PARTIAL_LOG_RECORDS
    ]
    with log_path.open("r") as f:
        partial_log_records = list(
            getattr(partial_log_record, attribute)
            for partial_log_record in logs.parse_logs(f)
        )
    assert partial_log_records == expected_partial_log_records


def test_parse_logs_last_message_is_present(log_path: Path) -> None:
    with log_path.open("r") as f:
        log_records = list(logs.parse_logs(f))
    assert log_records[-1].message == PARTIAL_LOG_RECORDS[-1].message


def test_logs_processor(log_path: Path):
    backend = MockBackend()
    log_processor = LogsRecorder(backend, "default", log_path)
    log_records = log_processor.record()
    assert all(log_record in log_records for log_record in PARTIAL_LOG_RECORDS)

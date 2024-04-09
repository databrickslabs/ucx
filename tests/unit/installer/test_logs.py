import datetime
import logging
from collections.abc import Iterator
from pathlib import Path

import pytest
from databricks.labs.lsql.backends import MockBackend

from databricks.labs.ucx.framework.tasks import TaskLogger
from databricks.labs.ucx.installer import logs
from databricks.labs.ucx.installer.logs import TaskRunWarningRecorder, PartialLogRecord

COMPONENT = "databricks.logs"
WORKFLOW = "tests"
WORKFLOW_ID = 123
TASK_NAME = "parse_logs"
WORKFLOW_RUN_ID = 456
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
    PartialLogRecord("15", "07", "10", "ERROR", COMPONENT, "Message."),
    PartialLogRecord("15", "07", "12", "INFO", COMPONENT, "Other message."),
    PartialLogRecord("15", "07", "15", "WARNING", COMPONENT, "Warning message."),
    PartialLogRecord("15", "08", "23", "CRITICAL", COMPONENT, "Watch out!"),
    PartialLogRecord("15", "12", "20", "ERROR", COMPONENT, MULTILINE_LOG_MESSAGE),
    PartialLogRecord("15", "12", "21", "WARNING", COMPONENT, "Last message"),
]


@pytest.fixture()
def log_path(tmp_path: Path) -> Iterator[Path]:
    """The TaskLogger is reused so that parsing works against the currnet log format."""
    logger = logging.getLogger(COMPONENT)
    with TaskLogger(
        tmp_path,
        workflow=WORKFLOW,
        workflow_id=str(WORKFLOW_ID),
        task_name=TASK_NAME,
        workflow_run_id=str(WORKFLOW_RUN_ID),
        log_level=logging.DEBUG,
    ) as task_logger:
        for log_record in PARTIAL_LOG_RECORDS:
            logger.log(logging.getLevelName(log_record.level), log_record.message)
        yield task_logger.log_file


@pytest.mark.parametrize("attribute", ["level", "component", "message"])
def test_parse_logs_attributes(log_path: Path, attribute: str) -> None:
    """Verify the parsing of the logs.

    The time attribute are not tested as these are set differently on each test run.
    """
    expected_partial_log_records = [
        getattr(partial_log_record, attribute)
        for partial_log_record in PARTIAL_LOG_RECORDS
    ]
    with log_path.open("r") as f:
        partial_log_records = list(getattr(partial_log_record, attribute) for partial_log_record in logs.parse_logs(f))
    assert partial_log_records == expected_partial_log_records


def test_parse_logs_last_message_is_present(log_path: Path) -> None:
    """Verify if the last log message is present."""
    with log_path.open("r") as f:
        log_records = list(logs.parse_logs(f))
    assert log_records[-1].message == PARTIAL_LOG_RECORDS[-1].message


@pytest.mark.parametrize("attribute", ["level", "component", "message"])
def test_logs_processor_all(tmp_path: Path, log_path: Path, attribute: str):
    """End-to-end test for parsing logs with LogsRecorder."""
    expected_log_records = [
        getattr(partial_log_record, attribute)
        for partial_log_record in PARTIAL_LOG_RECORDS
        if logging.getLevelName(partial_log_record.level) >= logging.WARNING
    ]

    log_creation_time = log_path.stat().st_ctime
    log_creation_timestamp = datetime.datetime.utcfromtimestamp(log_creation_time)

    backend = MockBackend()
    log_processor = TaskRunWarningRecorder(
        tmp_path,
        WORKFLOW,
        WORKFLOW_ID,
        WORKFLOW_RUN_ID,
        backend,
        "default",
    )
    with log_path.open("r") as log:
        log_records = list(
            getattr(partial_log_record, attribute)
            for partial_log_record in log_processor.record(TASK_NAME, log, log_creation_timestamp)
        )
    assert log_records == expected_log_records

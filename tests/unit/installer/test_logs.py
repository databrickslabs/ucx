import datetime as dt
import io
import logging
from collections.abc import Iterator
from pathlib import Path

import pytest
from databricks.labs.lsql.backends import MockBackend
from databricks.sdk.errors import InternalError

from databricks.labs.ucx.installer import logs
from databricks.labs.ucx.installer.logs import TaskRunWarningRecorder, PartialLogRecord, TaskLogger
from tests.unit.framework.test_tasks import _log_contents

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
    PartialLogRecord(dt.time(15, 7, 10), "ERROR", COMPONENT, "Message."),
    PartialLogRecord(dt.time(15, 7, 12), "INFO", COMPONENT, "Other message."),
    PartialLogRecord(dt.time(15, 7, 15), "WARNING", COMPONENT, "Warning message."),
    PartialLogRecord(dt.time(15, 8, 23), "CRITICAL", COMPONENT, "Watch out!"),
    PartialLogRecord(dt.time(15, 12, 20), "ERROR", COMPONENT, MULTILINE_LOG_MESSAGE),
    PartialLogRecord(dt.time(15, 12, 21), "WARNING", COMPONENT, "Last message"),
]


@pytest.fixture()
def log_path(tmp_path: Path) -> Iterator[Path]:
    """Test log parsing against the format used in TaskLogger"""
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
        getattr(partial_log_record, attribute) for partial_log_record in PARTIAL_LOG_RECORDS
    ]
    with log_path.open("r") as f:
        partial_log_records = list(getattr(partial_log_record, attribute) for partial_log_record in logs.parse_logs(f))
    assert partial_log_records == expected_partial_log_records


def test_parse_logs_time(log_path: Path) -> None:
    """Verify the time of the log record lays within the short history."""
    current_time = dt.datetime.now().time()
    two_minutes_ago = (dt.datetime.now() - dt.timedelta(minutes=2)).time()

    with log_path.open("r") as f:
        partial_log_records = list(logs.parse_logs(f))
    assert all(two_minutes_ago <= partial_log_record.time <= current_time for partial_log_record in partial_log_records)


def test_parse_logs_last_message_is_present(log_path: Path) -> None:
    """Verify if the last log message is present."""
    with log_path.open("r") as f:
        log_records = list(logs.parse_logs(f))
    assert log_records[-1].message == PARTIAL_LOG_RECORDS[-1].message


@pytest.mark.parametrize("attribute", ["level", "component", "message"])
def test_logs_processor_snapshot_rows(tmp_path: Path, log_path: Path, attribute: str):
    """Verify the rows created by the snapshot"""
    excpected_log_records = [
        log_record for log_record in PARTIAL_LOG_RECORDS if logging.getLevelName(log_record.level) >= logging.WARNING
    ]
    backend = MockBackend()
    log_processor = TaskRunWarningRecorder(
        tmp_path,
        WORKFLOW,
        WORKFLOW_ID,
        WORKFLOW_RUN_ID,
        backend,
        "default",
    )
    with pytest.raises(InternalError):
        log_processor.snapshot()
    rows = backend.rows_written_for(log_processor.full_name, "append")
    assert all(
        getattr(row, attribute) == getattr(log_record, attribute)
        for row, log_record in zip(rows, excpected_log_records)
    )


def test_logs_processor_snapshot_error(tmp_path: Path, log_path: Path):
    """Test the error raised by the snapshot"""
    backend = MockBackend()
    log_processor = TaskRunWarningRecorder(
        tmp_path,
        WORKFLOW,
        WORKFLOW_ID,
        WORKFLOW_RUN_ID,
        backend,
        "default",
    )
    with pytest.raises(InternalError) as e:
        log_processor.snapshot()
    assert "Watch out!" in e.value.args[0]
    assert "Warning message." not in e.value.args[0]


def test_task_failure(tmp_path):
    with pytest.raises(ValueError):
        with TaskLogger(tmp_path, "assessment", "123", "crawl-tables", "234"):
            raise ValueError("some value not found")
    contents = _log_contents(tmp_path)
    assert len(contents) == 2
    # CLI debug info present
    assert "databricks workspace export" in contents["logs/assessment/run-234-0/crawl-tables.log"]
    # log file name present
    assert "logs/assessment/run-234-0/crawl-tables.log" in contents["logs/assessment/run-234-0/crawl-tables.log"]
    # traceback present
    assert 'raise ValueError("some value not found")' in contents["logs/assessment/run-234-0/crawl-tables.log"]


def test_task_logger(tmp_path):
    app_logger = logging.getLogger("databricks.labs.ucx.foo")
    databricks_logger = logging.getLogger("databricks.sdk.core")
    with TaskLogger(tmp_path, "assessment", "123", "crawl-tables", "234") as task_logger:
        app_logger.info(f"log file is {task_logger.log_file}")
        databricks_logger.debug("something from sdk")
    contents = _log_contents(tmp_path)
    assert len(contents) == 2
    assert "log file is" in contents["logs/assessment/run-234-0/crawl-tables.log"]
    assert "something from sdk" in contents["logs/assessment/run-234-0/crawl-tables.log"]
    assert "[run #234](/#job/123/run/234)" in contents["logs/assessment/run-234-0/README.md"]


def test_parse_logs_warns_for_corrupted_log_file(caplog):
    corrupted_log_line = "13:56:47  INF"
    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.installer.logs"):
        list(logs.parse_logs(io.StringIO(corrupted_log_line)))

    last_message = caplog.messages[-1]
    assert "Logs do not match expected format" in last_message
    assert last_message.endswith(corrupted_log_line)

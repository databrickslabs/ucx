import re
from pathlib import Path

import pytest
from databricks.labs.lsql.backends import MockBackend

from databricks.labs.ucx.hive_metastore import logs
from databricks.labs.ucx.hive_metastore.logs import LogRecord, LogsCrawler

LOGS = [
    "07:09 ERROR [module] Message.\n",
    "07:09 INFO [module] Other message.\n",
    "07:09 WARNING [module] Warning message.\n",
    "07:09 CRITICAL [module] Watch out!\n",
]
LOG_RECORDS = [
    LogRecord(40, "Message."),
    LogRecord(20, "Other message."),
    LogRecord(30, "Warning message."),
    LogRecord(50, "Watch out!"),
]


@pytest.fixture()
def log_path(tmp_path: Path) -> Path:
    _log_path = tmp_path / "test.log"
    with _log_path.open("w") as f:
        f.writelines(LOGS)
    return _log_path


@pytest.mark.parametrize("line,expected_log_record", list(zip(LOGS, LOG_RECORDS)))
def test_parse_log_record_examples(line: str, expected_log_record: LogRecord) -> None:
    log_format = r"\d+:\d+\s(\w+)\s\[.+\]\s(.+)"
    pattern = re.compile(log_format)

    log_record = logs.parse_log_record(line, pattern)
    assert log_record == expected_log_record


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
    assert snapshot == LOG_RECORDS

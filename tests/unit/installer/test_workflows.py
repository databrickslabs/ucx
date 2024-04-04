import re
from pathlib import Path

import pytest

from databricks.labs.ucx.installer import workflows
from databricks.labs.ucx.installer.workflows import LogRecord

LOGS = (
    "07:09 ERROR [module] Message.",
    "07:09 INFO [module] Other message.",
    "07:09 WARNING [module] Warning message.",
    "07:09 CRITICAL [module] Watch out!",
)
LOG_RECORDS = (
    LogRecord(40, "Message."),
    LogRecord(20, "Other message."),
    LogRecord(30, "Warning message."),
    LogRecord(50, "Watch out!"),
)


@pytest.mark.parametrize("line,expected_log_record", list(zip(LOGS, LOG_RECORDS)))
def test_parse_log_record_examples(line: str, expected_log_record: LogRecord) -> None:
    log_format = r"\d+:\d+\s(\w+)\s\[.+\]\s(.+)"
    pattern = re.compile(log_format)

    log_record = workflows.parse_log_record(line, pattern)
    assert log_record == expected_log_record


def test_parse_logs(tmp_path: Path) -> None:
    log_path = tmp_path / "test.log"
    with log_path.open("w") as f:
        f.writelines(LOGS)

    log_records = list(workflows.parse_logs(log_path))
    assert log_records == LOG_RECORDS

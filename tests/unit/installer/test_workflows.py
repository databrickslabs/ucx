import re

import pytest

from databricks.labs.ucx.installer import workflows
from databricks.labs.ucx.installer.workflows import LogRecord


@pytest.mark.parametrize(
    "line,expected_log_record",
    [
        (
            "07:09 ERROR [module] Message.",
            LogRecord(40, "Message."),
        )
    ],
)
def test_parse_log_record_examples(line: str, expected_log_record: LogRecord) -> None:
    log_format = r"\d+:\d+\s(\w+)\s\[.+\]\s(.+)"
    pattern = re.compile(log_format)

    log_record = workflows.parse_log_record(line, pattern)
    assert log_record == expected_log_record

import dataclasses
import logging
import re
from collections.abc import Iterator
from pathlib import Path


@dataclasses.dataclass
class LogRecord:
    """A subset from logging.LogRecord.

    Sources
    -------
    https://docs.python.org/3/library/logging.html#logging.LogRecord
    """

    level: int
    msg: str


def parse_log_record(line: str, pattern: re.Pattern) -> LogRecord | None:
    match = pattern.match(line)
    if match is None:
        log_record = None
    else:
        level, msg = match.groups()
        log_record = LogRecord(logging.getLevelName(level), msg)
    return log_record


def parse_logs(*log_paths: Path) -> Iterator[LogRecord]:
    log_format = r"\d+:\d+\s(\w+)\s\[.+\]\s(.+)"
    pattern = re.compile(log_format)

    for log_path in log_paths:
        with log_path.open("r") as f:
            for line in f.readlines():
                log_record = parse_log_record(line, pattern)
                if log_record is not None:
                    yield log_record

import dataclasses
import logging
import re
from collections.abc import Iterator
from pathlib import Path

from databricks.labs.lsql.backends import SqlBackend

from databricks.labs.ucx.framework.crawlers import CrawlerBase


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
    # TODO: Add test to cover a change to the log format
    log_format = r"\d+:\d+\s(\w+)\s\[.+\]\s(.+)"
    pattern = re.compile(log_format)

    for log_path in log_paths:
        with log_path.open("r") as f:
            for line in f.readlines():
                log_record = parse_log_record(line, pattern)
                if log_record is not None:
                    yield log_record


# TODO: Is log processor a crawler? -> Rename to be accurate
class LogsProcessor(CrawlerBase):
    def __init__(self, backend: SqlBackend, schema: str, log_paths: list[Path]):
        """
        Initializes a LogProcessor instance.

        Args:
            backend (SqlBackend): The SQL Execution Backend abstraction (either REST API or Spark)
            schema: The schema name for the logs persistence.
        """
        super().__init__(backend, "hive_metastore", schema, "logs", LogRecord)
        self._log_paths = log_paths

    def process(self) -> None:
        log_records = list(parse_logs(*self._log_paths))
        self._append_records(log_records)

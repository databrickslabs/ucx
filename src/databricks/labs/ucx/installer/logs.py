import dataclasses
import logging
import re
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

from databricks.labs.lsql.backends import SqlBackend

from databricks.labs.ucx.framework.crawlers import CrawlerBase

logger = logging.getLogger(__name__)


@dataclass
class LogRecord:
    ts: int  # fully specified timestamp in UTC (offset from a workspace fs file creation timestamp)
    job_id: int  # determined from workspace file path, resolved from the install state
    workflow_name: str
    task_name: str
    job_run_id: int
    level: str
    component: str
    message: str


@dataclass
class PartialLogRecord:
    """The information found within a log file record."""
    timestamp_offset: str
    level: str
    component: str
    message: str


def parse_logs(*log_paths: Path) -> Iterator[PartialLogRecord]:
    log_format = r"(\d+:\d+:\d+)\s(\w+)\s\[(.+)\]\s\{\w+\}\s(.+)"
    pattern = re.compile(log_format)

    for log_path in log_paths:
        if not log_path.is_file():
            logger.info("Log file does not exists: {%s}", log_path)
            continue
        with log_path.open("r") as f:
            line = f.readline()
            match = pattern.match(line)
            while len(line) > 0:
                # Logs spanning multilines do not match the regex on each subsequent line
                multi_line_message = ""
                next_line = f.readline()
                next_match = pattern.match(next_line)
                while len(next_line) > 0 and next_match is None:
                    multi_line_message += "\n" + next_line.rstrip()
                    next_line = f.readline()
                    next_match = pattern.match(next_line)

                assert match is not None
                timestamp_offset, level, component, message = match.groups()

                partial_log_record = PartialLogRecord(
                    timestamp_offset,
                    level,
                    component,
                    message + multi_line_message,
                )
                yield partial_log_record

                line, match = next_line, next_match


class LogsRecorder:
    def __init__(self, backend: SqlBackend, schema: str, *log_paths: Path):
        """
        Initializes a LogProcessor instance.

        Args:
            backend (SqlBackend): The SQL Execution Backend abstraction (either REST API or Spark)
            schema: The schema name for the logs persistence.
            log_paths: The paths to the log files.
        """
        self._catalog = "hive_metastore"
        self._table = "logs"

        self._backend = backend
        self._schema = schema
        self._log_paths = log_paths

    @property
    def full_name(self) -> str:
        return f"{self._catalog}.{self._schema}.{self._table}"

    def record(self) -> list[LogRecord]:
        log_records = list(parse_logs(*self._log_paths))
        self._backend.save_table(
            self.full_name,
            log_records,
            LogRecord,
            mode="append",
        )
        return log_records
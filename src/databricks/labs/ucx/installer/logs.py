import datetime
import logging
import re
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import TextIO

from databricks.labs.lsql.backends import SqlBackend

logger = logging.getLogger(__name__)


@dataclass
class LogRecord:
    timestamp: int
    job_id: int
    workflow_name: str
    task_name: str
    job_run_id: int
    level: str
    component: str
    message: str


@dataclass
class PartialLogRecord:
    """The information found within a log file record."""

    hour: str
    minute: str
    second: str
    level: str
    component: str
    message: str


def peak_multi_line_message(log: TextIO, pattern: re.Pattern) -> tuple[str, re.Match | None, str]:
    """
    A single log record message may span multiple log lines. In this case, the regex on
    subsequent lines do not match.

    Args:
         log (TextIO): The log file IO.
         pattern (re.Pattern): The regex pattern for a log line.
    """
    multi_line_message = ""
    line = log.readline()
    match = pattern.match(line)
    while len(line) > 0 and match is None:
        multi_line_message += "\n" + line.rstrip()
        line = log.readline()
        match = pattern.match(line)
    return line, match, multi_line_message


def parse_logs(log: TextIO) -> Iterator[PartialLogRecord]:
    """Parse the logs to retrieve values for PartialLogRecord fields.

    Args:
         log (TextIO): The log file IO.
    """
    # This regex matches the log format defined in databricks.labs.ucx.installer.logs.TaskLogger
    log_format = r"(\d+):(\d+):(\d+)\s(\w+)\s\[(.+)\]\s\{\w+\}\s(.+)"
    pattern = re.compile(log_format)

    line = log.readline()
    match = pattern.match(line)
    while len(line) > 0:
        assert match is not None
        *groups, message = match.groups()

        next_line, next_match, multi_line_message = peak_multi_line_message(log, pattern)

        # Mypy can't determine length of regex expressions
        partial_log_record = PartialLogRecord(*groups, message + multi_line_message)  # type: ignore

        yield partial_log_record

        line, match = next_line, next_match


class LogsRecorder:
    def __init__(
        self,
        install_dir: Path | str,
        workflow: str,
        job_id: int,
        job_run_id: int,
        sql_backend: SqlBackend,
        schema: str,
        *,
        minimum_log_level: int = logging.WARNING,
    ):
        """
        Initializes a LogProcessor instance.

        Args:
            install_dir (Path | str): The installation folder on WorkspaceFS
            workflow (str): The workflow name.
            job_id (int): The job id of the job to store the log records for.
            job_run_id (int): The job run id of the job to store the log records for.
            sql_backend (SqlBackend): The SQL Execution Backend abstraction (either REST API or Spark)
            schema (str): The schema name for the logs persistence.
            minimum_log_level (int) : The minimum log level to record, all records with a lower log level are excluded.
        """
        self._catalog = "hive_metastore"
        self._table = "logs"

        self._workflow = workflow
        self._job_id = job_id
        self._job_run_id = job_run_id
        self._sql_backend = sql_backend
        self._schema = schema
        self._minimum_log_level = minimum_log_level

        self.log_path = Path(install_dir) / "logs" / self._workflow / f"run-{self._job_run_id}"

    @property
    def full_name(self) -> str:
        return f"{self._catalog}.{self._schema}.{self._table}"

    def record(self, task_name: str, log: TextIO, log_creation_timestamp: datetime.datetime) -> list[LogRecord]:
        """Record the logs of a given task.

        Args:
            task_name (str): The name of the task
            log (TextIO): The task log
            log_creation_timestamp (datetime.datetime): The log creation timestamp.
        """
        log_records = [
            LogRecord(
                timestamp=int(
                    log_creation_timestamp.replace(
                        hour=int(partial_log_record.hour),
                        minute=int(partial_log_record.minute),
                        second=int(partial_log_record.second),
                    ).timestamp()
                ),
                job_id=self._job_id,
                workflow_name=self._workflow,
                task_name=task_name,
                job_run_id=self._job_run_id,
                level=partial_log_record.level,
                component=partial_log_record.component,
                message=partial_log_record.message,
            )
            for partial_log_record in parse_logs(log)
            if logging.getLevelName(partial_log_record.level) >= self._minimum_log_level
        ]
        self._sql_backend.save_table(
            self.full_name,
            log_records,
            LogRecord,
            mode="append",
        )
        return log_records

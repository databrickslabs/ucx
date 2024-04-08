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
    ts: int  # fully specified timestamp in UTC (offset from a workspace fs file creation timestamp)
    job_id: int  # determined from workspace file path, resolved from the install state
    workflow_name: str
    task_name: str
    job_run_id: int
    level: str
    component: str
    message: str


@dataclass
class _PartialLogRecord:
    """The information found within a log file record."""

    hour: str
    minute: str
    second: str
    level: str
    component: str
    message: str


def _get_task_names_at_runtime(log_path: Path) -> list[str]:
    log_files = log_path.glob("*.log")
    task_names = [log_file.stem for log_file in log_files]
    return task_names


def _peak_multi_line_message(log: TextIO, pattern: re.Pattern) -> tuple[str, re.Match, str]:
    """
    A single log record message may span multiple log lines. In this case, the regex on
    subsequent lines do not match.
    """
    multi_line_message = ""
    next_line = log.readline()
    next_match = pattern.match(next_line)
    while len(next_line) > 0 and next_match is None:
        multi_line_message += "\n" + next_line.rstrip()
        next_line = log.readline()
        next_match = pattern.match(next_line)
    assert next_match is not None
    return next_line, next_match, multi_line_message


def _parse_logs(log: TextIO) -> Iterator[_PartialLogRecord]:
    # This regex matches the log format defined in
    log_format = r"(\d+):(\d+):(\d+)\s(\w+)\s\[(.+)\]\s\{\w+\}\s(.+)"
    pattern = re.compile(log_format)

    line = log.readline()
    match = pattern.match(line)
    while len(line) > 0:
        assert match is not None
        *groups, message = match.groups()

        next_line, next_match, multi_line_message = _peak_multi_line_message(log, pattern)
        partial_log_record = _PartialLogRecord(*groups, message + multi_line_message)
        yield partial_log_record

        line, match = next_line, next_match


class LogsRecorder:
    def __init__(
        self,
        install_dir: Path,
        workflow: str,
        job_id: int,
        job_run_id: int,
        backend: SqlBackend,
        schema: str,
        *,
        minimum_log_level: int = logging.WARNING,
    ):
        """
        Initializes a LogProcessor instance.

        Args:
            install_dir (str): The installation folder on WorkspaceFS
            workflow (str): The workflow name.
            job_id (int): The job id of the job to store the log records for.
            job_run_id (int): The job run id of the job to store the log records for.
            backend (SqlBackend): The SQL Execution Backend abstraction (either REST API or Spark)
            schema (str): The schema name for the logs persistence.
            minimum_log_level (int) : The minimum log level to record, all records with a lower log level are excluded.
        """
        self._catalog = "hive_metastore"
        self._table = "logs"

        self._workflow = workflow
        self._job_id = job_id
        self._job_run_id = job_run_id
        self._backend = backend
        self._schema = schema
        self._minimum_log_level = minimum_log_level

        self._log_path = install_dir / "logs" / self._workflow / f"run-{self._job_run_id}"

    @property
    def full_name(self) -> str:
        return f"{self._catalog}.{self._schema}.{self._table}"

    def get_task_log_path(self, task_name: str) -> Path:
        return self._log_path / f"{task_name}.json"

    def get_task_names_at_runtime(self) -> list[str]:
        return _get_task_names_at_runtime(self._log_path)

    def record(self, task_name: str, log: TextIO, log_creation_timestamp: datetime.datetime) -> list[LogRecord]:
        log_records = [
            LogRecord(
                ts=int(
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
            for partial_log_record in _parse_logs(log)
            if logging.getLevelName(partial_log_record.level) >= self._minimum_log_level
        ]
        self._backend.save_table(
            self.full_name,
            log_records,
            LogRecord,
            mode="append",
        )
        return log_records

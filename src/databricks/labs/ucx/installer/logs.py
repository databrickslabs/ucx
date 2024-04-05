import dataclasses
import logging
import re
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import TextIO

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


def _get_task_names_at_runtime(log_path: Path) -> list[str]:
    log_files = log_path.glob("*.log")
    task_names = [log_file.stem for log_file in log_files]
    return task_names


def parse_logs(log: TextIO) -> Iterator[PartialLogRecord]:
    log_format = r"(\d+:\d+:\d+)\s(\w+)\s\[(.+)\]\s\{\w+\}\s(.+)"
    pattern = re.compile(log_format)

    line = log.readline()
    match = pattern.match(line)
    while len(line) > 0:
        # Logs spanning multilines do not match the regex on each subsequent line
        multi_line_message = ""
        next_line = log.readline()
        next_match = pattern.match(next_line)
        while len(next_line) > 0 and next_match is None:
            multi_line_message += "\n" + next_line.rstrip()
            next_line = log.readline()
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
    def __init__(
            self,
            install_dir: Path,
            job_id: int,
            job_run_id: int,
            workflow: str,
            task_names: list[str],
            backend: SqlBackend,
            schema: str,
    ):
        """
        Initializes a LogProcessor instance.

        Args:
            install_dir (str): The installation folder on WorkspaceFS
            job_id (int): The job id of the job to store the log records for.
            job_run_id (int): The job run id of the job to store the log records for.
            workflow (str): The workflow name.
            backend (SqlBackend): The SQL Execution Backend abstraction (either REST API or Spark)
            schema (str): The schema name for the logs persistence.
        """
        self._catalog = "hive_metastore"
        self._table = "logs"

        self._workflow = workflow
        self._job_id = job_id
        self._job_run_id = job_run_id
        self._backend = backend
        self._schema = schema

        self._log_path = install_dir / "logs" / self._workflow / f"run-{self._job_run_id}"

    @property
    def full_name(self) -> str:
        return f"{self._catalog}.{self._schema}.{self._table}"

    def get_task_log_path(self, task_name: str) -> Path:
        return self._log_path / f"{task_name}.json"

    def get_task_names_at_runtime(self) -> list[str]:
        return _get_task_names_at_runtime(self._log_path)

    def record(self, task_name: str, log: TextIO) -> list[LogRecord]:
        log_records = [
            LogRecord(
                # TODO: Implement offset
                ts=partial_log_record.timestamp_offset,
                job_id=self._job_id,
                workflow_name=self._workflow,
                task_name=task_name,
                job_run_id=self._job_run_id,
                level=partial_log_record.level,
                component=partial_log_record.component,
                message=partial_log_record.message,
            )
            for partial_log_record in parse_logs(log)
        ]
        self._backend.save_table(
            self.full_name,
            log_records,
            LogRecord,
            mode="append",
        )
        return log_records
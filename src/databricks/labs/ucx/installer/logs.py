import dataclasses
import logging
import re
from collections.abc import Iterator
from pathlib import Path

from databricks.labs.lsql.backends import SqlBackend

from databricks.labs.ucx.framework.crawlers import CrawlerBase

logger = logging.getLogger(__name__)


@dataclasses.dataclass
class LogRecord:
    """A subset from logging.LogRecord.

    Sources
    -------
    https://docs.python.org/3/library/logging.html#logging.LogRecord
    """

    level: int
    msg: str


def parse_logs(*log_paths: Path) -> Iterator[LogRecord]:
    log_format = r"\d+:\d+:\d+\s(\w+)\s\[.+\]\s\{\w+\}\s(.+)"
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
                level, msg = match.groups()
                log_record = LogRecord(logging.getLevelName(level), msg + multi_line_message)
                yield log_record

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
        super().__init__(backend, "hive_metastore", schema, "logs", LogRecord)
        self._log_paths = log_paths

    def snapshot(self) -> list[LogRecord]:
        return self._snapshot(self._try_fetch, self._crawl)

    def _try_fetch(self) -> Iterator[LogRecord]:
        for row in self._fetch(f"SELECT * FROM {self.full_name}"):
            yield LogRecord(*row)

    def _crawl(self) -> Iterator[LogRecord]:
        yield from parse_logs(*self._log_paths)

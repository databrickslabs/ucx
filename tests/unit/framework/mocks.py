import logging
import re
from collections.abc import Iterator

from databricks.labs.ucx.framework.crawlers import SqlBackend

logger = logging.getLogger(__name__)


class MockBackend(SqlBackend):
    def __init__(self, *, fails_on_first: dict | None = None, rows: dict | None = None):
        self._fails_on_first = fails_on_first
        if not rows:
            rows = {}
        self._rows = rows
        self._save_table = []
        self._create_table = []
        self.queries = []

    def _sql(self, sql):
        logger.debug(f"Mock backend.sql() received SQL: {sql}")
        seen_before = sql in self.queries
        self.queries.append(sql)
        if not seen_before and self._fails_on_first is not None:
            for match, failure in self._fails_on_first.items():
                if match in sql:
                    raise RuntimeError(failure)

    def execute(self, sql):
        self._sql(sql)

    def fetch(self, sql) -> Iterator[any]:
        self._sql(sql)
        rows = []
        if self._rows:
            for pattern in self._rows.keys():
                r = re.compile(pattern)
                if r.match(sql):
                    logger.debug(f"Found match: {sql}")
                    rows.extend(self._rows[pattern])
        logger.debug(f"Returning rows: {rows}")
        return iter(rows)

    def save_table(self, full_name: str, rows: list[any], mode: str = "append"):
        self._save_table.append((full_name, rows, mode))

    def create_empty_table(self, full_name: str, klass):
        self._create_table.append((full_name, klass))

    def rows_written_for(self, full_name: str, mode: str) -> list[any]:
        rows = []
        for stub_full_name, stub_rows, stub_mode in self._save_table:
            if not (stub_full_name == full_name and stub_mode == mode):
                continue
            rows += stub_rows
        return rows

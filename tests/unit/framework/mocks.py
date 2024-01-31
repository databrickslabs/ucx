import logging
import re
from collections.abc import Iterator, Sequence

from databricks.labs.ucx.framework.crawlers import DataclassInstance, SqlBackend
from databricks.labs.ucx.mixins.sql import Row

logger = logging.getLogger(__name__)


class MockBackend(SqlBackend):
    def __init__(self, *, fails_on_first: dict | None = None, rows: dict | None = None, debug_truncate_bytes=96):
        self._fails_on_first = fails_on_first
        if not rows:
            rows = {}
        self._rows = rows
        self._save_table: list[tuple[str, Sequence[DataclassInstance], str]] = []
        self._debug_truncate_bytes = debug_truncate_bytes
        self.queries: list[str] = []

    def _sql(self, sql: str):
        logger.debug(f"Mock backend.sql() received SQL: {self._only_n_bytes(sql, self._debug_truncate_bytes)}")
        seen_before = sql in self.queries
        self.queries.append(sql)
        if not seen_before and self._fails_on_first is not None:
            for match, failure in self._fails_on_first.items():
                if match in sql:
                    raise RuntimeError(failure)

    def execute(self, sql):
        self._sql(sql)

    def fetch(self, sql) -> Iterator[Row]:
        self._sql(sql)
        rows = []
        if self._rows:
            for pattern in self._rows.keys():
                r = re.compile(pattern)
                if r.search(sql):
                    logger.debug(f"Found match: {sql}")
                    rows.extend(self._rows[pattern])
        logger.debug(f"Returning rows: {rows}")
        return iter(rows)

    def save_table(self, full_name: str, rows: Sequence[DataclassInstance], klass, mode: str = "append"):
        if klass.__class__ == type:
            self._save_table.append((full_name, rows, mode))

    def rows_written_for(self, full_name: str, mode: str) -> list[DataclassInstance]:
        rows: list[DataclassInstance] = []
        for stub_full_name, stub_rows, stub_mode in self._save_table:
            if not (stub_full_name == full_name and stub_mode == mode):
                continue
            rows += stub_rows
        return rows

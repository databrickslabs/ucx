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
                logger.debug(f"Checking pattern: {pattern}")
                r = re.compile(pattern)
                if r.match(sql):
                    logger.debug(f"Found match: {sql}")
                    rows.extend(self._rows[pattern])
        logger.debug(f"Returning rows: {rows}")
        return iter(rows)

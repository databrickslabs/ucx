from __future__ import annotations

import logging
from collections.abc import Sequence, Iterable

from databricks.labs.ucx.framework.crawlers import CrawlerBase
from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk.errors import DatabricksError

from databricks.labs.ucx.framework.utils import escape_sql_identifier
from databricks.labs.ucx.source_code.base import UsedTable

logger = logging.getLogger(__name__)


class UsedTablesCrawler(CrawlerBase[UsedTable]):

    def __init__(self, backend: SqlBackend, schema: str, table: str) -> None:
        """
        Initializes a DFSACrawler instance.

        Args:
            sql_backend (SqlBackend): The SQL Execution Backend abstraction (either REST API or Spark)
            schema: The schema name for the inventory persistence.
        """
        super().__init__(backend=backend, catalog="hive_metastore", schema=schema, table=table, klass=UsedTable)

    @classmethod
    def for_paths(cls, backend: SqlBackend, schema: str) -> UsedTablesCrawler:
        return UsedTablesCrawler(backend, schema, "used_tables_in_paths")

    @classmethod
    def for_queries(cls, backend: SqlBackend, schema: str) -> UsedTablesCrawler:
        return UsedTablesCrawler(backend, schema, "used_tables_in_queries")

    def dump_all(self, tables: Sequence[UsedTable]) -> None:
        """This crawler doesn't follow the pull model because the fetcher fetches data for 3 crawlers, not just one
        It's not **bad** because all records are pushed at once.
        Providing a multi-entity crawler is out-of-scope of this PR
        """
        try:
            # TODO until we historize data, we append all TableInfos
            self._update_snapshot(tables, mode="append")
        except DatabricksError as e:
            logger.error("Failed to store DFSAs", exc_info=e)

    def _try_fetch(self) -> Iterable[UsedTable]:
        sql = f"SELECT * FROM {escape_sql_identifier(self.full_name)}"
        for row in self._backend.fetch(sql):
            yield self._klass.from_dict(row.as_dict())

    def _crawl(self) -> Iterable[UsedTable]:
        return []
        # TODO raise NotImplementedError() once CrawlerBase supports empty snapshots

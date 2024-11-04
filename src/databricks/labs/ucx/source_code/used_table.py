from __future__ import annotations

import logging
from collections.abc import Sequence, Iterable
from functools import cached_property

from databricks.labs.ucx.framework.crawlers import CrawlerBase
from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk.errors import DatabricksError

from databricks.labs.ucx.framework.owners import (
    AdministratorLocator,
    LegacyQueryOwnership,
    Ownership,
    WorkspacePathOwnership,
)
from databricks.labs.ucx.framework.utils import escape_sql_identifier
from databricks.labs.ucx.source_code.base import UsedTable

logger = logging.getLogger(__name__)


class UsedTablesCrawler(CrawlerBase[UsedTable]):

    def __init__(self, sql_backend: SqlBackend, schema: str, table: str) -> None:
        """
        Initializes a DFSACrawler instance.

        Args:
            sql_backend (SqlBackend): The SQL Execution Backend abstraction (either REST API or Spark)
            schema: The schema name for the inventory persistence.
        """
        super().__init__(sql_backend=sql_backend, catalog="hive_metastore", schema=schema, table=table, klass=UsedTable)

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
            # TODO: @JCZuurmond until we historize data, we append all TableInfos
            self._update_snapshot(tables, mode="append")
        except DatabricksError as e:
            logger.error("Failed to store DFSAs", exc_info=e)

    def _try_fetch(self) -> Iterable[UsedTable]:
        sql = f"SELECT * FROM {escape_sql_identifier(self.full_name)}"
        for row in self._sql_backend.fetch(sql):
            yield self._klass.from_dict(row.asDict())

    def _crawl(self) -> Iterable[UsedTable]:
        return []
        # TODO raise NotImplementedError() once CrawlerBase supports empty snapshots


class UsedTableOwnership(Ownership[UsedTable]):
    """Used table ownership."""

    def __init__(
        self,
        administrator_locator: AdministratorLocator,
        used_tables_in_paths: UsedTablesCrawler,
        used_tables_in_queries: UsedTablesCrawler,
        legacy_query_ownership: LegacyQueryOwnership,
        workspace_path_ownership: WorkspacePathOwnership,
    ) -> None:
        super().__init__(administrator_locator)
        self._used_tables_in_paths = used_tables_in_paths
        self._used_tables_in_queries = used_tables_in_queries
        self._legacy_query_ownership = legacy_query_ownership
        self._workspace_path_ownership = workspace_path_ownership

    @cached_property
    def _used_tables_snapshot(self) -> dict[tuple[str, str, str], UsedTable]:
        index = {}
        for collection in (self._used_tables_in_paths.snapshot(), self._used_tables_in_queries.snapshot()):
            for used_table in collection:
                key = used_table.catalog_name, used_table.schema_name, used_table.table_name
                index[key] = used_table
        return index

    def _maybe_direct_owner(self, record: UsedTable) -> str | None:
        used_table = self._used_tables_snapshot.get((record.catalog_name, record.schema_name, record.table_name))
        if not used_table:
            return None
        # If something writes to a table, then it's an owner of it
        if not used_table.is_write:
            return None
        if used_table.source_type == 'QUERY' and used_table.query_id:
            return self._legacy_query_ownership.owner_of(used_table.query_id)
        if used_table.source_type in {'NOTEBOOK', 'FILE'}:
            return self._workspace_path_ownership.owner_of_path(used_table.source_id)
        logger.warning(f"Unknown source type {used_table.source_type} for {used_table.source_id}")
        return None

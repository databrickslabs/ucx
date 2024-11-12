"""
Separate table ownership module to resolve circular dependency for `:mod:hive_metastore.grants` using `:class:Table`
and `:class:TableOwnership` using the `GrantsCrawler`.
"""

import logging
from functools import cached_property

from databricks.labs.ucx.framework.owners import (
    Ownership,
    AdministratorLocator,
    LegacyQueryOwnership,
    WorkspacePathOwnership,
)
from databricks.labs.ucx.hive_metastore.grants import GrantsCrawler
from databricks.labs.ucx.hive_metastore.tables import Table
from databricks.labs.ucx.source_code.base import UsedTable
from databricks.labs.ucx.source_code.used_table import UsedTablesCrawler


logger = logging.getLogger(__name__)


class TableOwnership(Ownership[Table]):
    """Determine ownership of tables in the inventory based on the following rules:
    - If a table is owned by a principal in the grants table, then that principal is the owner.
    - If a table is written to by a query, then the owner of that query is the owner of the table.
    - If a table is written to by a notebook or file, then the owner of the path is the owner of the table.
    """

    def __init__(
        self,
        administrator_locator: AdministratorLocator,
        grants_crawler: GrantsCrawler,
        used_tables_in_paths: UsedTablesCrawler,
        used_tables_in_queries: UsedTablesCrawler,
        legacy_query_ownership: LegacyQueryOwnership,
        workspace_path_ownership: WorkspacePathOwnership,
    ) -> None:
        super().__init__(administrator_locator)
        self._grants_crawler = grants_crawler
        self._used_tables_in_paths = used_tables_in_paths
        self._used_tables_in_queries = used_tables_in_queries
        self._legacy_query_ownership = legacy_query_ownership
        self._workspace_path_ownership = workspace_path_ownership

    def _maybe_direct_owner(self, record: Table) -> str | None:
        owner = self._maybe_from_grants(record)
        if owner:
            return owner
        return self._maybe_from_sources(record)

    def _maybe_from_sources(self, record: Table) -> str | None:
        used_table = self._used_tables_snapshot.get((record.catalog, record.database, record.name))
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

    @cached_property
    def _used_tables_snapshot(self) -> dict[tuple[str, str, str], UsedTable]:
        index = {}
        for collection in (self._used_tables_in_paths.snapshot(), self._used_tables_in_queries.snapshot()):
            for used_table in collection:
                key = used_table.catalog_name, used_table.schema_name, used_table.table_name
                index[key] = used_table
        return index

    def _maybe_from_grants(self, record: Table) -> str | None:
        for grant in self._grants_snapshot:
            if not grant.action_type == 'OWN':
                continue
            object_type, full_name = grant.this_type_and_key()
            if object_type == 'TABLE' and full_name == record.key:
                return grant.principal
            if object_type in {'DATABASE', 'SCHEMA'} and full_name == f"{record.catalog}.{record.database}":
                return grant.principal
        return None

    @cached_property
    def _grants_snapshot(self):
        return self._grants_crawler.snapshot()

import logging
from collections.abc import Iterable, Callable
from functools import cached_property

from databricks.labs.ucx.framework.owners import (
    Ownership,
    AdministratorLocator,
    LegacyQueryOwnership,
    WorkspacePathOwnership,
)
from databricks.labs.ucx.hive_metastore import TablesCrawler
from databricks.labs.ucx.hive_metastore.grants import GrantsCrawler, Grant
from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationStatus
from databricks.labs.ucx.hive_metastore.tables import Table
from databricks.labs.ucx.source_code.base import UsedTable
from databricks.labs.ucx.source_code.used_table import UsedTablesCrawler
from databricks.labs.ucx.workspace_access.groups import GroupManager

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


class DefaultSecurableOwnership(Ownership[Table]):
    """Determine ownership of tables in the inventory based on the following rules:
    -- If a global owner group is defined, then all tables are owned by that group.
    -- If the user running the application can be identified, then all tables are owned by that user.
    -- Otherwise, the table is owned by the administrator.
    """

    def __init__(
        self,
        administrator_locator: AdministratorLocator,
        table_crawler: TablesCrawler,
        group_manager: GroupManager,
        default_owner_group: str | None,
        app_principal_resolver: Callable[[], str | None],
    ) -> None:
        super().__init__(administrator_locator)
        self._tables_crawler = table_crawler
        self._group_manager = group_manager
        self._default_owner_group = default_owner_group
        self._app_principal_resolver = app_principal_resolver

    @cached_property
    def _application_principal(self) -> str | None:
        return self._app_principal_resolver()

    @cached_property
    def _static_owner(self) -> str | None:
        # If the default owner group is not valid, fall back to the application principal
        if self._default_owner_group and self._group_manager.current_user_in_owner_group(self._default_owner_group):
            logger.warning("Default owner group is not valid, falling back to administrator ownership.")
            return self._default_owner_group
        return self._application_principal

    def load(self) -> Iterable[Grant]:
        databases = set()
        catalogs = set()
        owner = self._static_owner
        if not owner:
            logger.warning("No owner found for tables and databases")
            return
        for table in self._tables_crawler.snapshot():
            table_name, view_name = self._names(table)

            if table.database not in databases:
                databases.add(table.database)
            if table.catalog not in catalogs:
                catalogs.add(table.catalog)
            yield Grant(
                principal=owner,
                action_type='OWN',
                catalog=table.catalog,
                database=table.database,
                table=table_name,
                view=view_name,
            )
        for database in databases:
            yield Grant(
                principal=owner,
                action_type='OWN',
                catalog="hive_metastore",
                database=database,
                table=None,
                view=None,
            )

        for catalog in catalogs:
            yield Grant(
                principal=owner,
                action_type='OWN',
                catalog=catalog,
                database=None,
                table=None,
                view=None,
            )

    @staticmethod
    def _names(table: Table) -> tuple[str | None, str | None]:
        if table.view_text:
            return None, table.name
        return table.name, None

    def _maybe_direct_owner(self, record: Table) -> str | None:
        return self._static_owner


class TableOwnershipGrantLoader:
    def __init__(self, tables_crawler: TablesCrawler, table_ownership: Ownership[Table]) -> None:
        self._tables_crawler = tables_crawler
        self._table_ownership = table_ownership

    def load(self) -> Iterable[Grant]:
        for table in self._tables_crawler.snapshot():
            owner = self._table_ownership.owner_of(table)
            table_name, view_name = self._names(table)
            yield Grant(
                principal=owner,
                action_type='OWN',
                catalog=table.catalog,
                database=table.database,
                table=table_name,
                view=view_name,
            )

    @staticmethod
    def _names(table: Table) -> tuple[str | None, str | None]:
        if table.view_text:
            return None, table.name
        return table.name, None


class TableMigrationOwnership(Ownership[TableMigrationStatus]):
    """Determine ownership of table migration records in the inventory.

    This is the owner of the source table, if (and only if) the source table is present in the inventory.
    """

    def __init__(self, tables_crawler: TablesCrawler, table_ownership: TableOwnership) -> None:
        super().__init__(table_ownership._administrator_locator)  # TODO: Fix this
        self._tables_crawler = tables_crawler
        self._table_ownership = table_ownership
        self._indexed_tables: dict[tuple[str, str], Table] | None = None

    def _tables_snapshot_index(self, reindex: bool = False) -> dict[tuple[str, str], Table]:
        index = self._indexed_tables
        if index is None or reindex:
            snapshot = self._tables_crawler.snapshot()
            index = {(table.database, table.name): table for table in snapshot}
            self._indexed_tables = index
        return index

    def _maybe_direct_owner(self, record: TableMigrationStatus) -> str | None:
        index = self._tables_snapshot_index()
        source_table = index.get((record.src_schema, record.src_table), None)
        return self._table_ownership.owner_of(source_table) if source_table is not None else None

import logging

from databricks.labs.ucx.framework.owners import (
    Ownership,
)
from databricks.labs.ucx.hive_metastore import TablesCrawler
from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationStatus
from databricks.labs.ucx.hive_metastore.table_ownership import TableOwnership
from databricks.labs.ucx.hive_metastore.tables import Table

logger = logging.getLogger(__name__)


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

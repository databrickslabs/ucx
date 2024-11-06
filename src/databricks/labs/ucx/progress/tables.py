from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import replace
from typing import ClassVar

from databricks.labs.lsql.backends import SqlBackend
from databricks.labs.ucx.framework.crawlers import CrawlerBase

from databricks.labs.ucx.hive_metastore.tables import Table
from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationIndex, TableMigrationStatus
from databricks.labs.ucx.hive_metastore.ownership import TableOwnership
from databricks.labs.ucx.progress.history import ProgressEncoder
from databricks.labs.ucx.progress.install import Historical


class TableProgressEncoder(ProgressEncoder[Table]):
    """Encoder class:Table to class:History.

    A progress failure for a table means:
    - the table is not migrated yet
    - the associated grants have a failure
    """

    def __init__(
        self,
        sql_backend: SqlBackend,
        ownership: TableOwnership,
        migration_status_refresher: CrawlerBase[TableMigrationStatus],
        run_id: int,
        workspace_id: int,
        catalog: str,
        schema: str = "multiworkspace",
        table: str = "historical",
    ) -> None:
        super().__init__(
            sql_backend,
            ownership,
            Table,
            run_id,
            workspace_id,
            catalog,
            schema,
            table,
        )
        self._migration_status_refresher = migration_status_refresher

    SC: ClassVar = TableMigrationIndex

    @contextmanager
    def _snapshot_context(self) -> Generator[SC, None, None]:
        yield TableMigrationIndex(self._migration_status_refresher.snapshot())

    def _encode_record_as_historical(self, record: Table, snapshot_context: SC) -> Historical:
        """Encode record as historical.

        A table failure means that the table is pending migration. Grants are purposefully left out, because a grant
        might not be mappable to UC, like `READ_METADATA`, thus possibly resulting in false "pending migration" failure
        for tables that are migrated to UC with their relevant grants also being migrated.
        """
        historical = super()._encode_record_as_historical(record, snapshot_context=None)
        failures = []
        if not snapshot_context.is_migrated(record.database, record.name):
            failures.append("Pending migration")
        return replace(historical, failures=historical.failures + failures)

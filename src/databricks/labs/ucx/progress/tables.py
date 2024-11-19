import logging
from collections.abc import Iterable
from dataclasses import replace

from databricks.labs.lsql.backends import SqlBackend

from databricks.labs.ucx.framework.crawlers import CrawlerBase
from databricks.labs.ucx.framework.utils import escape_sql_identifier
from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationStatus, TableMigrationIndex
from databricks.labs.ucx.hive_metastore.tables import Table
from databricks.labs.ucx.hive_metastore.ownership import TableOwnership
from databricks.labs.ucx.progress.history import ProgressEncoder
from databricks.labs.ucx.progress.install import Historical
from databricks.labs.ucx.source_code.used_table import UsedTablesCrawler


logger = logging.getLogger(__name__)


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
        used_tables_crawler_for_paths: UsedTablesCrawler,
        used_tables_crawler_for_queries: UsedTablesCrawler,
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
        self._used_tables_for_paths = used_tables_crawler_for_paths
        self._used_tables_for_queries = used_tables_crawler_for_queries

    def append_inventory_snapshot(self, snapshot: Iterable[Table]) -> None:
        migration_index = TableMigrationIndex(self._migration_status_refresher.snapshot())
        history_records = [self._encode_table_as_historical(record, migration_index) for record in snapshot]
        logger.debug(f"Appending {len(history_records)} {self._klass} table record(s) to history.")
        # The mode is 'append'. This is documented as conflict-free.
        self._sql_backend.save_table(escape_sql_identifier(self.full_name), history_records, Historical, mode="append")

    def _encode_table_as_historical(self, record: Table, migration_index: TableMigrationIndex) -> Historical:
        """Encode a table record, enriching with the migration status.

        A table failure means that the table is pending migration. Grants are purposefully left out, because a grant
        might not be mappable to UC, like `READ_METADATA`, thus possibly resulting in false "pending migration" failure
        for tables that are migrated to UC with their relevant grants also being migrated.
        """
        historical = super()._encode_record_as_historical(record)
        failures = []
        if not migration_index.is_migrated(record.database, record.name):
            failures.append("Pending migration")
        return replace(historical, failures=historical.failures + failures)

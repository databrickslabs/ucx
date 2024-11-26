import logging
from collections import defaultdict
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
from databricks.labs.ucx.source_code.base import UsedTable
from databricks.labs.ucx.source_code.used_table import UsedTablesCrawler


logger = logging.getLogger(__name__)


class TableProgressEncoder(ProgressEncoder[Table]):
    """Encoder class:Table to class:History."""

    def __init__(
        self,
        sql_backend: SqlBackend,
        ownership: TableOwnership,
        migration_status_refresher: CrawlerBase[TableMigrationStatus],
        used_tables_crawlers: list[UsedTablesCrawler],
        run_id: int,
        workspace_id: int,
        catalog: str,
    ) -> None:
        super().__init__(
            sql_backend,
            ownership,
            Table,
            run_id,
            workspace_id,
            catalog,
            "multiworkspace",
            "historical",
        )
        self._migration_status_refresher = migration_status_refresher
        self._used_tables_crawlers = used_tables_crawlers

    def append_inventory_snapshot(self, snapshot: Iterable[Table]) -> None:
        migration_index = TableMigrationIndex(self._migration_status_refresher.snapshot())
        used_hive_tables = self._get_used_hive_tables()
        history_records = []
        for record in snapshot:
            history_record = self._encode_table_as_historical(record, migration_index, used_hive_tables)
            history_records.append(history_record)
        logger.debug(f"Appending {len(history_records)} {self._klass} table record(s) to history.")
        # The mode is 'append'. This is documented as conflict-free.
        self._sql_backend.save_table(escape_sql_identifier(self.full_name), history_records, Historical, mode="append")

    def _get_used_hive_tables(self) -> dict[str, list[UsedTable]]:
        used_tables: dict[str, list[UsedTable]] = defaultdict(list[UsedTable])
        for crawler in self._used_tables_crawlers:
            for used_table in crawler.snapshot():
                if used_table.catalog_name == "hive_metastore":
                    used_tables[used_table.full_name].append(used_table)
        return used_tables

    def _encode_table_as_historical(
        self, record: Table, migration_index: TableMigrationIndex, used_hive_tables: dict[str, list[UsedTable]]
    ) -> Historical:
        """Encode a table record, enriching with the migration status and used table references.

        Possible failures, the table is
        - Pending migration
        - A Hive table referenced by code

        Grants are purposefully left out, because a grant might not be mappable to UC, like `READ_METADATA`, thus
        possibly resulting in false "pending migration" failure for tables that are migrated to UC with their relevant
        grants also being migrated.
        """
        historical = super()._encode_record_as_historical(record)
        failures = []
        if not migration_index.is_migrated(record.database, record.name):
            failures.append("Pending migration")
        for used_table in used_hive_tables.get(record.full_name, []):
            failures.append(f"Used by {used_table.source_type}: {used_table.source_id}")
        return replace(historical, failures=historical.failures + failures)

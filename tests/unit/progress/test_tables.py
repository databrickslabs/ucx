import datetime as dt
from unittest.mock import create_autospec

from databricks.labs.ucx.framework.owners import Ownership
from databricks.labs.ucx.framework.utils import escape_sql_identifier
from databricks.labs.ucx.hive_metastore.table_migration_status import (
    TableMigrationStatusRefresher,
    TableMigrationStatus,
)
from databricks.labs.ucx.hive_metastore.tables import Table
from databricks.labs.ucx.progress.tables import TableProgressEncoder
from databricks.labs.ucx.source_code.base import LineageAtom, UsedTable
from databricks.labs.ucx.source_code.used_table import UsedTablesCrawler


def test_table_progress_encoder_no_failures(mock_backend) -> None:
    table = Table("hive_metastore", "schema", "table", "MANAGED", "DELTA")
    ownership = create_autospec(Ownership)
    ownership.owner_of.return_value = "user"
    migration_status_crawler = create_autospec(TableMigrationStatusRefresher)
    migration_status_crawler.snapshot.return_value = (
        TableMigrationStatus(table.database, table.name, "main", "default", table.name, update_ts=None),
    )
    used_tables_crawler = create_autospec(UsedTablesCrawler)
    used_tables_crawler.snapshot.return_value = []
    encoder = TableProgressEncoder(
        mock_backend,
        ownership,
        migration_status_crawler,
        [used_tables_crawler],
        run_id=1,
        workspace_id=123456789,
        catalog="test",
    )

    encoder.append_inventory_snapshot([table])

    rows = mock_backend.rows_written_for(escape_sql_identifier(encoder.full_name), "append")
    assert rows, f"No rows written for: {encoder.full_name}"
    assert len(rows[0].failures) == 0
    ownership.owner_of.assert_called_once()
    migration_status_crawler.snapshot.assert_called_once()
    used_tables_crawler.snapshot.assert_called_once()


def test_table_progress_encoder_pending_migration_failure(mock_backend) -> None:
    table = Table("hive_metastore", "schema", "table", "MANAGED", "DELTA")
    ownership = create_autospec(Ownership)
    ownership.owner_of.return_value = "user"
    migration_status_crawler = create_autospec(TableMigrationStatusRefresher)
    migration_status_crawler.snapshot.return_value = (
        TableMigrationStatus(table.database, table.name),  # No destination: therefore not yet migrated.
    )
    used_tables_crawler_for_paths = create_autospec(UsedTablesCrawler)
    used_table = UsedTable(
        catalog_name=table.catalog,
        schema_name=table.database,
        table_name=table.name,
        source_id="test/test.py",
        source_timestamp=dt.datetime.now(tz=dt.timezone.utc),
        source_lineage=[LineageAtom(object_type="NOTEBOOK", object_id="test/test.py")],
        assessment_start_timestamp=dt.datetime.now(tz=dt.timezone.utc),
        assessment_end_timestamp=dt.datetime.now(tz=dt.timezone.utc),
    )
    used_tables_crawler_for_paths.snapshot.return_value = [used_table]
    encoder = TableProgressEncoder(
        mock_backend,
        ownership,
        migration_status_crawler,
        [used_tables_crawler_for_paths],
        run_id=1,
        workspace_id=123456789,
        catalog="test",
    )

    encoder.append_inventory_snapshot([table])

    rows = mock_backend.rows_written_for(escape_sql_identifier(encoder.full_name), "append")
    assert len(rows) > 0, f"No rows written for: {encoder.full_name}"
    assert rows[0].failures == ["Pending migration", "Used by NOTEBOOK: test/test.py"]
    ownership.owner_of.assert_called_once()
    migration_status_crawler.snapshot.assert_called_once()
    used_tables_crawler_for_paths.snapshot.assert_called_once()

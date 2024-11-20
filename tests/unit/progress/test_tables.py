from unittest.mock import create_autospec

import pytest

from databricks.labs.ucx.framework.owners import Ownership
from databricks.labs.ucx.framework.utils import escape_sql_identifier
from databricks.labs.ucx.hive_metastore.table_migration_status import (
    TableMigrationStatusRefresher,
    TableMigrationStatus,
)
from databricks.labs.ucx.hive_metastore.tables import Table
from databricks.labs.ucx.progress.tables import TableProgressEncoder


@pytest.mark.parametrize(
    "table",
    [
        Table("hive_metastore", "schema", "table", "MANAGED", "DELTA"),
    ],
)
def test_table_progress_encoder_no_failures(mock_backend, table: Table) -> None:
    ownership = create_autospec(Ownership)
    ownership.owner_of.return_value = "user"
    migration_status_crawler = create_autospec(TableMigrationStatusRefresher)
    migration_status_crawler.snapshot.return_value = (
        TableMigrationStatus(table.database, table.name, "main", "default", table.name, update_ts=None),
    )
    encoder = TableProgressEncoder(
        mock_backend, ownership, migration_status_crawler, run_id=1, workspace_id=123456789, catalog="test"
    )

    encoder.append_inventory_snapshot([table])

    rows = mock_backend.rows_written_for(escape_sql_identifier(encoder.full_name), "append")
    assert rows, f"No rows written for: {encoder.full_name}"
    assert len(rows[0].failures) == 0
    ownership.owner_of.assert_called_once()
    migration_status_crawler.snapshot.assert_called_once()


@pytest.mark.parametrize(
    "table",
    [
        Table("hive_metastore", "schema", "table", "MANAGED", "DELTA"),
    ],
)
def test_table_progress_encoder_pending_migration_failure(mock_backend, table: Table) -> None:
    ownership = create_autospec(Ownership)
    ownership.owner_of.return_value = "user"
    migration_status_crawler = create_autospec(TableMigrationStatusRefresher)
    migration_status_crawler.snapshot.return_value = (
        TableMigrationStatus(table.database, table.name),  # No destination: therefore not yet migrated.
    )
    encoder = TableProgressEncoder(
        mock_backend, ownership, migration_status_crawler, run_id=1, workspace_id=123456789, catalog="test"
    )

    encoder.append_inventory_snapshot([table])

    rows = mock_backend.rows_written_for(escape_sql_identifier(encoder.full_name), "append")
    assert len(rows) > 0, f"No rows written for: {encoder.full_name}"
    assert rows[0].failures == ["Pending migration"]
    ownership.owner_of.assert_called_once()
    migration_status_crawler.snapshot.assert_called_once()

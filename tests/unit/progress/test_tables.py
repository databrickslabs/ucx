import datetime as dt
from unittest.mock import create_autospec

import pytest

from databricks.labs.ucx.framework.owners import Ownership
from databricks.labs.ucx.framework.utils import escape_sql_identifier
from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationIndex
from databricks.labs.ucx.hive_metastore.tables import Table
from databricks.labs.ucx.progress.grants import GrantProgressEncoder
from databricks.labs.ucx.progress.tables import TableProgressEncoder, UsedTableProgressEncoder
from databricks.labs.ucx.source_code.used_table import UsedTable


def test_table_progress_encoder_no_failures(mock_backend) -> None:
    table = Table("hive_metastore", "schema", "table", "MANAGED", "DELTA")
    ownership = create_autospec(Ownership)
    ownership.owner_of.return_value = "user"
    table_migration_index = create_autospec(TableMigrationIndex)
    table_migration_index.is_migrated.return_value = True
    grant_progress_encoder = create_autospec(GrantProgressEncoder)
    encoder = TableProgressEncoder(
        mock_backend, ownership, table_migration_index, run_id=1, workspace_id=123456789, catalog="test"
    )

    encoder.append_inventory_snapshot([table])

    rows = mock_backend.rows_written_for(escape_sql_identifier(encoder.full_name), "append")
    assert len(rows) > 0, f"No rows written for: {encoder.full_name}"
    assert len(rows[0].failures) == 0
    ownership.owner_of.assert_called_once()
    table_migration_index.is_migrated.assert_called_with(table.database, table.name)
    grant_progress_encoder.assert_not_called()


def test_table_progress_encoder_pending_migration_failure(mock_backend) -> None:
    table = Table("hive_metastore", "schema", "table", "MANAGED", "DELTA")
    ownership = create_autospec(Ownership)
    ownership.owner_of.return_value = "user"
    table_migration_index = create_autospec(TableMigrationIndex)
    table_migration_index.is_migrated.return_value = False
    grant_progress_encoder = create_autospec(GrantProgressEncoder)
    encoder = TableProgressEncoder(
        mock_backend, ownership, table_migration_index, run_id=1, workspace_id=123456789, catalog="test"
    )

    encoder.append_inventory_snapshot([table])

    rows = mock_backend.rows_written_for(escape_sql_identifier(encoder.full_name), "append")
    assert len(rows) > 0, f"No rows written for: {encoder.full_name}"
    assert rows[0].failures == ["Pending migration"]
    ownership.owner_of.assert_called_once()
    table_migration_index.is_migrated.assert_called_with(table.database, table.name)
    grant_progress_encoder.assert_not_called()


def test_used_table_progress_encoder_no_failures(mock_backend) -> None:
    """No failures when the table is not in the Hive metastore."""
    used_table = UsedTable(
        catalog_name="catalog",
        schema_name="schema",
        table_name="table",
        source_timestamp=dt.datetime.now(dt.timezone.utc),
        assessment_start_timestamp=dt.datetime.now(dt.timezone.utc),
        assessment_end_timestamp=dt.datetime.now(dt.timezone.utc),
    )
    ownership = create_autospec(Ownership)
    ownership.owner_of.return_value = "user"
    encoder = UsedTableProgressEncoder(mock_backend, ownership, run_id=1, workspace_id=123456789, catalog="test")

    encoder.append_inventory_snapshot([used_table])

    rows = mock_backend.rows_written_for(escape_sql_identifier(encoder.full_name), "append")
    assert len(rows) > 0, f"No rows written for: {encoder.full_name}"
    assert len(rows[0].failures) == 0
    ownership.owner_of.assert_called_once()


def test_used_table_progress_encoder_pending_migration_failure(mock_backend) -> None:
    """Failures when the table is in the Hive metastore."""
    used_table = UsedTable(
        catalog_name="hive_metastore",
        schema_name="schema",
        table_name="table",
        source_timestamp=dt.datetime.now(dt.timezone.utc),
        assessment_start_timestamp=dt.datetime.now(dt.timezone.utc),
        assessment_end_timestamp=dt.datetime.now(dt.timezone.utc),
    )
    ownership = create_autospec(Ownership)
    ownership.owner_of.return_value = "user"
    encoder = UsedTableProgressEncoder(mock_backend, ownership, run_id=1, workspace_id=123456789, catalog="test")

    encoder.append_inventory_snapshot([used_table])

    rows = mock_backend.rows_written_for(escape_sql_identifier(encoder.full_name), "append")
    assert len(rows) > 0, f"No rows written for: {encoder.full_name}"
    assert rows[0].failures == ["Pending migration"]
    ownership.owner_of.assert_called_once()

import pytest
from databricks.labs.lsql.backends import DataclassInstance, MockBackend

from databricks.labs.ucx.hive_metastore.workflows import (
    TableMigration,
    MigrateExternalTablesCTAS,
    MigrateHiveSerdeTablesInPlace,
    MigrateTablesInMounts,
    ScanTablesInMounts,
)


def test_migrate_external_tables_sync(run_workflow):
    ctx = run_workflow(TableMigration.migrate_external_tables_sync)
    ctx.workspace_client.catalogs.list.assert_called_once()


def test_migrate_dbfs_root_delta_tables(run_workflow):
    ctx = run_workflow(TableMigration.migrate_dbfs_root_delta_tables)
    ctx.workspace_client.catalogs.list.assert_called_once()


def test_migrate_dbfs_root_non_delta_tables(run_workflow):
    ctx = run_workflow(TableMigration.migrate_dbfs_root_non_delta_tables)
    ctx.workspace_client.catalogs.list.assert_called_once()


def test_migrate_tables_views(run_workflow):
    ctx = run_workflow(TableMigration.migrate_views)
    ctx.workspace_client.catalogs.list.assert_called()


def test_migrate_hive_serde_in_place(run_workflow):
    ctx = run_workflow(MigrateHiveSerdeTablesInPlace.migrate_hive_serde_in_place)
    ctx.workspace_client.catalogs.list.assert_called_once()


def test_migrate_hive_serde_views(run_workflow):
    ctx = run_workflow(MigrateHiveSerdeTablesInPlace.migrate_views)
    ctx.workspace_client.catalogs.list.assert_called()


def test_migrate_other_external_ctas(run_workflow):
    ctx = run_workflow(MigrateExternalTablesCTAS.migrate_other_external_ctas)
    ctx.workspace_client.catalogs.list.assert_called_once()


def test_migrate_hive_serde_ctas(run_workflow):
    ctx = run_workflow(MigrateExternalTablesCTAS.migrate_hive_serde_ctas)
    ctx.workspace_client.catalogs.list.assert_called_once()


def test_migrate_ctas_views(run_workflow):
    ctx = run_workflow(MigrateExternalTablesCTAS.migrate_views)
    ctx.workspace_client.catalogs.list.assert_called()


class MockBackendFriend(MockBackend):
    """A wrapper class to change the return type on :meth:`rows_written_for`."""

    def rows_written_for(self, full_name: str, mode: str) -> list[DataclassInstance] | None:
        """Retrieve the rows written for a table name given a mode.

        Additionally to the logic of the parent class, differentiate between no rows (empty list) and no match (None).
        """
        rows: list[DataclassInstance] = []
        found_write_match = False
        for stub_full_name, stub_rows, stub_mode in self._save_table:
            if not (stub_full_name == full_name and stub_mode == mode):
                continue
            found_write_match = True
            rows += stub_rows
        if len(rows) == 0 and not found_write_match:
            return None
        return rows


@pytest.mark.parametrize(
    "workflow",
    [
        TableMigration,
        MigrateHiveSerdeTablesInPlace,
        MigrateExternalTablesCTAS,
        ScanTablesInMounts,
        MigrateTablesInMounts,
    ],
)
def test_update_migration_status(run_workflow, workflow):
    """Migration status is refreshed by deleting and showing new tables"""
    ctx = run_workflow(getattr(workflow, "update_migration_status"), replace={"sql_backend": MockBackendFriend()})
    assert ctx.sql_backend.rows_written_for("hive_metastore.ucx.migration_status", "overwrite") is not None
    assert "SHOW DATABASES" in ctx.sql_backend.queries

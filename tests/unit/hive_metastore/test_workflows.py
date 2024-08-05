import pytest
import logging
from unittest.mock import create_autospec
from databricks.labs.ucx.hive_metastore.workflows import (
    TableMigration,
    MigrateExternalTablesCTAS,
    MigrateHiveSerdeTablesInPlace,
    MigrateTablesInMounts,
    ScanTablesInMounts,
)

from databricks.labs.ucx.hive_metastore.tables import (
    Table,
    TablesCrawler,
)

from databricks.labs.ucx.hive_metastore.migration_status import (
    MigrationStatusRefresher,
    MigrationIndex,
    MigrationStatus,
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
def test_refresh_migration_status_is_refreshed(run_workflow, workflow):
    """Migration status is refreshed by deleting and showing new tables"""
    ctx = run_workflow(getattr(workflow, "refresh_migration_status"))
    assert "DELETE FROM hive_metastore.ucx.migration_status" in ctx.sql_backend.queries
    assert "SHOW DATABASES" in ctx.sql_backend.queries
    # No "SHOW TABLE FROM" query as table are not mocked

def test_refresh_migration_status_published_remained_tables(run_workflow):
    # class LogCaptureHandler(logging.Handler):
    #     def __init__(self):
    #         super().__init__()
    #         self.records = []
    #
    #     def emit(self, record):
    #         self.records.append(record)


    # # Setup custom log handler
    # log_capture_handler = LogCaptureHandler()
    # logger = logging.getLogger(__name__)
    # logger.addHandler(log_capture_handler)
    # logger.setLevel(logging.INFO)

    # Setup mocks
    custom_table_crawler = create_autospec(TablesCrawler)
    custom_table_crawler.snapshot.return_value = [
        Table(
            object_type="EXTERNAL",
            table_format="DELTA",
            catalog="hive_metastore",
            database="schema1",
            name="table1",
            location="s3://some_location/table1",
            upgraded_to="ucx_default.db1_dst.dst_table1",
        ),
        Table(
            object_type="EXTERNAL",
            table_format="DELTA",
            catalog="hive_metastore",
            database="schema1",
            name="table2",
            location="s3://some_location/table2",
            upgraded_to="ucx_default.db1_dst.dst_table2",
        ),
        Table(
            object_type="EXTERNAL",
            table_format="DELTA",
            catalog="hive_metastore",
            database="schema1",
            name="table3",
            location="s3://some_location/table3",
        ),
    ]
    custom_migration_status_refresher = create_autospec(MigrationStatusRefresher)
    migration_index = MigrationIndex(
        [
            MigrationStatus("schema1", "table1", "ucx_default", "db1_dst", "dst_table1"),
            MigrationStatus("schema1", "table2", "ucx_default", "db1_dst", "dst_table2"),
        ]
    )
    custom_migration_status_refresher.index.return_value = migration_index

    # Call the method
    ctx = run_workflow(
        TableMigration.refresh_migration_status,
        table_crawler=custom_table_crawler,
        migration_status_refresher=custom_migration_status_refresher,
    )

    # # Assert the log message
    # log_messages = [record.getMessage() for record in log_capture_handler.records]
    # assert 'remained-table-to-migrate: schema1.table3' in log_messages
    return ctx.task_run_warning_recorder.snapshot()

    # # Remove the custom log handler
    # logger.removeHandler(log_capture_handler)

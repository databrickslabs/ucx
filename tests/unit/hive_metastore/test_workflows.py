import datetime as dt
from collections.abc import Sequence

import pytest

from databricks.sdk.service.catalog import CatalogInfo, MetastoreAssignment
from databricks.sdk.service.jobs import BaseRun, RunResultState, RunState

from databricks.labs.ucx.framework.tasks import Workflow
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


_migration_workflows: Sequence[type[Workflow]] = (
    TableMigration,
    MigrateHiveSerdeTablesInPlace,
    MigrateExternalTablesCTAS,
    ScanTablesInMounts,
    MigrateTablesInMounts,
)


@pytest.mark.parametrize(
    "task",
    [getattr(workflow, "verify_prerequisites") for workflow in _migration_workflows],
    ids=[workflow.__name__ for workflow in _migration_workflows],
)
def test_with_valid_prerequisites(ws, run_workflow, task) -> None:
    ws.metastores.current.return_value = MetastoreAssignment(metastore_id="test", workspace_id=123456789)
    ws.catalogs.get.return_value = CatalogInfo()
    ws.jobs.list_runs.return_value = [BaseRun(state=RunState(result_state=RunResultState.SUCCESS))]
    run_workflow(task, workspace_client=ws)
    # run_workflow will raise RuntimeError if the prerequisites could not be verified.


@pytest.mark.parametrize(
    "task",
    [getattr(workflow, "verify_prerequisites") for workflow in _migration_workflows],
    ids=[workflow.__name__ for workflow in _migration_workflows],
)
def test_with_invalid_prerequisites(ws, run_workflow, task) -> None:
    """All invalid prerequisites permutations are tested for `VerifyProgressTracking` separately."""
    ws.metastores.current.return_value = None
    with pytest.raises(RuntimeWarning, match="Metastore not attached to workspace."):
        run_workflow(task, workspace_client=ws)


@pytest.mark.parametrize(
    "workflow",
    # Special case here for ScanTablesInMounts, handled below.
    [workflow for workflow in _migration_workflows if workflow is not ScanTablesInMounts],
)
def test_update_migration_status(run_workflow, workflow) -> None:
    """Migration status is refreshed by updating the table inventory, migration status and history log."""
    task1 = getattr(workflow, "update_table_inventory")
    task2 = getattr(workflow, "update_migration_status")
    task3 = getattr(workflow, "update_tables_history_log")
    assert task1.__name__ in task2.__task__.depends_on and task2.__name__ in task3.__task__.depends_on

    # Refresh tables inventory.
    ctx1 = run_workflow(task1)
    assert "SHOW DATABASES" in ctx1.sql_backend.queries
    assert ctx1.sql_backend.has_rows_written_for("hive_metastore.ucx.tables")

    # Given the tables inventory, refresh the migration status of the tables.
    ctx2 = run_workflow(task2, named_parameters={"parent_run_id": "53"})
    assert "SELECT * FROM `hive_metastore`.`ucx`.`tables`" in ctx2.sql_backend.queries
    assert ctx2.sql_backend.has_rows_written_for("hive_metastore.ucx.migration_status")

    # Given the tables inventory and migration status snapshot, update the historical log.
    ctx3 = run_workflow(task3, named_parameters={"parent_run_id": "53"})
    assert "SELECT * FROM `hive_metastore`.`ucx`.`tables`" in ctx3.sql_backend.queries
    assert "SELECT * FROM `hive_metastore`.`ucx`.`migration_status`" in ctx3.sql_backend.queries
    assert ctx3.sql_backend.has_rows_written_for("`ucx`.`multiworkspace`.`historical`")


def test_scan_tables_in_mounts_update_migration_status(run_workflow) -> None:
    """Migration status is refreshed by updating the migration status and history log."""
    task1 = ScanTablesInMounts.update_migration_status
    task2 = ScanTablesInMounts.update_tables_history_log
    assert task1.__name__ in getattr(task2, "__task__").depends_on

    # Given the tables inventory, refresh the migration status of the tables.
    ctx1 = run_workflow(task1, named_parameters={"parent_run_id": "53"})
    assert "SELECT * FROM `hive_metastore`.`ucx`.`tables`" in ctx1.sql_backend.queries
    assert ctx1.sql_backend.has_rows_written_for("hive_metastore.ucx.migration_status")

    # Given the tables inventory and migration status snapshot, update the historical log.
    ctx2 = run_workflow(task2, named_parameters={"parent_run_id": "53"})
    assert "SELECT * FROM `hive_metastore`.`ucx`.`tables`" in ctx2.sql_backend.queries
    assert "SELECT * FROM `hive_metastore`.`ucx`.`migration_status`" in ctx2.sql_backend.queries
    assert ctx2.sql_backend.has_rows_written_for("`ucx`.`multiworkspace`.`historical`")


@pytest.mark.parametrize(
    "task",
    [getattr(workflow, "record_workflow_run") for workflow in _migration_workflows],
    ids=[workflow.__name__ for workflow in _migration_workflows],
)
def test_migration_record_workflow_run(run_workflow, task) -> None:
    """Verify that we log the workflow run."""
    start_time = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    context_replacements = {
        "named_parameters": {
            "workflow": "the_workflow",
            "job_id": "12345",
            "parent_run_id": "456",
            "attempt": "1",
            "start_time": start_time.isoformat(),
        },
    }

    ctx = run_workflow(task, **context_replacements)

    rows = ctx.sql_backend.rows_written_for("ucx.multiworkspace.workflow_runs", "append")

    rows_as_dict = [{k: v for k, v in rows.asDict().items() if k != 'finished_at'} for rows in rows]
    assert rows_as_dict == [
        {
            "started_at": start_time,
            # finished_at: checked below.
            "workspace_id": 123,
            "workflow_name": "the_workflow",
            "workflow_id": 12345,
            "workflow_run_id": 456,
            "workflow_run_attempt": 1,
        }
    ]
    # Finish-time must be indistinguishable from or later than the start time.
    assert all(row["started_at"] <= row["finished_at"] for row in rows)

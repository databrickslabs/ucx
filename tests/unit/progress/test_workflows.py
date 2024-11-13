import datetime as dt
from typing import get_type_hints
from unittest.mock import create_autospec

import pytest
from databricks.labs.ucx.hive_metastore import TablesCrawler
from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationStatusRefresher
from databricks.labs.ucx.progress.history import ProgressEncoder
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import CatalogInfo, MetastoreAssignment
from databricks.sdk.service.jobs import BaseRun, RunResultState, RunState

from databricks.labs.ucx.progress.workflows import MigrationProgress
from databricks.labs.ucx.contexts.workflow_task import RuntimeContext


@pytest.mark.parametrize(
    "task, crawler, history_log",
    (
        (MigrationProgress.crawl_udfs, RuntimeContext.udfs_crawler, RuntimeContext.udfs_progress),
        (MigrationProgress.crawl_grants, RuntimeContext.grants_crawler, RuntimeContext.grants_progress),
        (MigrationProgress.assess_jobs, RuntimeContext.jobs_crawler, RuntimeContext.jobs_progress),
        (MigrationProgress.assess_clusters, RuntimeContext.clusters_crawler, RuntimeContext.clusters_progress),
        (MigrationProgress.assess_pipelines, RuntimeContext.pipelines_crawler, RuntimeContext.pipelines_progress),
        (
            MigrationProgress.crawl_cluster_policies,
            RuntimeContext.policies_crawler,
            RuntimeContext.policies_progress,
        ),
    ),
)
def test_migration_progress_runtime_refresh(run_workflow, task, crawler, history_log) -> None:
    crawler_class = get_type_hints(crawler.func)["return"]
    mock_crawler = create_autospec(crawler_class)
    mock_history_log = create_autospec(ProgressEncoder)
    crawler_name = crawler.attrname
    history_log_name = history_log.attrname
    context_replacements = {
        crawler_name: mock_crawler,
        history_log_name: mock_history_log,
        "named_parameters": {"parent_run_id": 53},
    }
    run_workflow(task, **context_replacements)
    mock_crawler.snapshot.assert_called_once_with(force_refresh=True)
    mock_history_log.append_inventory_snapshot.assert_called_once()


def test_migration_progress_runtime_tables_refresh(run_workflow) -> None:
    """Ensure that the split crawl and update-history-log tasks perform their part of the refresh process."""
    mock_tables_crawler = create_autospec(TablesCrawler)
    mock_migration_status_refresher = create_autospec(TableMigrationStatusRefresher)
    mock_history_log = create_autospec(ProgressEncoder)
    context_replacements = {
        "tables_crawler": mock_tables_crawler,
        "migration_status_refresher": mock_migration_status_refresher,
        "tables_progress": mock_history_log,
        "named_parameters": {"parent_run_id": 53},
    }

    # The first part of a 3-step update: the table crawl without updating the history log.
    run_workflow(MigrationProgress.crawl_tables, **context_replacements)
    mock_tables_crawler.snapshot.assert_called_once_with(force_refresh=True)
    mock_tables_crawler.snapshot.reset_mock()
    mock_history_log.append_inventory_snapshot.assert_not_called()

    # The second part of a 3-step update: updating table migration status without updating the history log.
    run_workflow(MigrationProgress.refresh_table_migration_status, **context_replacements)
    mock_migration_status_refresher.snapshot.assert_called_once_with(force_refresh=True)
    mock_migration_status_refresher.snapshot.reset_mock()
    mock_history_log.append_inventory_snapshot.assert_not_called()

    # The final part of the 3-step update: updating the history log (without a forced crawl).
    run_workflow(MigrationProgress.update_tables_history_log, **context_replacements)
    mock_tables_crawler.snapshot.assert_called_once_with()
    # migration_status_refresher is not directly used within step 3, so interactions don't need to be checked.
    mock_history_log.append_inventory_snapshot.assert_called_once()


@pytest.mark.parametrize(
    "task, linter",
    (
        (MigrationProgress.assess_dashboards, RuntimeContext.query_linter),
        (MigrationProgress.assess_workflows, RuntimeContext.workflow_linter),
    ),
)
def test_linter_runtime_refresh(run_workflow, task, linter) -> None:
    linter_class = get_type_hints(linter.func)["return"]
    mock_linter = create_autospec(linter_class)
    linter_name = linter.attrname
    ctx = run_workflow(task, **{linter_name: mock_linter})
    mock_linter.refresh_report.assert_called_once_with(ctx.sql_backend, ctx.inventory_database)


def test_migration_progress_with_valid_prerequisites(run_workflow) -> None:
    ws = create_autospec(WorkspaceClient)
    ws.metastores.current.return_value = MetastoreAssignment(metastore_id="test", workspace_id=123456789)
    ws.catalogs.get.return_value = CatalogInfo()
    ws.jobs.list_runs.return_value = [BaseRun(state=RunState(result_state=RunResultState.SUCCESS))]
    task = MigrationProgress.verify_prerequisites
    try:
        run_workflow(task, workspace_client=ws)
    except RuntimeError as e:
        assert False, f"{task} raise error: {e}"
    else:
        assert True, "Valid prerequisites found"


def test_migration_progress_with_invalid_prerequisites(run_workflow) -> None:
    """All invalid prerequisites permutations are tested for `VerifyProgressTracking` separately."""
    ws = create_autospec(WorkspaceClient)
    ws.metastores.current.return_value = None
    task = MigrationProgress.verify_prerequisites
    with pytest.raises(RuntimeWarning, match="Metastore not attached to workspace."):
        run_workflow(task, workspace_client=ws)


def test_migration_progress_record_workflow_run(run_workflow, mock_backend) -> None:
    """Verify that we log the workflow run."""
    task = MigrationProgress.record_workflow_run
    start_time = dt.datetime.now(dt.timezone.utc).replace(microsecond=0)
    context_replacements = {
        "sql_backend": mock_backend,
        "named_parameters": {
            "workflow": "test",
            "job_id": "123456",
            "parent_run_id": "456",
            "attempt": "0",
            "start_time": start_time.isoformat(),
        },
    }

    run_workflow(task, **context_replacements)

    rows = mock_backend.rows_written_for("ucx.multiworkspace.workflow_runs", "append")

    rows_as_dict = [{k: v for k, v in rows.asDict().items() if k != 'finished_at'} for rows in rows]
    assert rows_as_dict == [
        {
            "started_at": start_time,
            # finished_at: checked below.
            "workspace_id": 123,
            "workflow_name": "test",
            "workflow_id": 123456,
            "workflow_run_id": 456,
            "workflow_run_attempt": 0,
        }
    ]
    # Finish-time must be indistinguishable from or later than the start time.
    assert all(row["started_at"] <= row["finished_at"] for row in rows)

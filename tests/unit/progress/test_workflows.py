import datetime as dt
from unittest.mock import create_autospec

import pytest
from databricks.labs.ucx.progress.history import HistoryLog
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import CatalogInfo, MetastoreAssignment
from databricks.sdk.service.jobs import BaseRun, RunResultState, RunState

from databricks.labs.ucx.assessment.clusters import ClustersCrawler, PoliciesCrawler
from databricks.labs.ucx.assessment.jobs import JobsCrawler
from databricks.labs.ucx.assessment.pipelines import PipelinesCrawler
from databricks.labs.ucx.progress.workflows import MigrationProgress
from databricks.labs.ucx.contexts.workflow_task import RuntimeContext
from databricks.labs.ucx.hive_metastore import TablesCrawler
from databricks.labs.ucx.hive_metastore.grants import GrantsCrawler
from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationStatusRefresher
from databricks.labs.ucx.hive_metastore.udfs import UdfsCrawler


@pytest.mark.parametrize(
    "task, crawler, crawler_class, history_log",
    [
        (
            MigrationProgress.crawl_tables,
            RuntimeContext.tables_crawler,
            TablesCrawler,
            RuntimeContext.historical_tables_log,
        ),
        (
            MigrationProgress.crawl_udfs,
            RuntimeContext.udfs_crawler,
            UdfsCrawler,
            RuntimeContext.historical_udfs_log,
        ),
        (
            MigrationProgress.crawl_grants,
            RuntimeContext.grants_crawler,
            GrantsCrawler,
            RuntimeContext.historical_grants_log,
        ),
        (
            MigrationProgress.assess_jobs,
            RuntimeContext.jobs_crawler,
            JobsCrawler,
            RuntimeContext.historical_jobs_log,
        ),
        (
            MigrationProgress.assess_clusters,
            RuntimeContext.clusters_crawler,
            ClustersCrawler,
            RuntimeContext.historical_clusters_log,
        ),
        (
            MigrationProgress.assess_pipelines,
            RuntimeContext.pipelines_crawler,
            PipelinesCrawler,
            RuntimeContext.historical_pipelines_log,
        ),
        (
            MigrationProgress.crawl_cluster_policies,
            RuntimeContext.policies_crawler,
            PoliciesCrawler,
            RuntimeContext.historical_cluster_policies_log,
        ),
        (
            MigrationProgress.refresh_table_migration_status,
            RuntimeContext.migration_status_refresher,
            TableMigrationStatusRefresher,
            None,
        ),
    ],
)
@pytest.mark.xfail(raises=AttributeError, reason="Work in progress.")
def test_migration_progress_runtime_refresh(run_workflow, task, crawler, crawler_class, history_log) -> None:
    mock_crawler = create_autospec(crawler_class)
    mock_history_log = create_autospec(HistoryLog)
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

    rows_as_dict = [{k: v for k, v in rows.as_dict().items() if k != 'finished_at'} for rows in rows]
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

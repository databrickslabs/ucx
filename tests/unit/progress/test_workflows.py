from typing import get_type_hints
from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import CatalogInfo, MetastoreAssignment
from databricks.sdk.service.jobs import BaseRun, RunResultState, RunState

from databricks.labs.ucx.progress.workflows import MigrationProgress
from databricks.labs.ucx.contexts.workflow_task import RuntimeContext


@pytest.mark.parametrize(
    "task, crawler",
    (
        (MigrationProgress.crawl_tables, RuntimeContext.tables_crawler),
        (MigrationProgress.crawl_udfs, RuntimeContext.udfs_crawler),
        (MigrationProgress.crawl_grants, RuntimeContext.grants_crawler),
        (MigrationProgress.assess_jobs, RuntimeContext.jobs_crawler),
        (MigrationProgress.assess_clusters, RuntimeContext.clusters_crawler),
        (MigrationProgress.assess_pipelines, RuntimeContext.pipelines_crawler),
        (MigrationProgress.crawl_cluster_policies, RuntimeContext.policies_crawler),
        (MigrationProgress.refresh_table_migration_status, RuntimeContext.migration_status_refresher),
    ),
)
def test_migration_progress_runtime_refresh(run_workflow, task, crawler) -> None:
    crawler_class = get_type_hints(crawler.func)["return"]
    mock_crawler = create_autospec(crawler_class)
    crawler_name = crawler.attrname
    run_workflow(task, **{crawler_name: mock_crawler})
    mock_crawler.snapshot.assert_called_once_with(force_refresh=True)


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

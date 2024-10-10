from unittest.mock import create_autospec

import pytest
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
    "task, crawler, crawler_class",
    [
        (MigrationProgress.crawl_tables, RuntimeContext.tables_crawler, TablesCrawler),
        (MigrationProgress.crawl_udfs, RuntimeContext.udfs_crawler, UdfsCrawler),
        (MigrationProgress.crawl_grants, RuntimeContext.grants_crawler, GrantsCrawler),
        (MigrationProgress.assess_jobs, RuntimeContext.jobs_crawler, JobsCrawler),
        (MigrationProgress.assess_clusters, RuntimeContext.clusters_crawler, ClustersCrawler),
        (MigrationProgress.assess_pipelines, RuntimeContext.pipelines_crawler, PipelinesCrawler),
        (MigrationProgress.crawl_cluster_policies, RuntimeContext.policies_crawler, PoliciesCrawler),
        (
            MigrationProgress.refresh_table_migration_status,
            RuntimeContext.migration_status_refresher,
            TableMigrationStatusRefresher,
        ),
    ],
)
def test_migration_progress_runtime_refresh(run_workflow, task, crawler, crawler_class) -> None:
    mock_crawler = create_autospec(crawler_class)
    crawler_name = crawler.attrname
    run_workflow(task, **{crawler_name: mock_crawler})
    mock_crawler.snapshot.assert_called_once_with(force_refresh=True)


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

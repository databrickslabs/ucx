from unittest.mock import create_autospec

import pytest

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

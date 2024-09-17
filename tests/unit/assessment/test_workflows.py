from unittest.mock import create_autospec

import pytest
from databricks.labs.lsql.backends import SqlBackend

from databricks.labs.ucx.assessment.clusters import ClustersCrawler, PoliciesCrawler
from databricks.labs.ucx.assessment.jobs import JobsCrawler
from databricks.labs.ucx.assessment.pipelines import PipelinesCrawler
from databricks.labs.ucx.assessment.workflows import Assessment, Failing, MigrationProgress
from databricks.labs.ucx.contexts.workflow_task import RuntimeContext
from databricks.labs.ucx.hive_metastore import TablesCrawler
from databricks.labs.ucx.hive_metastore.grants import GrantsCrawler
from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationStatusRefresher
from databricks.labs.ucx.hive_metastore.udfs import UdfsCrawler


def test_assess_azure_service_principals(run_workflow):
    sql_backend = create_autospec(SqlBackend)
    sql_backend.fetch.return_value = [
        ["1", "secret_scope", "secret_key", "tenant_id", "storage_account"],
    ]
    run_workflow(Assessment.assess_azure_service_principals, sql_backend=sql_backend)


def test_runtime_workspace_listing(run_workflow):
    ctx = run_workflow(Assessment.workspace_listing)
    assert "SELECT * FROM `hive_metastore`.`ucx`.`workspace_objects`" in ctx.sql_backend.queries


def test_runtime_crawl_grants(run_workflow):
    ctx = run_workflow(Assessment.crawl_grants)
    assert "SELECT * FROM `hive_metastore`.`ucx`.`grants`" in ctx.sql_backend.queries


def test_runtime_crawl_permissions(run_workflow):
    ctx = run_workflow(Assessment.crawl_permissions)
    assert "TRUNCATE TABLE `hive_metastore`.`ucx`.`permissions`" in ctx.sql_backend.queries


def test_runtime_crawl_groups(run_workflow):
    ctx = run_workflow(Assessment.crawl_groups)
    assert "SELECT * FROM `hive_metastore`.`ucx`.`groups`" in ctx.sql_backend.queries


def test_runtime_crawl_cluster_policies(run_workflow):
    ctx = run_workflow(Assessment.crawl_cluster_policies)
    assert "SELECT * FROM `hive_metastore`.`ucx`.`policies`" in ctx.sql_backend.queries


def test_runtime_crawl_init_scripts(run_workflow):
    ctx = run_workflow(Assessment.assess_global_init_scripts)
    assert "SELECT * FROM `hive_metastore`.`ucx`.`global_init_scripts`" in ctx.sql_backend.queries


def test_estimate_table_size_for_migration(run_workflow):
    ctx = run_workflow(Assessment.estimate_table_size_for_migration)
    assert "SELECT * FROM `hive_metastore`.`ucx`.`table_size`" in ctx.sql_backend.queries
    assert "SHOW DATABASES" in ctx.sql_backend.queries


def test_runtime_mounts(run_workflow):
    ctx = run_workflow(Assessment.crawl_mounts)
    assert "SELECT * FROM `hive_metastore`.`ucx`.`mounts`" in ctx.sql_backend.queries


def test_guess_external_locations(run_workflow):
    ctx = run_workflow(Assessment.guess_external_locations)
    assert "SELECT * FROM `hive_metastore`.`ucx`.`mounts`" in ctx.sql_backend.queries


def test_assess_jobs(run_workflow):
    ctx = run_workflow(Assessment.assess_jobs)
    assert "SELECT * FROM `hive_metastore`.`ucx`.`jobs`" in ctx.sql_backend.queries


def test_assess_clusters(run_workflow):
    ctx = run_workflow(Assessment.assess_clusters)
    assert "SELECT * FROM `hive_metastore`.`ucx`.`clusters`" in ctx.sql_backend.queries


def test_assess_pipelines(run_workflow):
    ctx = run_workflow(Assessment.assess_pipelines)
    assert "SELECT * FROM `hive_metastore`.`ucx`.`pipelines`" in ctx.sql_backend.queries


def test_incompatible_submit_runs(run_workflow):
    ctx = run_workflow(Assessment.assess_incompatible_submit_runs)
    assert "SELECT * FROM `hive_metastore`.`ucx`.`submit_runs`" in ctx.sql_backend.queries


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


def test_failing_task_raises_value_error(run_workflow):
    with pytest.raises(ValueError):
        run_workflow(Failing.failing_task)

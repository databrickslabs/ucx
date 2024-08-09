from unittest.mock import create_autospec

import pytest
from databricks.labs.lsql.backends import SqlBackend

from databricks.labs.ucx.assessment.workflows import Assessment, Failing


def test_assess_azure_service_principals(run_workflow):
    sql_backend = create_autospec(SqlBackend)
    sql_backend.fetch.return_value = [
        ["1", "secret_scope", "secret_key", "tenant_id", "storage_account"],
    ]
    run_workflow(Assessment.assess_azure_service_principals, sql_backend=sql_backend)


def test_runtime_workspace_listing(run_workflow):
    ctx = run_workflow(Assessment.workspace_listing)
    assert "SELECT * FROM ucx.workspace_objects" in ctx.sql_backend.queries


def test_runtime_crawl_grants(run_workflow):
    ctx = run_workflow(Assessment.crawl_grants)
    assert "SELECT * FROM hive_metastore.ucx.grants" in ctx.sql_backend.queries


def test_runtime_crawl_permissions(run_workflow):
    ctx = run_workflow(Assessment.crawl_permissions)
    assert "DELETE FROM hive_metastore.ucx.permissions" in ctx.sql_backend.queries


def test_runtime_crawl_groups(run_workflow):
    ctx = run_workflow(Assessment.crawl_groups)
    assert "SELECT * FROM hive_metastore.ucx.groups" in ctx.sql_backend.queries


def test_runtime_crawl_cluster_policies(run_workflow):
    ctx = run_workflow(Assessment.crawl_cluster_policies)
    assert "SELECT * FROM ucx.policies" in ctx.sql_backend.queries


def test_runtime_crawl_init_scripts(run_workflow):
    ctx = run_workflow(Assessment.assess_global_init_scripts)
    assert "SELECT * FROM ucx.global_init_scripts" in ctx.sql_backend.queries


def test_estimate_table_size_for_migration(run_workflow):
    ctx = run_workflow(Assessment.estimate_table_size_for_migration)
    assert "SELECT * FROM hive_metastore.ucx.table_size" in ctx.sql_backend.queries
    assert "SHOW DATABASES" in ctx.sql_backend.queries


def test_runtime_mounts(run_workflow):
    ctx = run_workflow(Assessment.crawl_mounts)
    assert "SELECT * FROM ucx.mounts" in ctx.sql_backend.queries


def test_guess_external_locations(run_workflow):
    ctx = run_workflow(Assessment.guess_external_locations)
    assert "SELECT * FROM ucx.mounts" in ctx.sql_backend.queries


def test_assess_jobs(run_workflow):
    ctx = run_workflow(Assessment.assess_jobs)
    assert "SELECT * FROM ucx.jobs" in ctx.sql_backend.queries


def test_assess_clusters(run_workflow):
    ctx = run_workflow(Assessment.assess_clusters)
    assert "SELECT * FROM ucx.clusters" in ctx.sql_backend.queries


def test_assess_pipelines(run_workflow):
    ctx = run_workflow(Assessment.assess_pipelines)
    assert "SELECT * FROM ucx.pipelines" in ctx.sql_backend.queries


def test_incompatible_submit_runs(run_workflow):
    ctx = run_workflow(Assessment.assess_incompatible_submit_runs)
    assert "SELECT * FROM ucx.submit_runs" in ctx.sql_backend.queries


def test_failing_task_raises_value_error(run_workflow):
    with pytest.raises(ValueError):
        run_workflow(Failing.failing_task)

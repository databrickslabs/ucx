import pytest

from databricks.labs.ucx.assessment.jobs import JobInfo, JobsCrawler

from ..framework.mocks import MockBackend
from . import workspace_client_mock


def test_job_assessment():
    ws = workspace_client_mock(
        job_ids=['on-simplest-autoscale', 'on-outdated-autoscale'],
        cluster_ids=['simplest-autoscale', 'outdated-autoscale'],
    )
    sql_backend = MockBackend()
    result_set = JobsCrawler(ws, sql_backend, "ucx").snapshot()
    assert len(result_set) == 2
    assert result_set[0].success == 1
    assert result_set[1].success == 0


def test_job_assessment_no_job_tasks():
    ws = workspace_client_mock(job_ids=['no-tasks'])
    sql_backend = MockBackend()
    result_set = JobsCrawler(ws, sql_backend, "ucx").snapshot()
    assert len(result_set) == 1
    assert result_set[0].success == 1


def test_job_assessment_no_job_settings():
    ws = workspace_client_mock(job_ids=['no-settings'])
    sql_backend = MockBackend()
    result_set = JobsCrawler(ws, sql_backend, "ucx").snapshot()
    assert len(result_set) == 0


def test_job_assessment_for_azure_spark_config():
    ws = workspace_client_mock(job_ids=['on-azure-spn-secret'], cluster_ids=['azure-spn-secret'])
    sql_backend = MockBackend()
    result_set = JobsCrawler(ws, sql_backend, "ucx").snapshot()
    assert len(result_set) == 1
    assert result_set[0].success == 0


@pytest.mark.skip
def test_jobs_assessment_with_spn_cluster_no_job_tasks():
    ws = workspace_client_mock(job_ids=['legacy-job-on-azure-spn-secret'], cluster_ids=['azure-spn-secret'])
    sql_backend = MockBackend()
    result_set = JobsCrawler(ws, sql_backend, "ucx").snapshot()
    assert len(result_set) == 1
    assert result_set[0].success == 0


def test_job_crawler_with_no_owner_should_have_empty_creator_name():
    ws = workspace_client_mock(job_ids=['no-tasks'])
    sql_backend = MockBackend()
    JobsCrawler(ws, sql_backend, "ucx").snapshot()
    result = sql_backend.rows_written_for("hive_metastore.ucx.jobs", "append")
    assert result == [JobInfo(job_id="9001", job_name="No Tasks", creator=None, success=1, failures="[]")]

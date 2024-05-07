import pytest
from databricks.labs.lsql import Row
from databricks.labs.lsql.backends import MockBackend

from databricks.labs.ucx.assessment.jobs import JobsCrawler, SubmitRunsCrawler

from .. import workspace_client_mock


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


def test_spark_jar_task_failures():
    ws = workspace_client_mock(job_ids=['spark-jar-task'], cluster_ids=['azure-spn-secret'])
    sql_backend = MockBackend()
    result_set = JobsCrawler(ws, sql_backend, "ucx").snapshot()
    assert len(result_set) == 1
    assert result_set[0].success == 0
    assert "task spark_jar_task is a jar task" in result_set[0].failures


def test_job_assessment_for_azure_spark_config():
    ws = workspace_client_mock(job_ids=['on-azure-spn-secret'], cluster_ids=['azure-spn-secret'])
    sql_backend = MockBackend()
    result_set = JobsCrawler(ws, sql_backend, "ucx").snapshot()
    assert len(result_set) == 1
    assert result_set[0].success == 0


def test_jobs_assessment_with_spn_cluster_no_job_tasks():
    ws = workspace_client_mock(job_ids=['no-tasks'])
    sql_backend = MockBackend()
    result_set = JobsCrawler(ws, sql_backend, "ucx").snapshot()
    assert len(result_set) == 1
    assert result_set[0].success == 1


def test_job_crawler_with_no_owner_should_have_empty_creator_name():
    ws = workspace_client_mock(job_ids=['no-tasks'])
    sql_backend = MockBackend()
    JobsCrawler(ws, sql_backend, "ucx").snapshot()
    result = sql_backend.rows_written_for("hive_metastore.ucx.jobs", "append")
    assert result == [Row(job_id='9001', success=1, failures='[]', job_name='No Tasks', creator=None)]


@pytest.mark.parametrize(
    "jobruns_ids,cluster_ids,run_ids,failures",
    [
        (
            ['notebook_task'],
            ['outdated-autoscale'],
            '[123]',
            '["not supported DBR: 9.3.x-cpu-ml-scala2.12"]',
        ),
        (
            ['notebook_task', 'notebook_dupe_task'],
            ['outdated-autoscale'],
            '[123, 124]',
            '["not supported DBR: 9.3.x-cpu-ml-scala2.12"]',
        ),
        (
            ['sql_tasks'],
            ['outdated-autoscale'],
            '[123]',
            '["not supported DBR: 9.3.x-cpu-ml-scala2.12"]',
        ),
        (['gitsource_task'], ['outdated-autoscale'], '[123]', '["not supported DBR: 9.3.x-cpu-ml-scala2.12"]'),
        (['dbt_task'], ['outdated-autoscale'], '[123]', '["not supported DBR: 9.3.x-cpu-ml-scala2.12"]'),
        (
            ['jar_task'],
            ['outdated-autoscale'],
            '[123]',
            '["not supported DBR: 9.3.x-cpu-ml-scala2.12"]',
        ),
        (['python_wheel_task'], ['outdated-autoscale'], '[123]', '["not supported DBR: 9.3.x-cpu-ml-scala2.12"]'),
        (['run_condition_task'], ['outdated-autoscale'], '[123]', '["not supported DBR: 9.3.x-cpu-ml-scala2.12"]'),
        (['notebook_no_failure_task'], ['simplest-autoscale'], '[123]', '[]'),
        (
            ['notebook_no_sec_no_comp_task'],
            ['simplest-autoscale'],
            '[123]',
            '["not supported DBR: 9.3.x-cpu-ml-scala2.12"]',
        ),
        (['notebook_no_sec_comp_task'], ['simplest-autoscale'], '[123]', '["no data security mode specified"]'),
        (
            ['notebook_spark_conf_task'],
            ['simplest-autoscale'],
            '[123]',
            '["Uses passthrough config: spark.databricks.passthrough.enabled in notebook."]',
        ),
        (['spark_jar_task'], ['simplest-autoscale'], '[123]', '["task jar is a jar task"]'),
    ],
)
def test_job_run_crawler(jobruns_ids, cluster_ids, run_ids, failures):
    ws = workspace_client_mock(jobruns_ids=jobruns_ids, cluster_ids=cluster_ids)
    sql_backend = MockBackend()
    SubmitRunsCrawler(ws, sql_backend, "ucx", 10).snapshot()
    result = sql_backend.rows_written_for("hive_metastore.ucx.submit_runs", "append")
    assert len(result) == 1
    assert result[0].run_ids == run_ids
    assert result[0].failures == failures

from unittest.mock import create_autospec

import pytest
from databricks.labs.lsql.backends import MockBackend
from databricks.labs.lsql.core import Row
from databricks.labs.ucx.progress.history import HistoryLog
from databricks.sdk.service.jobs import BaseJob, JobSettings

from databricks.labs.ucx.__about__ import __version__ as ucx_version
from databricks.labs.ucx.assessment.jobs import JobInfo, JobOwnership, JobsCrawler, SubmitRunsCrawler
from databricks.labs.ucx.framework.owners import AdministratorLocator

from .. import mock_workspace_client


def test_job_assessment():
    ws = mock_workspace_client(
        job_ids=['on-simplest-autoscale', 'on-outdated-autoscale'],
        cluster_ids=['simplest-autoscale', 'outdated-autoscale'],
    )
    sql_backend = MockBackend()
    result_set = JobsCrawler(ws, sql_backend, "ucx").snapshot()
    assert len(result_set) == 2
    assert result_set[0].success == 1
    assert result_set[1].success == 0


def test_job_assessment_no_job_tasks():
    ws = mock_workspace_client(job_ids=['no-tasks'])
    sql_backend = MockBackend()
    result_set = JobsCrawler(ws, sql_backend, "ucx").snapshot()
    assert len(result_set) == 1
    assert result_set[0].success == 1


def test_job_assessment_no_job_settings():
    ws = mock_workspace_client(job_ids=['no-settings'])
    sql_backend = MockBackend()
    result_set = JobsCrawler(ws, sql_backend, "ucx").snapshot()
    assert len(result_set) == 0


def test_spark_jar_task_failures():
    ws = mock_workspace_client(job_ids=['spark-jar-task'], cluster_ids=['azure-spn-secret'])
    sql_backend = MockBackend()
    result_set = JobsCrawler(ws, sql_backend, "ucx").snapshot()
    assert len(result_set) == 1
    assert result_set[0].success == 0
    assert "task spark_jar_task is a jar task" in result_set[0].failures


def test_job_assessment_for_azure_spark_config():
    ws = mock_workspace_client(job_ids=['on-azure-spn-secret'], cluster_ids=['azure-spn-secret'])
    sql_backend = MockBackend()
    result_set = JobsCrawler(ws, sql_backend, "ucx").snapshot()
    assert len(result_set) == 1
    assert result_set[0].success == 0


def test_jobs_assessment_with_spn_cluster_no_job_tasks():
    ws = mock_workspace_client(job_ids=['no-tasks'])
    sql_backend = MockBackend()
    result_set = JobsCrawler(ws, sql_backend, "ucx").snapshot()
    assert len(result_set) == 1
    assert result_set[0].success == 1


def test_pipeline_crawler_creator():
    ws = mock_workspace_client()
    ws.jobs.list.return_value = (
        BaseJob(job_id=1, settings=JobSettings(), creator_user_name=None),
        BaseJob(job_id=2, settings=JobSettings(), creator_user_name=""),
        BaseJob(job_id=3, settings=JobSettings(), creator_user_name="bob"),
    )
    result = JobsCrawler(ws, MockBackend(), "ucx").snapshot(force_refresh=True)

    expected_creators = [None, None, "bob"]
    crawled_creators = [record.creator for record in result]
    assert len(expected_creators) == len(crawled_creators)
    assert set(expected_creators) == set(crawled_creators)


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
    ws = mock_workspace_client(jobruns_ids=jobruns_ids, cluster_ids=cluster_ids)
    sql_backend = MockBackend()
    SubmitRunsCrawler(ws, sql_backend, "ucx", 10).snapshot()
    result = sql_backend.rows_written_for("hive_metastore.ucx.submit_runs", "overwrite")
    assert len(result) == 1
    assert result[0].run_ids == run_ids
    assert result[0].failures == failures


def test_pipeline_owner_creator() -> None:
    admin_locator = create_autospec(AdministratorLocator)

    ownership = JobOwnership(admin_locator)
    owner = ownership.owner_of(JobInfo(creator="bob", job_id="1", success=1, failures="[]"))

    assert owner == "bob"
    admin_locator.get_workspace_administrator.assert_not_called()


def test_pipeline_owner_creator_unknown() -> None:
    admin_locator = create_autospec(AdministratorLocator)
    admin_locator.get_workspace_administrator.return_value = "an_admin"

    ownership = JobOwnership(admin_locator)
    owner = ownership.owner_of(JobInfo(creator=None, job_id="1", success=1, failures="[]"))

    assert owner == "an_admin"
    admin_locator.get_workspace_administrator.assert_called_once()


@pytest.mark.parametrize(
    "job_info_record,history_record",
    (
        (
            JobInfo(
                job_id="1234",
                success=1,
                failures="[]",
            ),
            Row(
                workspace_id=2,
                job_run_id=1,
                object_type="JobInfo",
                object_id=["1234"],
                data={
                    "job_id": "1234",
                    "success": "1",
                },
                failures=[],
                owner="the_admin",
                ucx_version=ucx_version,
            ),
        ),
        (
            JobInfo(
                job_id="1234",
                job_name="the_job",
                creator="user@domain",
                success=0,
                failures='["first-failure", "second-failure"]',
            ),
            Row(
                workspace_id=2,
                job_run_id=1,
                object_type="JobInfo",
                object_id=["1234"],
                data={
                    "job_id": "1234",
                    "job_name": "the_job",
                    "creator": "user@domain",
                    "success": "0",
                },
                failures=["first-failure", "second-failure"],
                owner="user@domain",
                ucx_version=ucx_version,
            ),
        ),
    ),
)
def test_pipeline_supports_history(mock_backend, job_info_record: JobInfo, history_record: Row) -> None:
    """Verify that JobInfo records are written as expected to the history log."""
    admin_locator = create_autospec(AdministratorLocator)
    admin_locator.get_workspace_administrator.return_value = "the_admin"
    job_ownership = JobOwnership(admin_locator)
    history_log = HistoryLog[JobInfo](
        mock_backend,
        job_ownership,
        JobInfo,
        run_id=1,
        workspace_id=2,
        catalog="a_catalog",
    )

    history_log.append_inventory_snapshot([job_info_record])

    rows = mock_backend.rows_written_for("`a_catalog`.`ucx`.`history`", mode="append")

    assert rows == [history_record]

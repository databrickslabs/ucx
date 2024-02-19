from unittest.mock import Mock, create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError, InternalError, NotFound
from databricks.sdk.service.compute import (
    AutoScale,
    ClusterDetails,
    ClusterSource,
    ClusterSpec,
    DbfsStorageInfo,
    InitScriptInfo,
    WorkspaceStorageInfo,
)
from databricks.sdk.service.jobs import BaseJob, JobSettings, NotebookTask, Task

from databricks.labs.ucx.assessment.azure import AzureServicePrincipalCrawler
from databricks.labs.ucx.assessment.jobs import JobInfo, JobsCrawler

from ..framework.mocks import MockBackend
from . import workspace_client_mock


def test_job_assessment():
    ws = workspace_client_mock(job_ids=['on-simplest-autoscale', 'on-outdated-autoscale'], cluster_ids=['simplest-autoscale', 'outdated-autoscale'])
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
    sample_jobs = [
        BaseJob(
            created_time=1694536604319,
            creator_user_name="anonymous@databricks.com",
            job_id=536591785949415,
            settings=None,
        ),
    ]

    sample_clusters = [
        ClusterDetails(
            autoscale=AutoScale(min_workers=1, max_workers=6),
            spark_conf={"spark.databricks.delta.preview.enabled": "true"},
            spark_context_id=5134472582179566666,
            spark_env_vars=None,
            spark_version="13.3.x-cpu-ml-scala2.12",
            cluster_id="0810-229933-chicago99",
            cluster_source=ClusterSource.JOB,
        ),
    ]
    ws = Mock()
    result_set = JobsCrawler(ws, MockBackend(), "ucx")._assess_jobs(
        sample_jobs, {c.cluster_id: c for c in sample_clusters}
    )
    assert len(result_set) == 0


def test_job_assessment_for_azure_spark_config():
    sample_jobs = [
        BaseJob(
            created_time=1694536604319,
            creator_user_name="anonymous@databricks.com",
            job_id=536591785949415,
            settings=JobSettings(
                compute=None,
                continuous=None,
                tasks=[
                    Task(
                        task_key="Ingest",
                        existing_cluster_id="0807-225846-avon493",
                        notebook_task=NotebookTask(
                            notebook_path="/Users/foo.bar@databricks.com/Customers/Example/Test/Load"
                        ),
                        timeout_seconds=0,
                    )
                ],
                timeout_seconds=0,
            ),
        ),
        BaseJob(
            created_time=1694536604321,
            creator_user_name="anonymous@databricks.com",
            job_id=536591785949416,
            settings=JobSettings(
                compute=None,
                continuous=None,
                tasks=[
                    Task(
                        task_key="Ingest",
                        existing_cluster_id="0810-229933-chicago99",
                        notebook_task=NotebookTask(
                            notebook_path="/Users/foo.bar@databricks.com/Customers/Example/Test/Load"
                        ),
                        timeout_seconds=0,
                    )
                ],
                timeout_seconds=0,
            ),
        ),
        BaseJob(
            created_time=1694536604319,
            creator_user_name="anonymous@databricks.com",
            job_id=536591785949417,
            settings=JobSettings(
                compute=None,
                continuous=None,
                tasks=[
                    Task(
                        task_key="Ingest",
                        existing_cluster_id="0811-929933-maine96",
                        notebook_task=NotebookTask(
                            notebook_path="/Users/foo.bar@databricks.com/Customers/Example/Test/Load"
                        ),
                        timeout_seconds=0,
                    )
                ],
                timeout_seconds=0,
            ),
        ),
    ]

    sample_clusters = [
        ClusterDetails(
            autoscale=AutoScale(min_workers=1, max_workers=6),
            spark_conf={
                "spark.hadoop.fs.azure.account.oauth2.client.id.abcde.dfs.core.windows.net": "{{secrets/abcff"
                "/sp_app_client_id}}",
                "spark.hadoop.fs.azure.account.oauth2.client.endpoint.abcde.dfs.core.windows.net": "https://login"
                ".microsoftonline"
                ".com/dedededede"
                "/token",
                "spark.hadoop.fs.azure.account.oauth2.client.secret.abcde.dfs.core.windows.net": "{{secrets/abcff"
                "/sp_secret}}",
            },
            spark_context_id=5134472582179566666,
            spark_env_vars=None,
            spark_version="13.3.x-cpu-ml-scala2.12",
            cluster_id="0807-225846-avon493",
            cluster_source=ClusterSource.JOB,
        ),
        ClusterDetails(
            autoscale=AutoScale(min_workers=1, max_workers=6),
            spark_conf={"spark.databricks.delta.preview.enabled": "true"},
            spark_context_id=5134472582179566666,
            spark_env_vars=None,
            spark_version="13.3.x-cpu-ml-scala2.12",
            cluster_id="0810-229933-chicago99",
            cluster_source=ClusterSource.JOB,
        ),
        ClusterDetails(
            autoscale=AutoScale(min_workers=1, max_workers=6),
            spark_conf={"spark.databricks.delta.preview.enabled": "true"},
            spark_context_id=5134472582179566666,
            spark_env_vars=None,
            spark_version="13.3.x-cpu-ml-scala2.12",
            policy_id="D96308F1BF0003A9",
            cluster_id="0811-929933-maine96",
            cluster_source=ClusterSource.JOB,
        ),
    ]
    ws = Mock()
    ws.cluster_policies.get().definition = (
        '{\n  "spark_conf.fs.azure.account.auth.type": {\n    '
        '"type": "fixed",\n    "value": "OAuth",\n   '
        ' "hidden": true\n  },\n  "spark_conf.fs.azure.account.oauth.provider.type": {\n   '
        ' "type": "fixed",\n    "value": '
        '"org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",\n    '
        '"hidden": true\n  },\n  "spark_conf.fs.azure.account.oauth2.client.id": {\n    '
        '"type": "fixed",\n    "value": "fsfsfsfsffsfsf",\n    "hidden": true\n  },\n  '
        '"spark_conf.fs.azure.account.oauth2.client.secret": {\n    "type": "fixed",\n    '
        '"value": "gfgfgfgfggfggfgfdds",\n    "hidden": true\n  },\n  '
        '"spark_conf.fs.azure.account.oauth2.client.endpoint": {\n    '
        '"type": "fixed",\n    '
        '"value": "https://login.microsoftonline.com/1234ededed/oauth2/token",\n    '
        '"hidden": true\n  }\n}'
    )
    ws.cluster_policies.get().policy_family_definition_overrides = (
        '{\n  "spark_conf.fs.azure.account.auth.type": {\n    '
        '"type": "fixed",\n    "value": "OAuth",\n   '
        ' "hidden": true\n  },\n  "spark_conf.fs.azure.account.oauth.provider.type": {\n   '
        ' "type": "fixed",\n    "value": '
        '"org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",\n    '
        '"hidden": true\n  },\n  "spark_conf.fs.azure.account.oauth2.client.id": {\n    '
        '"type": "fixed",\n    "value": "fsfsfsfsffsfsf",\n    "hidden": true\n  },\n  '
        '"spark_conf.fs.azure.account.oauth2.client.secret": {\n    "type": "fixed",\n    '
        '"value": "gfgfgfgfggfggfgfdds",\n    "hidden": true\n  },\n  '
        '"spark_conf.fs.azure.account.oauth2.client.endpoint": {\n    '
        '"type": "fixed",\n    '
        '"value": "https://login.microsoftonline.com/1234ededed/oauth2/token",\n    '
        '"hidden": true\n  }\n}'
    )
    result_set = JobsCrawler(ws, MockBackend(), "ucx")._assess_jobs(
        sample_jobs, {c.cluster_id: c for c in sample_clusters}
    )
    assert len(result_set) == 3
    assert result_set[0].success == 0
    assert result_set[1].success == 1
    assert result_set[2].success == 0


def test_jobs_assessment_with_spn_cluster_no_job_tasks(mocker):
    sample_jobs = [
        BaseJob(
            created_time=1694536604319,
            creator_user_name="anonymous@databricks.com",
            job_id=536591785949415,
            settings=JobSettings(
                compute=None,
                continuous=None,
                tasks=None,
                timeout_seconds=0,
            ),
        )
    ]

    ws = workspace_client_mock(cluster_ids=['policy-single-user-with-spn'])
    ws.jobs.list.return_value = sample_jobs

    jobs = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()
    assert len(jobs) == 0


def test_jobs_assessment_with_spn_cluster_no_job_settings(mocker):
    sample_jobs = [
        BaseJob(
            created_time=1694536604319,
            creator_user_name="anonymous@databricks.com",
            job_id=536591785949415,
            settings=None,
        )
    ]

    ws = workspace_client_mock(cluster_ids=['policy-single-user-with-spn'])
    ws.jobs.list.return_value = sample_jobs

    jobs = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()
    assert len(jobs) == 0


def test_jobs_assessment_with_spn_cluster_policy_not_found(mocker):
    sample_jobs = [
        BaseJob(
            created_time=1694536604319,
            creator_user_name="anonymous@databricks.com",
            job_id=536591785949415,
            settings=JobSettings(
                compute=None,
                continuous=None,
                tasks=[
                    Task(
                        task_key="Ingest",
                        new_cluster=ClusterSpec(
                            autoscale=None,
                            node_type_id="Standard_DS3_v2",
                            num_workers=2,
                            spark_conf={
                                "spark.hadoop.fs.azure.account.oauth2.client.id.abcde.dfs"
                                ".core.windows.net": "1234567890",
                                "spark.databricks.delta.formatCheck.enabled": "false",
                            },
                            policy_id="bdqwbdqiwd1111",
                        ),
                        notebook_task=NotebookTask(
                            notebook_path="/Users/foo.bar@databricks.com/Customers/Example/Test/Load"
                        ),
                        timeout_seconds=0,
                    )
                ],
                timeout_seconds=0,
            ),
        )
    ]
    ws = mocker.Mock()
    ws.clusters.list.return_value = []
    ws.pipelines.list_pipelines.return_value = []
    ws.jobs.list.return_value = sample_jobs
    ws.warehouses.get_workspace_warehouse_config.return_value.data_access_config = None
    ws.cluster_policies.get.side_effect = NotFound("NO_POLICY")
    crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()
    assert len(crawler) == 1


def test_jobs_assessment_with_spn_cluster_policy_exception(mocker):
    sample_jobs = [
        BaseJob(
            created_time=1694536604319,
            creator_user_name="anonymous@databricks.com",
            job_id=536591785949415,
            settings=JobSettings(
                compute=None,
                continuous=None,
                tasks=[
                    Task(
                        task_key="Ingest",
                        new_cluster=ClusterSpec(
                            autoscale=None,
                            node_type_id="Standard_DS3_v2",
                            num_workers=2,
                            spark_conf={
                                "spark.hadoop.fs.azure.account.oauth2.client.id.abcde.dfs"
                                ".core.windows.net": "1234567890",
                                "spark.databricks.delta.formatCheck.enabled": "false",
                            },
                            policy_id="bdqwbdqiwd1111",
                        ),
                        notebook_task=NotebookTask(
                            notebook_path="/Users/foo.bar@databricks.com/Customers/Example/Test/Load"
                        ),
                        timeout_seconds=0,
                    )
                ],
                timeout_seconds=0,
            ),
        )
    ]

    ws = mocker.Mock()
    ws.clusters.list.return_value = []
    ws.pipelines.list_pipelines.return_value = []
    ws.jobs.list.return_value = sample_jobs
    ws.cluster_policies.get.side_effect = InternalError(...)

    with pytest.raises(DatabricksError):
        AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()


def test_job_cluster_init_script():
    sample_jobs = [
        BaseJob(
            created_time=1694536604319,
            creator_user_name="anonymous@databricks.com",
            job_id=536591785949415,
            settings=JobSettings(
                compute=None,
                continuous=None,
                tasks=[
                    Task(
                        task_key="Ingest",
                        existing_cluster_id="0807-225846-avon493",
                        notebook_task=NotebookTask(
                            notebook_path="/Users/foo.bar@databricks.com/Customers/Example/Test/Load"
                        ),
                        timeout_seconds=0,
                    )
                ],
                timeout_seconds=0,
            ),
        ),
        BaseJob(
            created_time=1694536604321,
            creator_user_name="anonymous@databricks.com",
            job_id=536591785949416,
            settings=JobSettings(
                compute=None,
                continuous=None,
                tasks=[
                    Task(
                        task_key="Ingest",
                        existing_cluster_id="0810-229933-chicago99",
                        notebook_task=NotebookTask(
                            notebook_path="/Users/foo.bar@databricks.com/Customers/Example/Test/Load"
                        ),
                        timeout_seconds=0,
                    )
                ],
                timeout_seconds=0,
            ),
        ),
        BaseJob(
            created_time=1694536604319,
            creator_user_name="anonymous@databricks.com",
            job_id=536591785949417,
            settings=JobSettings(
                compute=None,
                continuous=None,
                tasks=[
                    Task(
                        task_key="Ingest",
                        existing_cluster_id="0811-929933-maine96",
                        notebook_task=NotebookTask(
                            notebook_path="/Users/foo.bar@databricks.com/Customers/Example/Test/Load"
                        ),
                        timeout_seconds=0,
                    )
                ],
                timeout_seconds=0,
            ),
        ),
    ]

    ws = workspace_client_mock(cluster_ids=['init-scripts-dbfs'])
    ws.jobs.list.return_value = sample_jobs
    ws.dbfs.read().data = "JXNoCmVjaG8gIj0="
    result_set = JobsCrawler(ws, MockBackend(), "ucx").snapshot()
    assert len(result_set) == 3


def test_job_cluster_init_script_check_dbfs():
    sample_jobs = [
        BaseJob(
            created_time=1694536604319,
            creator_user_name="anonymous@databricks.com",
            job_id=536591785949415,
            settings=JobSettings(
                compute=None,
                continuous=None,
                tasks=[
                    Task(
                        task_key="Ingest",
                        existing_cluster_id="0807-225846-avon493",
                        notebook_task=NotebookTask(
                            notebook_path="/Users/foo.bar@databricks.com/Customers/Example/Test/Load"
                        ),
                        timeout_seconds=0,
                    )
                ],
                timeout_seconds=0,
            ),
        ),
        BaseJob(
            created_time=1694536604321,
            creator_user_name="anonymous@databricks.com",
            job_id=536591785949416,
            settings=JobSettings(
                compute=None,
                continuous=None,
                tasks=[
                    Task(
                        task_key="Ingest",
                        existing_cluster_id="0810-229933-chicago99",
                        notebook_task=NotebookTask(
                            notebook_path="/Users/foo.bar@databricks.com/Customers/Example/Test/Load"
                        ),
                        timeout_seconds=0,
                    )
                ],
                timeout_seconds=0,
            ),
        ),
        BaseJob(
            created_time=1694536604319,
            creator_user_name="anonymous@databricks.com",
            job_id=536591785949417,
            settings=JobSettings(
                compute=None,
                continuous=None,
                tasks=[
                    Task(
                        task_key="Ingest",
                        existing_cluster_id="0811-929933-maine96",
                        notebook_task=NotebookTask(
                            notebook_path="/Users/foo.bar@databricks.com/Customers/Example/Test/Load"
                        ),
                        timeout_seconds=0,
                    )
                ],
                timeout_seconds=0,
            ),
        ),
    ]

    sample_clusters = [
        ClusterDetails(
            init_scripts=[
                InitScriptInfo(
                    dbfs=DbfsStorageInfo(destination="dbfs"),
                    s3=None,
                    volumes=None,
                    workspace=None,
                ),
                InitScriptInfo(
                    dbfs=DbfsStorageInfo(destination="dbfs:"),
                    s3=None,
                    volumes=None,
                    workspace=None,
                ),
                InitScriptInfo(
                    dbfs=DbfsStorageInfo(destination=":/users/test@test.com/init_scripts/test.sh"),
                    s3=None,
                    volumes=None,
                    workspace=None,
                ),
                InitScriptInfo(
                    dbfs=None,
                    s3=None,
                    volumes=None,
                    workspace=WorkspaceStorageInfo(
                        destination="/Users/dipankar.kushari@databricks.com/init_script_1.sh"
                    ),
                ),
            ],
            autoscale=AutoScale(min_workers=1, max_workers=6),
            spark_context_id=5134472582179566666,
            spark_env_vars=None,
            spark_version="13.3.x-cpu-ml-scala2.12",
            cluster_id="0807-225846-avon493",
            cluster_source=ClusterSource.JOB,
        )
    ]
    ws = create_autospec(WorkspaceClient)
    ws.workspace.export().content = "JXNoCmVjaG8gIj0="
    ws.dbfs.read().data = "JXNoCmVjaG8gIj0="
    result_set = JobsCrawler(ws, MockBackend(), "ucx")._assess_jobs(
        sample_jobs, {c.cluster_id: c for c in sample_clusters}
    )
    assert len(result_set) == 3


def test_job_crawler_with_no_owner_should_have_empty_creator_name():
    sample_jobs = [
        BaseJob(
            created_time=1694536604319,
            creator_user_name=None,
            job_id=536591785949415,
            settings=JobSettings(
                compute=None,
                continuous=None,
                tasks=[
                    Task(
                        task_key="Ingest",
                        existing_cluster_id="0807-225846-avon493",
                        notebook_task=NotebookTask(
                            notebook_path="/Users/foo.bar@databricks.com/Customers/Example/Test/Load"
                        ),
                        timeout_seconds=0,
                    )
                ],
                timeout_seconds=0,
            ),
        )
    ]

    sample_clusters = [
        ClusterDetails(
            autoscale=AutoScale(min_workers=1, max_workers=6),
            spark_context_id=5134472582179566666,
            spark_env_vars=None,
            spark_version="13.3.x-cpu-ml-scala2.12",
            cluster_id="0807-225846-avon493",
            cluster_source=ClusterSource.JOB,
        )
    ]
    ws = Mock()
    mockbackend = MockBackend()
    ws.jobs.list.return_value = sample_jobs
    ws.clusters.list.return_value = sample_clusters
    JobsCrawler(ws, mockbackend, "ucx").snapshot()
    result = mockbackend.rows_written_for("hive_metastore.ucx.jobs", "append")
    assert result == [JobInfo(job_id="536591785949415", job_name="Unknown", creator=None, success=1, failures="[]")]

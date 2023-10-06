from unittest.mock import Mock

from databricks.sdk.core import DatabricksError
from databricks.sdk.service.compute import (
    AutoScale,
    ClusterDetails,
    ClusterSource,
    ClusterSpec,
    DataSecurityMode,
)
from databricks.sdk.service.jobs import (
    BaseJob,
    BaseRun,
    ClusterInstance,
    JobCluster,
    JobSettings,
    NotebookTask,
    RunTask,
    RunType,
    SparkJarTask,
    Task,
)
from databricks.sdk.service.pipelines import PipelineState, PipelineStateInfo

from databricks.labs.ucx.assessment.crawlers import (
    ClustersCrawler,
    ExternallyOrchestratedJobRunsWithFailingConfigCrawler,
    ExternallyOrchestratedJobRunWithFailingConfiguration,
    JobsCrawler,
    PipelineInfo,
    PipelinesCrawler,
)
from databricks.labs.ucx.hive_metastore.data_objects import ExternalLocationCrawler
from databricks.labs.ucx.hive_metastore.mounts import Mount
from databricks.labs.ucx.mixins.sql import Row
from tests.unit.framework.mocks import MockBackend


def test_external_locations():
    crawler = ExternalLocationCrawler(Mock(), MockBackend(), "test")
    row_factory = type("Row", (Row,), {"__columns__": ["location"]})
    sample_locations = [
        row_factory(["s3://us-east-1-dev-account-staging-uc-ext-loc-bucket-1/Location/Table"]),
        row_factory(["s3://us-east-1-dev-account-staging-uc-ext-loc-bucket-1/Location/Table2"]),
        row_factory(["s3://us-east-1-dev-account-staging-uc-ext-loc-bucket-23/testloc/Table3"]),
        row_factory(["s3://us-east-1-dev-account-staging-uc-ext-loc-bucket-23/anotherloc/Table4"]),
        row_factory(["dbfs:/mnt/ucx/database1/table1"]),
        row_factory(["dbfs:/mnt/ucx/database2/table2"]),
        row_factory(["DatabricksRootmntDatabricksRoot"]),
    ]
    sample_mounts = [Mount("/mnt/ucx", "s3://us-east-1-ucx-container")]
    result_set = crawler._external_locations(sample_locations, sample_mounts)
    assert len(result_set) == 3
    assert result_set[0].location == "s3://us-east-1-dev-account-staging-uc-ext-loc-bucket-1/Location/"
    assert result_set[1].location == "s3://us-east-1-dev-account-staging-uc-ext-loc-bucket-23/"
    assert result_set[2].location == "s3://us-east-1-ucx-container/"


def test_job_assessment():
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
                        existing_cluster_id="0807-225846-motto493",
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
                        existing_cluster_id="0810-225833-atlanta69",
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
            spark_conf={"spark.databricks.delta.preview.enabled": "true"},
            spark_context_id=5134472582179565315,
            spark_env_vars=None,
            spark_version="13.3.x-cpu-ml-scala2.12",
            cluster_id="0807-225846-motto493",
            cluster_source=ClusterSource.UI,
        ),
        ClusterDetails(
            autoscale=AutoScale(min_workers=1, max_workers=6),
            spark_conf={"spark.databricks.delta.preview.enabled": "true"},
            spark_context_id=5134472582179565315,
            spark_env_vars=None,
            spark_version="9.3.x-cpu-ml-scala2.12",
            cluster_id="0810-225833-atlanta69",
            cluster_source=ClusterSource.UI,
        ),
        ClusterDetails(
            autoscale=AutoScale(min_workers=1, max_workers=6),
            spark_conf={"spark.databricks.delta.preview.enabled": "true"},
            spark_context_id=5134472582179566666,
            spark_env_vars=None,
            spark_version="13.3.x-cpu-ml-scala2.12",
            cluster_id="0810-229933-chicago12",
            cluster_source=ClusterSource.JOB,
        ),
    ]
    result_set = JobsCrawler(Mock(), MockBackend(), "ucx")._assess_jobs(
        sample_jobs, {c.cluster_id: c for c in sample_clusters}
    )
    assert len(result_set) == 2
    assert result_set[0].success == 1
    assert result_set[1].success == 0


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
                "spark.hadoop.fs.azure.account."
                "oauth2.client.id.abcde.dfs.core.windows.net": "{{secrets/abcff/sp_app_client_id}}",
                "spark.hadoop.fs.azure.account."
                "oauth2.client.endpoint.abcde.dfs.core.windows.net": "https://login.microsoftonline.com/dedededede/token",
                "spark.hadoop.fs.azure.account."
                "oauth2.client.secret.abcde.dfs.core.windows.net": "{{secrets/abcff/sp_secret}}",
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


def test_cluster_assessment(mocker):
    sample_clusters = [
        ClusterDetails(
            autoscale=AutoScale(min_workers=1, max_workers=6),
            spark_conf={"spark.databricks.delta.preview.enabled": "true"},
            spark_context_id=5134472582179565315,
            spark_env_vars=None,
            spark_version="13.3.x-cpu-ml-scala2.12",
            cluster_id="0807-225846-motto493",
        ),
        ClusterDetails(
            autoscale=AutoScale(min_workers=1, max_workers=6),
            spark_conf={"spark.databricks.delta.preview.enabled": "true"},
            spark_context_id=5134472582179565315,
            spark_env_vars=None,
            spark_version="9.3.x-cpu-ml-scala2.12",
            cluster_id="0810-225833-atlanta69",
        ),
        ClusterDetails(
            cluster_name="Tech Summit FY24 Cluster",
            autoscale=AutoScale(min_workers=1, max_workers=6),
            spark_conf={
                "spark.hadoop.fs.azure.account."
                "oauth2.client.id.abcde.dfs.core.windows.net": "{{secrets/abcff/sp_app_client_id}}",
                "spark.hadoop.fs.azure.account."
                "oauth2.client.endpoint.abcde.dfs.core.windows.net": "https://login.microsoftonline.com/dedededede/token",
                "spark.hadoop.fs.azure.account."
                "oauth2.client.secret.abcde.dfs.core.windows.net": "{{secrets/abcff/sp_secret}}",
            },
            spark_context_id=5134472582179565315,
            spark_env_vars=None,
            spark_version="13.3.x-cpu-ml-scala2.12",
            cluster_id="0915-190044-3dqy6751",
        ),
        ClusterDetails(
            cluster_name="Tech Summit FY24 Cluster-1",
            autoscale=AutoScale(min_workers=1, max_workers=6),
            spark_context_id=5134472582179565315,
            spark_env_vars=None,
            policy_id="D96308F1BF0003A7",
            spark_version="13.3.x-cpu-ml-scala2.12",
            cluster_id="0915-190044-3dqy6751",
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

    crawler = ClustersCrawler(ws, MockBackend(), "ucx")._assess_clusters(sample_clusters)
    result_set = list(crawler)

    assert len(result_set) == 4
    assert result_set[0].success == 1
    assert result_set[1].success == 0
    assert result_set[2].success == 0
    assert result_set[3].success == 0


def test_cluster_assessment_cluster_policy_no_spark_conf(mocker):
    sample_clusters1 = [
        ClusterDetails(
            cluster_name="Tech Summit FY24 Cluster-2",
            autoscale=AutoScale(min_workers=1, max_workers=6),
            spark_context_id=5134472582179565315,
            spark_env_vars=None,
            policy_id="D96308F1BF0003A8",
            spark_version="13.3.x-cpu-ml-scala2.12",
            cluster_id="0915-190044-3dqy6751",
        )
    ]
    ws = Mock()
    ws.cluster_policies.get().definition = (
        '{"node_type_id":{"type":"allowlist","values":["Standard_DS3_v2",'
        '"Standard_DS4_v2","Standard_DS5_v2","Standard_NC4as_T4_v3"],"defaultValue":'
        '"Standard_DS3_v2"},"spark_version":{"type":"unlimited","defaultValue":"auto:latest-ml"},'
        '"runtime_engine":{"type":"fixed","value":"STANDARD","hidden":true},'
        '"num_workers":{"type":"fixed","value":0,"hidden":true},"data_security_mode":'
        '{"type":"allowlist","values":["SINGLE_USER","LEGACY_SINGLE_USER","LEGACY_SINGLE_USER_STANDARD"],'
        '"defaultValue":"SINGLE_USER","hidden":true},"driver_instance_pool_id":{"type":"forbidden","hidden":true},'
        '"cluster_type":{"type":"fixed","value":"all-purpose"},"instance_pool_id":{"type":"forbidden","hidden":true},'
        '"azure_attributes.availability":{"type":"fixed","value":"ON_DEMAND_AZURE","hidden":true},'
        '"spark_conf.spark.databricks.cluster.profile":{"type":"fixed","value":"singleNode","hidden":true},'
        '"autotermination_minutes":{"type":"unlimited","defaultValue":4320,"isOptional":true}}'
    )

    ws.cluster_policies.get().policy_family_definition_overrides = (
        '{\n  "not.spark.conf": {\n    '
        '"type": "fixed",\n    "value": "OAuth",\n   '
        ' "hidden": true\n  },\n  "not.a.type": {\n   '
        ' "type": "fixed",\n    "value": '
        '"not.a.matching.type",\n    '
        '"hidden": true\n  },\n  "not.a.matching.type": {\n    '
        '"type": "fixed",\n    "value": "fsfsfsfsffsfsf",\n    "hidden": true\n  },\n  '
        '"not.a.matching.type": {\n    "type": "fixed",\n    '
        '"value": "gfgfgfgfggfggfgfdds",\n    "hidden": true\n  },\n  '
        '"not.a.matching.type": {\n    '
        '"type": "fixed",\n    '
        '"value": "https://login.microsoftonline.com/1234ededed/oauth2/token",\n    '
        '"hidden": true\n  }\n}'
    )

    crawler = ClustersCrawler(ws, MockBackend(), "ucx")._assess_clusters(sample_clusters1)
    result_set1 = list(crawler)
    assert len(result_set1) == 1
    assert result_set1[0].success == 1


def test_cluster_assessment_cluster_policy_not_found(mocker):
    sample_clusters1 = [
        ClusterDetails(
            cluster_name="cluster1",
            autoscale=AutoScale(min_workers=1, max_workers=6),
            spark_context_id=5134472582179565315,
            spark_env_vars=None,
            policy_id="D96308F1BF0003A8",
            spark_version="13.3.x-cpu-ml-scala2.12",
            cluster_id="0915-190044-3dqy6751",
        )
    ]
    ws = Mock()
    ws.cluster_policies.get.side_effect = DatabricksError(error="NO_POLICY", error_code="NO_POLICY")
    crawler = ClustersCrawler(ws, MockBackend(), "ucx")._assess_clusters(sample_clusters1)
    list(crawler)


def test_pipeline_assessment_with_config(mocker):
    sample_pipelines = [
        PipelineStateInfo(
            cluster_id=None,
            creator_user_name="abcde.defgh@databricks.com",
            latest_updates=None,
            name="New DLT Pipeline",
            pipeline_id="0112eae7-9d11-4b40-a2b8-6c83cb3c7407",
            run_as_user_name="abcde.defgh@databricks.com",
            state=PipelineState.IDLE,
        )
    ]

    ws = Mock()
    config_dict = {
        "spark.hadoop.fs.azure.account.auth.type.abcde.dfs.core.windows.net": "SAS",
        "spark.hadoop.fs.azure.sas.token.provider.type.abcde.dfs."
        "core.windows.net": "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider",
        "spark.hadoop.fs.azure.sas.fixed.token.abcde.dfs.core.windows.net": "{{secrets/abcde_access/sasFixedToken}}",
    }
    ws.pipelines.get().spec.configuration = config_dict

    crawler = PipelinesCrawler(ws, MockBackend(), "ucx")._assess_pipelines(sample_pipelines)
    result_set = list(crawler)

    assert len(result_set) == 1
    assert result_set[0].success == 0


def test_pipeline_assessment_without_config(mocker):
    sample_pipelines = [
        PipelineStateInfo(
            cluster_id=None,
            creator_user_name="abcde.defgh@databricks.com",
            latest_updates=None,
            name="New DLT Pipeline",
            pipeline_id="0112eae7-9d11-4b40-a2b8-6c83cb3c7497",
            run_as_user_name="abcde.defgh@databricks.com",
            state=PipelineState.IDLE,
        )
    ]
    ws = Mock()
    config_dict = {}
    ws.pipelines.get().spec.configuration = config_dict
    crawler = PipelinesCrawler(ws, MockBackend(), "ucx")._assess_pipelines(sample_pipelines)
    result_set = list(crawler)

    assert len(result_set) == 1
    assert result_set[0].success == 1


def test_pipeline_snapshot_with_config():
    sample_pipelines = [
        PipelineInfo(
            creator_name="abcde.defgh@databricks.com",
            pipeline_name="New DLT Pipeline",
            pipeline_id="0112eae7-9d11-4b40-a2b8-6c83cb3c7497",
            success=1,
            failures="",
        )
    ]
    mock_ws = Mock()

    crawler = PipelinesCrawler(mock_ws, MockBackend(), "ucx")

    crawler._try_fetch = Mock(return_value=[])
    crawler._crawl = Mock(return_value=sample_pipelines)

    result_set = crawler.snapshot()

    assert len(result_set) == 1
    assert result_set[0].success == 1


def test_externally_orchestrated_jobs_with_failing_config_crawler():
    """
    Simple test to validate that JobsRunCrawler
     - returns a list of JobRunInfo objects
     - of appropriate size
     - with expected values
    """
    sample_ext_jobs = [
        ExternallyOrchestratedJobRunWithFailingConfiguration(
            run_id=123456789,
            hashed_id="test1",
            spark_version="11.3.x-scala2.12",
            num_tasks_with_failing_configuration=1,
        ),
        ExternallyOrchestratedJobRunWithFailingConfiguration(
            run_id=123456790,
            hashed_id="test2",
            spark_version="11.3.x-scala2.12",
            num_tasks_with_failing_configuration=3,
        ),
    ]
    mock_ws = Mock()

    crawler = ExternallyOrchestratedJobRunsWithFailingConfigCrawler(mock_ws, MockBackend(), "ucx")

    crawler._try_fetch = Mock(return_value=[])
    crawler._crawl = Mock(return_value=sample_ext_jobs)

    result_set = crawler.snapshot()

    assert len(result_set) == 2
    assert result_set[0].num_tasks_with_failing_configuration == 1
    assert result_set[1].num_tasks_with_failing_configuration == 3


def test_externally_orchestrated_jobs_with_failing_config_crawler_filters_runs_with_job_id():
    """
    Test to validate
     - job runs with a persisted job id are not included in the result set
    """
    sample_job_runs = [
        BaseRun(
            job_id=12345678910,
            run_id=123456789,
            run_type=RunType.SUBMIT_RUN,
            tasks=[
                RunTask(
                    notebook_task=NotebookTask(notebook_path="/some/notebook/path"),
                    new_cluster=ClusterSpec(spark_version="11.3.x-scala2.12", node_type_id="r3.xlarge", num_workers=8),
                    task_key="task1",
                )
            ],
        ),
        BaseRun(
            job_id=12345678909,
            run_id=123456790,
            run_type=RunType.SUBMIT_RUN,
            job_clusters=[
                JobCluster(
                    job_cluster_key="my_ephemeral_job_cluster",
                    new_cluster=ClusterSpec(
                        spark_version="2.1.0-db3-scala2.11",
                        node_type_id="r3.xlarge",
                        num_workers=8,
                    ),
                )
            ],
            tasks=[
                RunTask(
                    spark_jar_task=SparkJarTask(
                        jar_uri="dbfs:/some/jar.jar", main_class_name="some.awesome.class.name"
                    ),
                    task_key="task2",
                    existing_cluster_id="my_ephemeral_job_cluster",
                )
            ],
        ),
    ]

    sample_jobs = [
        BaseJob(
            job_id=12345678909,
        )
    ]

    sample_clusters = [
        ClusterDetails(
            autoscale=AutoScale(min_workers=1, max_workers=6),
            spark_context_id=5134472582179565315,
            spark_env_vars=None,
            spark_version="11.3.x-scala2.12",
            cluster_id="my_job_cluster",
            cluster_source=ClusterSource.JOB,
        )
    ]
    mock_ws = Mock()

    mock_ws.jobs.list_runs = Mock(return_value=sample_job_runs)
    mock_ws.clusters.list = Mock(return_value=sample_clusters)
    mock_ws.jobs.list = Mock(return_value=sample_jobs)

    crawler = ExternallyOrchestratedJobRunsWithFailingConfigCrawler(mock_ws, MockBackend(), "ucx")

    crawler._try_fetch = Mock(return_value=[])

    result_set = crawler.snapshot()

    assert len(result_set) == 1


def test_externally_orchestrated_jobs_with_failing_config_crawler_returns_multiple_task_counts():
    """
    Test to validate
     - multiple tasks are returned in a single job run
     - clusters are resolved from new_cluster, existing_cluster_id on the task
     - if data security mode is set, task will not count
    """
    sample_job_runs = [
        BaseRun(
            job_id=12345678909,
            run_id=123456789,
            run_type=RunType.SUBMIT_RUN,
            job_clusters=[
                JobCluster(
                    job_cluster_key="my_ephemeral_job_cluster",
                    new_cluster=ClusterSpec(
                        spark_version="11.3.x-scala2.12",
                        node_type_id="r3.xlarge",
                        num_workers=8,
                        data_security_mode=DataSecurityMode.SINGLE_USER,
                    ),
                )
            ],
            tasks=[
                RunTask(
                    spark_jar_task=SparkJarTask(
                        jar_uri="dbfs:/some/jar.jar", main_class_name="some.awesome.class.name"
                    ),
                    task_key="task1",
                    existing_cluster_id="my_ephemeral_job_cluster",
                ),
                RunTask(
                    notebook_task=NotebookTask(notebook_path="/some/notebook/path"),
                    new_cluster=ClusterSpec(
                        spark_version="2.1.0-db3-scala2.11", node_type_id="r3.xlarge", num_workers=8
                    ),
                    task_key="task2",
                ),
                RunTask(
                    notebook_task=NotebookTask(notebook_path="/some/notebook/path"),
                    task_key="task3",
                    cluster_instance=ClusterInstance(cluster_id="my_persistent_job_cluster"),
                ),
            ],
        )
    ]

    sample_jobs = [
        BaseJob(
            job_id=12345678910,
        )
    ]

    sample_clusters = [
        ClusterDetails(
            autoscale=AutoScale(min_workers=1, max_workers=6),
            spark_context_id=5134472582179565315,
            spark_env_vars=None,
            spark_version="12.1.x-scala2.12",
            cluster_id="my_persistent_job_cluster",
            cluster_source=ClusterSource.JOB,
        )
    ]
    mock_ws = Mock()

    mock_ws.jobs.list_runs = Mock(return_value=sample_job_runs)
    mock_ws.clusters.list = Mock(return_value=sample_clusters)
    mock_ws.jobs.list = Mock(return_value=sample_jobs)

    crawler = ExternallyOrchestratedJobRunsWithFailingConfigCrawler(mock_ws, MockBackend(), "ucx")

    crawler._try_fetch = Mock(return_value=[])

    result_set = crawler.snapshot()

    assert len(result_set) == 1
    assert result_set[0].spark_version == "12.1.x-scala2.12"
    assert result_set[0].run_id == 123456789
    assert result_set[0].num_tasks_with_failing_configuration == 1


def test_externally_orchestrated_jobs_crawler_deterministic_hashing():
    """
    Test to validate
     - when multiple job runs are submitted that are effectively identical they receive the same hash
    """
    sample_job_runs = [
        BaseRun(
            job_id=12345678911,
            run_id=123456789,
            run_type=RunType.SUBMIT_RUN,
            job_clusters=[
                JobCluster(
                    job_cluster_key="my_ephemeral_job_cluster",
                    new_cluster=ClusterSpec(
                        spark_version="11.3.x-scala2.12",
                        node_type_id="r3.xlarge",
                        num_workers=8,
                        data_security_mode=DataSecurityMode.SINGLE_USER,
                    ),
                )
            ],
            tasks=[
                RunTask(  # should not fail
                    spark_jar_task=SparkJarTask(
                        jar_uri="dbfs:/some/jar.jar", main_class_name="some.awesome.class.name"
                    ),
                    task_key="task1",
                    existing_cluster_id="my_ephemeral_job_cluster",  # this should pull from all clusters, pre-existing
                ),
                RunTask(  # should not fail
                    notebook_task=NotebookTask(notebook_path="/some/notebook/path"),
                    new_cluster=ClusterSpec(
                        spark_version="2.1.0-db3-scala2.11", node_type_id="r3.xlarge", num_workers=8
                    ),
                    task_key="task2",
                ),
                RunTask(  # should fail
                    notebook_task=NotebookTask(notebook_path="/some/notebook/path"),
                    task_key="task3",
                    cluster_instance=ClusterInstance(cluster_id="my_persistent_job_cluster"),
                ),
            ],
        ),
        BaseRun(
            job_id=12345678912,
            run_id=987654321,
            run_type=RunType.SUBMIT_RUN,
            job_clusters=[
                JobCluster(
                    job_cluster_key="my_ephemeral_job_cluster",
                    new_cluster=ClusterSpec(
                        spark_version="11.3.x-scala2.12",
                        node_type_id="r3.xlarge",
                        num_workers=8,
                        data_security_mode=DataSecurityMode.SINGLE_USER,
                    ),
                )
            ],
            tasks=[
                RunTask(  # should not fail
                    spark_jar_task=SparkJarTask(
                        jar_uri="dbfs:/some/jar.jar", main_class_name="some.awesome.class.name"
                    ),
                    task_key="task4",
                    existing_cluster_id="my_ephemeral_job_cluster",  # this should pull from all clusters, pre-existing
                ),
                RunTask(  # should fail
                    notebook_task=NotebookTask(notebook_path="/some/notebook/path"),
                    new_cluster=ClusterSpec(
                        spark_version="11.3.x-scala2.12",
                        node_type_id="r3.xlarge",
                        num_workers=8,
                    ),
                    task_key="task5",
                ),
            ],
        ),
    ]
    sample_jobs = [
        BaseJob(
            job_id=12345678910,
        )
    ]

    sample_clusters = [
        ClusterDetails(
            autoscale=AutoScale(min_workers=1, max_workers=6),
            spark_context_id=5134472582179565315,
            spark_env_vars=None,
            spark_version="12.1.x-scala2.12",
            cluster_id="my_persistent_job_cluster",
            cluster_source=ClusterSource.JOB,
        )
    ]
    mock_ws = Mock()

    mock_ws.jobs.list_runs = Mock(return_value=sample_job_runs)
    mock_ws.clusters.list = Mock(return_value=sample_clusters)
    mock_ws.jobs.list = Mock(return_value=sample_jobs)

    crawler = ExternallyOrchestratedJobRunsWithFailingConfigCrawler(mock_ws, MockBackend(), "ucx")

    crawler._try_fetch = Mock(return_value=[])

    result_set = crawler.snapshot()

    assert len(result_set) == 1
    assert result_set[0].num_tasks_with_failing_configuration == 1

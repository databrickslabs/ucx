from unittest.mock import Mock

from databricks.sdk.service.pipelines import PipelineState, PipelineStateInfo

from databricks.labs.ucx.assessment.azure import AzureServicePrincipalCrawler
from databricks.labs.ucx.assessment.pipelines import PipelineInfo, PipelinesCrawler

from ..framework.mocks import MockBackend


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


def test_pipeline_list_with_no_config():
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
    mock_ws.pipelines.list_pipelines.return_value = sample_pipelines
    config_dict = {"spark.hadoop.fs.azure1.account.oauth2.client.id.abcde.dfs.core.windows.net": "wewewerty"}
    mock_ws.pipelines.get().spec.configuration = config_dict
    crawler = AzureServicePrincipalCrawler(mock_ws, MockBackend(), "ucx")._list_all_pipeline_with_spn_in_spark_conf()

    assert len(crawler) == 0


def test_pipeline_without_owners_should_have_empty_creator_name():
    sample_pipelines = [
        PipelineStateInfo(
            cluster_id=None,
            creator_user_name=None,
            latest_updates=None,
            name="New DLT Pipeline",
            pipeline_id="0112eae7-9d11-4b40-a2b8-6c83cb3c7407",
            run_as_user_name="abcde.defgh@databricks.com",
            state=PipelineState.IDLE,
        )
    ]

    ws = Mock()
    ws.pipelines.list_pipelines.return_value = sample_pipelines
    ws.pipelines.get().spec.configuration = {}
    mockbackend = MockBackend()
    PipelinesCrawler(ws, mockbackend, "ucx").snapshot()
    result = mockbackend.rows_written_for("hive_metastore.ucx.pipelines", "append")

    assert result == [
        PipelineInfo(
            pipeline_id="0112eae7-9d11-4b40-a2b8-6c83cb3c7407",
            pipeline_name="New DLT Pipeline",
            creator_name=None,
            success=1,
            failures="[]",
        )
    ]

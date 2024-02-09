from unittest.mock import Mock

from databricks.sdk.service.pipelines import PipelineState, PipelineStateInfo

from databricks.labs.ucx.assessment.azure import AzureServicePrincipalCrawler
from databricks.labs.ucx.assessment.pipelines import PipelineInfo, PipelinesCrawler

from ..framework.mocks import MockBackend
from . import workspace_client_mock


def test_pipeline_assessment_with_config():
    sample_pipelines = [
        PipelineStateInfo(
            cluster_id=None,
            creator_user_name="abcde.defgh@databricks.com",
            latest_updates=None,
            name="New DLT Pipeline",
            pipeline_id="spec-with-spn",
            run_as_user_name="abcde.defgh@databricks.com",
            state=PipelineState.IDLE,
        )
    ]

    ws = workspace_client_mock(clusters="job-source-cluster.json")
    ws.workspace.export().content = "JXNoCmVjaG8gIj0="
    ws.dbfs.read().data = "JXNoCmVjaG8gIj0="

    ws.pipelines.list_pipelines.return_value = sample_pipelines
    crawler = PipelinesCrawler(ws, MockBackend(), "ucx").snapshot()
    result_set = list(crawler)

    assert len(result_set) == 1
    assert result_set[0].success == 0


def test_pipeline_assessment_without_config():
    sample_pipelines = [
        PipelineStateInfo(
            cluster_id=None,
            creator_user_name="abcde.defgh@databricks.com",
            latest_updates=None,
            name="New DLT Pipeline",
            pipeline_id="empty-spec",
            run_as_user_name="abcde.defgh@databricks.com",
            state=PipelineState.IDLE,
        )
    ]
    ws = workspace_client_mock(clusters="job-source-cluster.json")
    ws.workspace.export().content = "JXNoCmVjaG8gIj0="
    ws.dbfs.read().data = "JXNoCmVjaG8gIj0="
    ws.pipelines.list_pipelines.return_value = sample_pipelines
    crawler = PipelinesCrawler(ws, MockBackend(), "ucx").snapshot()
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
    mock_ws = workspace_client_mock(clusters="job-source-cluster.json")
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
            pipeline_id="empty-spec",
            success=1,
            failures="",
        )
    ]
    mock_ws = workspace_client_mock(clusters="no-spark-conf.json")
    mock_ws.pipelines.list_pipelines.return_value = sample_pipelines
    crawler = AzureServicePrincipalCrawler(mock_ws, MockBackend(), "ucx").snapshot()

    assert len(crawler) == 0


def test_pipeline_without_owners_should_have_empty_creator_name():
    sample_pipelines = [
        PipelineStateInfo(
            cluster_id=None,
            creator_user_name=None,
            latest_updates=None,
            name="New DLT Pipeline",
            pipeline_id="empty-spec",
            run_as_user_name="abcde.defgh@databricks.com",
            state=PipelineState.IDLE,
        )
    ]

    ws = workspace_client_mock(clusters="no-spark-conf.json")
    ws.pipelines.list_pipelines.return_value = sample_pipelines
    ws.workspace.export().content = "JXNoCmVjaG8gIj0="
    ws.dbfs.read().data = "JXNoCmVjaG8gIj0="
    mockbackend = MockBackend()
    PipelinesCrawler(ws, mockbackend, "ucx").snapshot()
    result = mockbackend.rows_written_for("hive_metastore.ucx.pipelines", "append")

    assert result == [
        PipelineInfo(
            pipeline_id="empty-spec",
            pipeline_name="New DLT Pipeline",
            creator_name=None,
            success=1,
            failures="[]",
        )
    ]

from databricks.labs.lsql import Row
from databricks.labs.lsql.backends import MockBackend

from databricks.labs.ucx.assessment.azure import AzureServicePrincipalCrawler
from databricks.labs.ucx.assessment.pipelines import PipelinesCrawler

from .. import mock_workspace_client


def test_pipeline_assessment_with_config():
    ws = mock_workspace_client(pipeline_ids=['spec-with-spn'])
    ws.dbfs.read().data = "JXNoCmVjaG8gIj0="

    crawler = PipelinesCrawler(ws, MockBackend(), "ucx").snapshot()
    result_set = list(crawler)

    assert len(result_set) == 1
    assert result_set[0].success == 0


def test_pipeline_assessment_without_config():
    ws = mock_workspace_client(pipeline_ids=['empty-spec'])
    ws.dbfs.read().data = "JXNoCmVjaG8gIj0="
    crawler = PipelinesCrawler(ws, MockBackend(), "ucx").snapshot()
    result_set = list(crawler)

    assert len(result_set) == 1
    assert result_set[0].success == 1


def test_pipeline_snapshot_with_config():
    ws = mock_workspace_client(pipeline_ids=['empty-spec'])
    crawler = PipelinesCrawler(ws, MockBackend(), "ucx")
    result_set = crawler.snapshot()

    assert len(result_set) == 1
    assert result_set[0].success == 1


def test_pipeline_list_with_no_config():
    mock_ws = mock_workspace_client(pipeline_ids=['empty-spec'])
    crawler = AzureServicePrincipalCrawler(mock_ws, MockBackend(), "ucx").snapshot()

    assert len(crawler) == 0


def test_pipeline_without_owners_should_have_empty_creator_name():
    ws = mock_workspace_client(pipeline_ids=['empty-spec'])
    ws.dbfs.read().data = "JXNoCmVjaG8gIj0="
    mockbackend = MockBackend()
    PipelinesCrawler(ws, mockbackend, "ucx").snapshot()
    result = mockbackend.rows_written_for("hive_metastore.ucx.pipelines", "append")

    assert result == [
        Row(
            pipeline_id="empty-spec",
            pipeline_name="New DLT Pipeline",
            creator_name=None,
            success=1,
            failures="[]",
        )
    ]

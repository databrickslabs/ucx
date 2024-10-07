from unittest.mock import create_autospec, PropertyMock

from databricks.labs.lsql.backends import MockBackend
from databricks.sdk.service.pipelines import GetPipelineResponse, PipelineStateInfo

from databricks.labs.ucx.assessment.azure import AzureServicePrincipalCrawler
from databricks.labs.ucx.assessment.pipelines import PipelineOwnership, PipelineInfo, PipelinesCrawler
from databricks.labs.ucx.framework.owners import AdministratorLocator

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


def test_pipeline_crawler_creator():
    ws = mock_workspace_client()
    ws.pipelines.list_pipelines.return_value = (
        PipelineStateInfo(pipeline_id="1", creator_user_name=None),
        PipelineStateInfo(pipeline_id="2", creator_user_name=""),
        PipelineStateInfo(pipeline_id="3", creator_user_name="bob"),
    )
    ws.pipelines.get = create_autospec(GetPipelineResponse)  # pylint: disable=mock-no-usage
    result = PipelinesCrawler(ws, MockBackend(), "ucx").snapshot(force_refresh=True)

    expected_creators = [None, None, "bob"]
    crawled_creators = [record.creator_name for record in result]
    assert len(expected_creators) == len(crawled_creators)
    assert set(expected_creators) == set(crawled_creators)


def test_pipeline_owner_creator() -> None:
    admin_locator = create_autospec(AdministratorLocator)  # pylint: disable=mock-no-usage
    mock_workspace_administrator = PropertyMock(return_value="an_admin")
    type(admin_locator).workspace_administrator = mock_workspace_administrator

    ownership = PipelineOwnership(admin_locator)
    owner = ownership.owner_of(PipelineInfo(creator_name="bob", pipeline_id="1", success=1, failures="[]"))

    assert owner == "bob"
    mock_workspace_administrator.assert_not_called()


def test_pipeline_owner_creator_unknown() -> None:
    admin_locator = create_autospec(AdministratorLocator)  # pylint: disable=mock-no-usage
    mock_workspace_administrator = PropertyMock(return_value="an_admin")
    type(admin_locator).workspace_administrator = mock_workspace_administrator

    ownership = PipelineOwnership(admin_locator)
    owner = ownership.owner_of(PipelineInfo(creator_name=None, pipeline_id="1", success=1, failures="[]"))

    assert owner == "an_admin"
    mock_workspace_administrator.assert_called_once()

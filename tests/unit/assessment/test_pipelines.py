import logging
from unittest.mock import create_autospec

import pytest
from databricks.labs.lsql.backends import MockBackend
from databricks.labs.lsql.core import Row
from databricks.sdk.service.pipelines import GetPipelineResponse, PipelineStateInfo, PipelineSpec
from databricks.sdk.errors import ResourceDoesNotExist

from databricks.labs.ucx.__about__ import __version__ as ucx_version
from databricks.labs.ucx.assessment.azure import AzureServicePrincipalCrawler
from databricks.labs.ucx.assessment.pipelines import PipelineOwnership, PipelineInfo, PipelinesCrawler
from databricks.labs.ucx.framework.owners import AdministratorLocator
from databricks.labs.ucx.progress.history import ProgressEncoder

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


def test_include_pipeline_ids():
    ws = mock_workspace_client(pipeline_ids=['empty-spec', 'spec-with-spn'])
    crawler = PipelinesCrawler(ws, MockBackend(), "ucx", include_pipeline_ids=['empty-spec'])
    result_set = list(crawler.snapshot())

    assert len(result_set) == 1
    assert result_set[0].pipeline_id == 'empty-spec'


def test_pipeline_disappears_during_crawl(ws, mock_backend, caplog) -> None:
    """Check that crawling doesn't fail if a pipeline is deleted after we list the pipelines but before we assess it."""
    ws.pipelines.list_pipelines.return_value = (
        PipelineStateInfo(pipeline_id="1", name="will_remain"),
        PipelineStateInfo(pipeline_id="2", name="will_disappear"),
    )

    def mock_get(pipeline_id: str) -> GetPipelineResponse:
        if pipeline_id == "2":
            raise ResourceDoesNotExist("Simulated disappearance")
        return GetPipelineResponse(pipeline_id=pipeline_id, spec=PipelineSpec(id=pipeline_id))

    ws.pipelines.get = mock_get

    with caplog.at_level(logging.WARNING):
        results = PipelinesCrawler(ws, mock_backend, "a_schema").snapshot()

    assert results == [
        PipelineInfo(pipeline_id="1", pipeline_name="will_remain", creator_name=None, success=1, failures="[]")
    ]
    assert "Pipeline disappeared, cannot assess: will_disappear (id=2)" in caplog.messages


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
    admin_locator = create_autospec(AdministratorLocator)

    ownership = PipelineOwnership(admin_locator)
    owner = ownership.owner_of(PipelineInfo(creator_name="bob", pipeline_id="1", success=1, failures="[]"))

    assert owner == "bob"
    admin_locator.get_workspace_administrator.assert_not_called()


def test_pipeline_owner_creator_unknown() -> None:
    admin_locator = create_autospec(AdministratorLocator)
    admin_locator.get_workspace_administrator.return_value = "an_admin"

    ownership = PipelineOwnership(admin_locator)
    owner = ownership.owner_of(PipelineInfo(creator_name=None, pipeline_id="1", success=1, failures="[]"))

    assert owner == "an_admin"
    admin_locator.get_workspace_administrator.assert_called_once()


@pytest.mark.parametrize(
    "pipeline_info_record,history_record",
    (
        (
            PipelineInfo(
                pipeline_id="1234",
                success=1,
                failures="[]",
                pipeline_name="a_pipeline",
                creator_name="user@domain",
            ),
            Row(
                workspace_id=2,
                job_run_id=1,
                object_type="PipelineInfo",
                object_id=["1234"],
                data={
                    "pipeline_id": "1234",
                    "success": "1",
                    "pipeline_name": "a_pipeline",
                    "creator_name": "user@domain",
                },
                failures=[],
                owner="user@domain",
                ucx_version=ucx_version,
            ),
        ),
        (
            PipelineInfo(pipeline_id="1234", success=0, failures='["a-failure", "b-failure"]'),
            Row(
                workspace_id=2,
                job_run_id=1,
                object_type="PipelineInfo",
                object_id=["1234"],
                data={
                    "pipeline_id": "1234",
                    "success": "0",
                },
                failures=["a-failure", "b-failure"],
                owner="the_admin",
                ucx_version=ucx_version,
            ),
        ),
    ),
)
def test_pipeline_info_supports_history(mock_backend, pipeline_info_record: PipelineInfo, history_record: Row) -> None:
    """Verify that PipelineInfo records are written as expected to the history log."""
    admin_locator = create_autospec(AdministratorLocator)
    admin_locator.get_workspace_administrator.return_value = "the_admin"
    pipeline_ownership = PipelineOwnership(admin_locator)
    history_log = ProgressEncoder[PipelineInfo](
        mock_backend,
        pipeline_ownership,
        PipelineInfo,
        run_id=1,
        workspace_id=2,
        catalog="a_catalog",
    )

    history_log.append_inventory_snapshot([pipeline_info_record])

    rows = mock_backend.rows_written_for("`a_catalog`.`multiworkspace`.`historical`", mode="append")

    assert rows == [history_record]

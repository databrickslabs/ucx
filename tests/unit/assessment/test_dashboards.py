import logging
import json
from unittest.mock import call, create_autospec
from typing import Iterator

import pytest
from databricks.labs.lsql.lakeview import Dashboard as LsqlLakeviewDashboard, Dataset
from databricks.labs.lsql.backends import Row
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, PermissionDenied, TooManyRequests
from databricks.sdk.service.dashboards import Dashboard as SdkLakeviewDashboard
from databricks.sdk.service.sql import Dashboard as SdkRedashDashboard, LegacyVisualization, LegacyQuery, Widget

from databricks.labs.ucx.assessment.dashboards import (
    LakeviewDashboard,
    LakeviewDashboardCrawler,
    RedashDashboard,
    RedashDashboardCrawler,
)


@pytest.mark.parametrize(
    "sdk_dashboard, expected",
    [
        (SdkRedashDashboard(id="id"), RedashDashboard("id")),
        (
            SdkRedashDashboard(
                id="did",
                name="name",
                parent="parent",
                tags=["tag1", "tag2"],
                widgets=[
                    Widget(visualization=LegacyVisualization(query=LegacyQuery(id="qid1"))),
                    Widget(visualization=LegacyVisualization(query=LegacyQuery(id="qid2"))),
                ],
            ),
            RedashDashboard("did", "name", "parent", ["qid1", "qid2"], ["tag1", "tag2"]),
        ),
        (
            SdkRedashDashboard(
                id="did",
                name="name",
                parent="parent",
                tags=["tag1", "tag2"],
                widgets=[
                    Widget(),
                    Widget(visualization=LegacyVisualization()),
                    Widget(visualization=LegacyVisualization(query=LegacyQuery(id="qid1"))),
                ],
            ),
            RedashDashboard("did", "name", "parent", ["qid1"], ["tag1", "tag2"]),
        ),
    ],
)
def test_redash_dashboard_from_sdk_dashboard(sdk_dashboard: SdkRedashDashboard, expected: RedashDashboard) -> None:
    dashboard = RedashDashboard.from_sdk_dashboard(sdk_dashboard)
    assert dashboard == expected


def test_redash_dashboard_crawler_snapshot_persists_dashboards(mock_backend) -> None:
    ws = create_autospec(WorkspaceClient)
    dashboards = [
        SdkRedashDashboard(
            id="did",
            name="name",
            parent="parent",
            tags=["tag1", "tag2"],
            widgets=[
                Widget(visualization=LegacyVisualization(query=LegacyQuery(id="qid1"))),
                Widget(visualization=LegacyVisualization(query=LegacyQuery(id="qid2"))),
            ],
        ),
    ]
    ws.dashboards.list.side_effect = lambda: (dashboard for dashboard in dashboards)  # Expects an iterator
    crawler = RedashDashboardCrawler(ws, mock_backend, "test")

    crawler.snapshot()

    rows = mock_backend.rows_written_for("hive_metastore.test.redash_dashboards", "overwrite")
    assert rows == [Row(id="did", name="name", parent="parent", query_ids=["qid1", "qid2"], tags=["tag1", "tag2"])]
    ws.dashboards.list.assert_called_once()


def test_redash_dashboard_crawler_handles_databricks_error_on_list(caplog, mock_backend) -> None:
    ws = create_autospec(WorkspaceClient)
    ws.dashboards.list.side_effect = PermissionDenied("Missing permission")
    crawler = RedashDashboardCrawler(ws, mock_backend, "test")

    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.assessment.dashboards"):
        crawler.snapshot()

    rows = mock_backend.rows_written_for("hive_metastore.test.redash_dashboards", "overwrite")
    assert len(rows) == 0
    assert "Cannot list Redash dashboards" in caplog.text
    ws.dashboards.list.assert_called_once()


def test_redash_dashboard_crawler_handles_databricks_error_on_iterate(caplog, mock_backend) -> None:
    ws = create_autospec(WorkspaceClient)
    dashboards = [SdkRedashDashboard(id="did1"), SdkRedashDashboard(id="did2")]

    def list_dashboards() -> Iterator[SdkRedashDashboard]:
        for dashboard in dashboards:
            yield dashboard
            raise TooManyRequests("Exceeded API limit")

    ws.dashboards.list.side_effect = list_dashboards
    crawler = RedashDashboardCrawler(ws, mock_backend, "test")

    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.assessment.dashboards"):
        crawler.snapshot()

    rows = mock_backend.rows_written_for("hive_metastore.test.redash_dashboards", "overwrite")
    assert rows == [Row(id="did1", name="UNKNOWN", parent="ORPHAN", query_ids=[], tags=[])]
    assert "Cannot list next Redash dashboards page" in caplog.text
    ws.dashboards.list.assert_called_once()


def test_redash_dashboard_crawler_stops_when_debug_listing_upper_limit_reached(mock_backend) -> None:
    ws = create_autospec(WorkspaceClient)
    dashboards = [SdkRedashDashboard(id="did1"), SdkRedashDashboard(id="did2")]
    ws.dashboards.list.side_effect = lambda: (dashboard for dashboard in dashboards)
    crawler = RedashDashboardCrawler(ws, mock_backend, "test", debug_listing_upper_limit=1)

    crawler.snapshot()

    rows = mock_backend.rows_written_for("hive_metastore.test.redash_dashboards", "overwrite")
    assert rows == [Row(id="did1", name="UNKNOWN", parent="ORPHAN", query_ids=[], tags=[])]
    ws.dashboards.list.assert_called_once()


def test_redash_dashboard_crawler_includes_dashboard_ids(mock_backend) -> None:
    ws = create_autospec(WorkspaceClient)
    ws.dashboards.get.return_value = SdkRedashDashboard(id="did1")
    crawler = RedashDashboardCrawler(ws, mock_backend, "test", include_dashboard_ids=["did1"])

    crawler.snapshot()

    rows = mock_backend.rows_written_for("hive_metastore.test.redash_dashboards", "overwrite")
    assert rows == [Row(id="did1", name="UNKNOWN", parent="ORPHAN", query_ids=[], tags=[])]
    ws.dashboards.get.assert_called_once_with("did1")
    ws.dashboards.list.assert_not_called()


def test_redash_dashboard_crawler_skips_not_found_dashboard_ids(caplog, mock_backend) -> None:
    ws = create_autospec(WorkspaceClient)

    def get_dashboards(dashboard_id: str) -> SdkRedashDashboard:
        if dashboard_id == "did1":
            return SdkRedashDashboard(id="did1")
        raise NotFound(f"Did not find dashboard: {dashboard_id}")

    ws.dashboards.get.side_effect = get_dashboards
    crawler = RedashDashboardCrawler(ws, mock_backend, "test", include_dashboard_ids=["did1", "did2"])

    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.assessment.dashboards"):
        crawler.snapshot()

    rows = mock_backend.rows_written_for("hive_metastore.test.redash_dashboards", "overwrite")
    assert rows == [Row(id="did1", name="UNKNOWN", parent="ORPHAN", query_ids=[], tags=[])]
    assert "Cannot get Redash dashboard: did2" in caplog.messages
    ws.dashboards.get.has_calls([call("did1"), call("did2")])
    ws.dashboards.list.assert_not_called()


def test_redash_dashboard_crawler_snapshot_skips_dashboard_without_id(mock_backend) -> None:
    ws = create_autospec(WorkspaceClient)
    dashboards = [SdkRedashDashboard(id="did1"), SdkRedashDashboard()]  # Second misses dashboard id
    ws.dashboards.list.side_effect = lambda: (dashboard for dashboard in dashboards)  # Expects an iterator
    crawler = RedashDashboardCrawler(ws, mock_backend, "test")

    crawler.snapshot()

    rows = mock_backend.rows_written_for("hive_metastore.test.redash_dashboards", "overwrite")
    assert rows == [Row(id="did1", name="UNKNOWN", parent="ORPHAN", query_ids=[], tags=[])]
    ws.dashboards.list.assert_called_once()


def test_redash_dashboard_crawler_list_queries(mock_backend) -> None:
    ws = create_autospec(WorkspaceClient)
    ws.queries_legacy.list.return_value = [LegacyQuery(id="qid")]
    crawler = RedashDashboardCrawler(ws, mock_backend, "test")

    queries = list(crawler.list_queries())

    assert queries == [LegacyQuery(id="qid")]
    ws.queries_legacy.list.assert_called_once()


def test_redash_dashboard_crawler_list_queries_handles_permission_denied(caplog, mock_backend) -> None:
    ws = create_autospec(WorkspaceClient)
    ws.queries_legacy.list.side_effect = PermissionDenied("Missing permissions")
    crawler = RedashDashboardCrawler(ws, mock_backend, "test")

    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.assessment.dashboards"):
        queries = list(crawler.list_queries())

    assert len(queries) == 0
    assert "Cannot list Redash queries" in caplog.messages
    ws.queries_legacy.list.assert_called_once()


def test_redash_dashboard_crawler_get_query_calls_query_api_get(mock_backend) -> None:
    ws = create_autospec(WorkspaceClient)
    crawler = RedashDashboardCrawler(ws, mock_backend, "test")

    query = crawler.get_query("qid", RedashDashboard("did"))

    assert query is not None
    ws.queries_legacy.get.assert_called_once_with("qid")


def test_redash_dashboard_crawler_get_query_handles_not_found(caplog, mock_backend) -> None:
    ws = create_autospec(WorkspaceClient)
    ws.queries_legacy.get.side_effect = NotFound("Query not found: qid")
    crawler = RedashDashboardCrawler(ws, mock_backend, "test")

    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.assessment.dashboards"):
        query = crawler.get_query("qid", RedashDashboard("did"))

    assert query is None
    assert "Cannot get Redash query: qid" in caplog.messages
    ws.queries_legacy.get.assert_called_once_with("qid")


@pytest.mark.parametrize(
    "sdk_dashboard, expected",
    [
        (SdkLakeviewDashboard(dashboard_id="id"), LakeviewDashboard("id")),
        (
            SdkLakeviewDashboard(
                dashboard_id="did",
                display_name="name",
                parent_path="parent",
                serialized_dashboard=json.dumps(
                    LsqlLakeviewDashboard(
                        datasets=[Dataset("qid1", "SELECT 1"), Dataset("qid2", "SELECT 2")],
                        pages=[],
                    ).as_dict()
                ),
            ),
            LakeviewDashboard("did", "name", "parent", ["qid1", "qid2"]),
        ),
        (
            SdkLakeviewDashboard(
                dashboard_id="did",
                display_name="name",
                parent_path="parent",
                serialized_dashboard=json.dumps(LsqlLakeviewDashboard(datasets=[], pages=[]).as_dict()),
            ),
            LakeviewDashboard("did", "name", "parent", []),
        ),
    ],
)
def test_lakeview_dashboard_from_sdk_dashboard(
    sdk_dashboard: SdkLakeviewDashboard, expected: LakeviewDashboard
) -> None:
    dashboard = LakeviewDashboard.from_sdk_dashboard(sdk_dashboard)
    assert dashboard == expected


def test_lakeview_dashboard_crawler_snapshot_persists_dashboards(mock_backend) -> None:
    ws = create_autospec(WorkspaceClient)
    dashboards = [
        SdkLakeviewDashboard(
            dashboard_id="did",
            display_name="name",
            parent_path="parent",
            serialized_dashboard=json.dumps(
                LsqlLakeviewDashboard(
                    datasets=[Dataset("qid1", "SELECT 1"), Dataset("qid2", "SELECT 2")],
                    pages=[],
                ).as_dict()
            ),
        ),
    ]
    ws.lakeview.list.side_effect = lambda: (dashboard for dashboard in dashboards)  # Expects an iterator
    crawler = LakeviewDashboardCrawler(ws, mock_backend, "test")

    crawler.snapshot()

    rows = mock_backend.rows_written_for("hive_metastore.test.lakeview_dashboards", "overwrite")
    assert rows == [Row(id="did", name="name", parent="parent", query_ids=["qid1", "qid2"])]
    ws.lakeview.list.assert_called_once()


def test_lakeview_dashboard_crawler_handles_databricks_error_on_list(caplog, mock_backend) -> None:
    ws = create_autospec(WorkspaceClient)
    ws.lakeview.list.side_effect = PermissionDenied("Missing permission")
    crawler = LakeviewDashboardCrawler(ws, mock_backend, "test")

    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.assessment.dashboards"):
        crawler.snapshot()

    rows = mock_backend.rows_written_for("hive_metastore.test.lakeview_dashboards", "overwrite")
    assert len(rows) == 0
    assert "Cannot list Lakeview dashboards" in caplog.text
    ws.lakeview.list.assert_called_once()


def test_lakeview_dashboard_crawler_includes_dashboard_ids(mock_backend) -> None:
    ws = create_autospec(WorkspaceClient)
    ws.lakeview.get.return_value = SdkLakeviewDashboard(dashboard_id="did1")
    crawler = LakeviewDashboardCrawler(ws, mock_backend, "test", include_dashboard_ids=["did1"])

    crawler.snapshot()

    rows = mock_backend.rows_written_for("hive_metastore.test.lakeview_dashboards", "overwrite")
    assert rows == [Row(id="did1", name="UNKNOWN", parent="ORPHAN", query_ids=[])]
    ws.lakeview.get.assert_called_once_with("did1")
    ws.lakeview.list.assert_not_called()


def test_lakeview_dashboard_crawler_skips_not_found_dashboard_ids(caplog, mock_backend) -> None:
    ws = create_autospec(WorkspaceClient)

    def get_dashboards(dashboard_id: str) -> SdkLakeviewDashboard:
        if dashboard_id == "did1":
            return SdkLakeviewDashboard(dashboard_id="did1")
        raise NotFound(f"Did not find dashboard: {dashboard_id}")

    ws.lakeview.get.side_effect = get_dashboards
    crawler = LakeviewDashboardCrawler(ws, mock_backend, "test", include_dashboard_ids=["did1", "did2"])

    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.assessment.dashboards"):
        crawler.snapshot()

    rows = mock_backend.rows_written_for("hive_metastore.test.lakeview_dashboards", "overwrite")
    assert rows == [Row(id="did1", name="UNKNOWN", parent="ORPHAN", query_ids=[])]
    assert "Cannot get Lakeview dashboard: did2" in caplog.messages
    ws.lakeview.get.has_calls([call("did1"), call("did2")])
    ws.lakeview.list.assert_not_called()


def test_lakeview_dashboard_crawler_snapshot_skips_dashboard_without_id(mock_backend) -> None:
    ws = create_autospec(WorkspaceClient)
    dashboards = [SdkLakeviewDashboard(dashboard_id="did1"), SdkLakeviewDashboard()]  # Second misses dashboard id
    ws.lakeview.list.side_effect = lambda: (dashboard for dashboard in dashboards)  # Expects an iterator
    crawler = LakeviewDashboardCrawler(ws, mock_backend, "test")

    crawler.snapshot()

    rows = mock_backend.rows_written_for("hive_metastore.test.lakeview_dashboards", "overwrite")
    assert rows == [Row(id="did1", name="UNKNOWN", parent="ORPHAN", query_ids=[])]
    ws.lakeview.list.assert_called_once()


def test_lakeview_dashboard_crawler_list_queries(mock_backend) -> None:
    ws = create_autospec(WorkspaceClient)
    dashboards = [
        SdkLakeviewDashboard(
            serialized_dashboard=json.dumps(
                LsqlLakeviewDashboard(datasets=[Dataset("qid1", "SELECT 42 AS count")], pages=[]).as_dict()
            ),
        ),
    ]
    ws.lakeview.list.side_effect = lambda: (dashboard for dashboard in dashboards)  # Expects an iterator
    crawler = LakeviewDashboardCrawler(ws, mock_backend, "test")

    queries = list(crawler.list_queries())

    assert queries == ["SELECT 42 AS count"]
    ws.lakeview.list.assert_called_once()


def test_lakeview_dashboard_crawler_list_queries_handles_permission_denied(caplog, mock_backend) -> None:
    ws = create_autospec(WorkspaceClient)
    ws.lakeview.list.side_effect = PermissionDenied("Missing permissions")
    crawler = LakeviewDashboardCrawler(ws, mock_backend, "test")

    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.assessment.dashboards"):
        queries = list(crawler.list_queries())

    assert len(queries) == 0
    assert "Cannot list Lakeview dashboards" in caplog.messages
    ws.lakeview.list.assert_called_once()


def test_lakeview_dashboard_crawler_list_queries_handles_corrupted_serialized_dashboard(caplog, mock_backend) -> None:
    ws = create_autospec(WorkspaceClient)
    dashboards = [
        SdkLakeviewDashboard(dashboard_id="did", serialized_dashboard='{"invalid_lakeview": "serialized_dashboard"}')
    ]
    ws.lakeview.list.side_effect = lambda: (dashboard for dashboard in dashboards)  # Expects an iterator
    crawler = LakeviewDashboardCrawler(ws, mock_backend, "test")

    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.assessment.dashboards"):
        queries = list(crawler.list_queries())

    assert queries == []
    assert "Error when parsing Lakeview dashboard: did"
    ws.lakeview.list.assert_called_once()

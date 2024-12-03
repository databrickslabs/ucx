import logging
import json
from unittest.mock import create_autospec
from typing import Iterator

import pytest
from databricks.labs.lsql.lakeview import Dashboard as LsqlLakeviewDashboard, Dataset
from databricks.labs.lsql.backends import Row
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import PermissionDenied, TooManyRequests
from databricks.sdk.service.dashboards import Dashboard as SdkLakeviewDashboard
from databricks.sdk.service.sql import Dashboard as SdkRedashDashboard, LegacyVisualization, LegacyQuery, Widget

from databricks.labs.ucx.assessment.dashboards import LakeviewDashboard, RedashDashboard, RedashDashboardCrawler


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
    assert rows == [
        Row(id="did", name="name", parent="parent", query_ids=["qid1", "qid2"], tags=["tag1", "tag2"])
    ]
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

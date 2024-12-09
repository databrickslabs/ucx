import logging
import json
from collections.abc import Iterator
from unittest.mock import call, create_autospec

import pytest
from databricks.labs.lsql.lakeview import Dashboard as LsqlLakeviewDashboard, Dataset
from databricks.labs.lsql.backends import Row
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, PermissionDenied, TooManyRequests
from databricks.sdk.service.dashboards import Dashboard as SdkLakeviewDashboard
from databricks.sdk.service.sql import Dashboard as SdkRedashDashboard, LegacyVisualization, LegacyQuery, Widget

from databricks.labs.ucx.assessment.dashboards import (
    LakeviewDashboardCrawler,
    Dashboard,
    RedashDashboardCrawler,
    Query,
)


@pytest.mark.parametrize(
    "legacy_query, expected",
    [
        (LegacyQuery(id="qid"), Query("qid")),
        (
            LegacyQuery(id="qid", name="Query", query="SELECT 42 AS count", parent="parent", tags=["tag1", "tag2"]),
            Query("qid", "Query", "parent", "SELECT 42 AS count", ["tag1", "tag2"]),
        ),
    ],
)
def test_query_from_legacy_query(legacy_query: LegacyQuery, expected: Query) -> None:
    query = Query.from_legacy_query(legacy_query)
    assert query == expected


@pytest.mark.parametrize(
    "dataset, parent, expected",
    [
        (Dataset("qid", "SELECT 42 AS count"), None, Query("qid", query="SELECT 42 AS count")),
        (
            Dataset("qid", "SELECT 42 AS count", display_name="Query"),
            "parent",
            Query("qid", "Query", "parent", "SELECT 42 AS count"),
        ),
    ],
)
def test_query_from_lakeview_dataset(dataset: Dataset, parent: str | None, expected: Query) -> None:
    query = Query.from_lakeview_dataset(dataset, parent=parent)
    assert query == expected


@pytest.mark.parametrize(
    "sdk_dashboard, expected",
    [
        (SdkRedashDashboard(id="id"), Dashboard("id")),
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
            Dashboard("did", "name", "parent", ["qid1", "qid2"], ["tag1", "tag2"]),
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
            Dashboard("did", "name", "parent", ["qid1"], ["tag1", "tag2"]),
        ),
    ],
)
def test_redash_dashboard_from_sdk_dashboard(sdk_dashboard: SdkRedashDashboard, expected: Dashboard) -> None:
    dashboard = Dashboard.from_sdk_redash_dashboard(sdk_dashboard)
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
    assert "Cannot list Redash dashboards" in caplog.messages
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
    assert "Cannot list next Redash dashboards page" in caplog.messages
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
    ws.dashboards.get.assert_has_calls([call("did1"), call("did2")])
    ws.dashboards.list.assert_not_called()


def list_legacy_queries() -> list[LegacyQuery]:
    queries = [
        LegacyQuery(id="qid1", name="First query", parent="parent", query="SELECT 42 AS count"),
        LegacyQuery(id="qid2", name="Second query", parent="parent", query="SELECT 21 AS count"),
    ]
    return queries


def get_legacy_query(query_id: str) -> LegacyQuery:
    for query in list_legacy_queries():
        if query.id == query_id:
            return query
    raise NotFound(f"Legacy query: {query_id}")


def test_redash_dashboard_crawler_list_queries_includes_query_ids(mock_backend) -> None:
    ws = create_autospec(WorkspaceClient)
    ws.queries_legacy.list.side_effect = list_legacy_queries
    ws.queries_legacy.get.side_effect = get_legacy_query
    crawler = RedashDashboardCrawler(ws, mock_backend, "test", include_query_ids=["qid1"])

    queries = list(crawler.list_queries())

    assert queries == [Query(id="qid1", name="First query", parent="parent", query="SELECT 42 AS count")]
    ws.queries_legacy.list.assert_not_called()
    ws.queries_legacy.get.assert_called_once()


def test_redash_dashboard_crawler_list_queries_includes_query_ids_from_dashboard(mock_backend) -> None:
    dashboard = Dashboard("did", query_ids=["qid1", "qid2"])
    ws = create_autospec(WorkspaceClient)
    ws.queries_legacy.list.side_effect = list_legacy_queries
    ws.queries_legacy.get.side_effect = get_legacy_query
    crawler = RedashDashboardCrawler(ws, mock_backend, "test", include_query_ids=["qid1"])

    queries = list(crawler.list_queries(dashboard))

    assert queries == [Query(id="qid1", name="First query", parent="parent", query="SELECT 42 AS count")]
    ws.queries_legacy.list.assert_not_called()
    ws.queries_legacy.get.assert_called_once()


def test_redash_dashboard_crawler_skips_not_found_query_ids(caplog, mock_backend) -> None:
    ws = create_autospec(WorkspaceClient)
    ws.queries_legacy.list.side_effect = list_legacy_queries
    ws.queries_legacy.get.side_effect = get_legacy_query
    crawler = RedashDashboardCrawler(ws, mock_backend, "test", include_query_ids=["qid1", "non-existing-id"])

    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.assessment.dashboards"):
        queries = list(crawler.list_queries())

    assert queries == [Query(id="qid1", name="First query", parent="parent", query="SELECT 42 AS count")]
    assert "Cannot get Redash query: non-existing-id" in caplog.messages
    ws.queries_legacy.list.assert_not_called()
    ws.queries_legacy.get.assert_has_calls([call("qid1"), call("non-existing-id")])


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
    ws.queries_legacy.list.return_value = [
        LegacyQuery(id="qid1", name="First query", parent="parent", query="SELECT 42 AS count"),
        LegacyQuery(id="qid2", name="Second query", parent="parent", query="SELECT 21 AS count"),
    ]
    crawler = RedashDashboardCrawler(ws, mock_backend, "test")

    queries = list(crawler.list_queries())

    assert queries == [
        Query("qid1", "First query", "parent", "SELECT 42 AS count"),
        Query("qid2", "Second query", "parent", "SELECT 21 AS count"),
    ]
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


def test_redash_dashboard_crawler_list_queries_from_dashboard(mock_backend) -> None:
    ws = create_autospec(WorkspaceClient)
    ws.queries_legacy.get.return_value = LegacyQuery(
        id="qid", name="Query", parent="parent", query="SELECT 42 AS count"
    )
    crawler = RedashDashboardCrawler(ws, mock_backend, "test")

    queries = list(crawler.list_queries(dashboard=Dashboard("did", query_ids=["qid"])))

    assert queries == [Query("qid", "Query", "parent", "SELECT 42 AS count")]
    ws.queries_legacy.get.assert_called_once_with("qid")


def test_redash_dashboard_crawler_list_queries_handles_not_found(caplog, mock_backend) -> None:
    ws = create_autospec(WorkspaceClient)
    ws.queries_legacy.get.side_effect = NotFound("Query not found: qid")
    crawler = RedashDashboardCrawler(ws, mock_backend, "test")

    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.assessment.dashboards"):
        queries = list(crawler.list_queries(dashboard=Dashboard("did", query_ids=["qid"])))

    assert len(queries) == 0
    assert "Cannot get Redash query: qid" in caplog.messages
    ws.queries_legacy.get.assert_called_once_with("qid")


def test_redash_dashboard_crawler_list_queries_stops_when_debug_listing_upper_limit_reached(mock_backend) -> None:
    ws = create_autospec(WorkspaceClient)
    legacy_queries = [LegacyQuery(id="qid1"), LegacyQuery(id="qid2")]
    ws.queries_legacy.list.side_effect = lambda: (query for query in legacy_queries)
    crawler = RedashDashboardCrawler(ws, mock_backend, "test", debug_listing_upper_limit=1)

    queries = list(crawler.list_queries())

    assert len(queries) == 1
    ws.queries_legacy.list.assert_called_once()


@pytest.mark.parametrize(
    "sdk_dashboard, expected",
    [
        (SdkLakeviewDashboard(dashboard_id="id"), Dashboard("id")),
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
            Dashboard("did", "name", "parent", ["qid1", "qid2"]),
        ),
        (
            SdkLakeviewDashboard(
                dashboard_id="did",
                display_name="name",
                parent_path="parent",
                serialized_dashboard=json.dumps(LsqlLakeviewDashboard(datasets=[], pages=[]).as_dict()),
            ),
            Dashboard("did", "name", "parent", []),
        ),
    ],
)
def test_lakeview_dashboard_from_sdk_dashboard(sdk_dashboard: SdkLakeviewDashboard, expected: Dashboard) -> None:
    dashboard = Dashboard.from_sdk_lakeview_dashboard(sdk_dashboard)
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
    assert rows == [Row(id="did", name="name", parent="parent", query_ids=["qid1", "qid2"], tags=[])]
    ws.lakeview.list.assert_called_once()


def test_lakeview_dashboard_crawler_handles_databricks_error_on_list(caplog, mock_backend) -> None:
    ws = create_autospec(WorkspaceClient)
    ws.lakeview.list.side_effect = PermissionDenied("Missing permission")
    crawler = LakeviewDashboardCrawler(ws, mock_backend, "test")

    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.assessment.dashboards"):
        crawler.snapshot()

    rows = mock_backend.rows_written_for("hive_metastore.test.lakeview_dashboards", "overwrite")
    assert len(rows) == 0
    assert "Cannot list Lakeview dashboards" in caplog.messages
    ws.lakeview.list.assert_called_once()


def test_lakeview_dashboard_crawler_includes_dashboard_ids(mock_backend) -> None:
    ws = create_autospec(WorkspaceClient)
    ws.lakeview.get.return_value = SdkLakeviewDashboard(dashboard_id="did1")
    crawler = LakeviewDashboardCrawler(ws, mock_backend, "test", include_dashboard_ids=["did1"])

    crawler.snapshot()

    rows = mock_backend.rows_written_for("hive_metastore.test.lakeview_dashboards", "overwrite")
    assert rows == [Row(id="did1", name="UNKNOWN", parent="ORPHAN", query_ids=[], tags=[])]
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
    assert rows == [Row(id="did1", name="UNKNOWN", parent="ORPHAN", query_ids=[], tags=[])]
    assert "Cannot get Lakeview dashboard: did2" in caplog.messages
    ws.lakeview.get.assert_has_calls([call("did1"), call("did2")])
    ws.lakeview.list.assert_not_called()


def test_lakeview_dashboard_crawler_list_queries_includes_query_ids(mock_backend) -> None:
    ws = create_autospec(WorkspaceClient)
    datasets = [
        Dataset("qid1", "SELECT 42 AS count", "First query"),
        Dataset("qid2", "SELECT 21 AS count", "Second query"),
    ]
    dashboard = SdkLakeviewDashboard(
        dashboard_id="parent",
        serialized_dashboard=json.dumps(LsqlLakeviewDashboard(datasets=datasets, pages=[]).as_dict()),
    )
    ws.lakeview.list.return_value = [dashboard]
    crawler = LakeviewDashboardCrawler(ws, mock_backend, "test", include_query_ids=["qid1"])

    queries = list(crawler.list_queries())

    assert queries == [Query(id="qid1", name="First query", parent="parent", query="SELECT 42 AS count")]
    ws.lakeview.list.assert_called_once()
    ws.lakeview.get.assert_not_called()


def test_lakeview_dashboard_crawler_list_queries_includes_query_ids_from_dashboard(mock_backend) -> None:
    ws = create_autospec(WorkspaceClient)
    datasets = [
        Dataset("qid1", "SELECT 42 AS count", "First query"),
        Dataset("qid2", "SELECT 21 AS count", "Second query"),
    ]
    dashboard = SdkLakeviewDashboard(
        dashboard_id="parent",
        serialized_dashboard=json.dumps(LsqlLakeviewDashboard(datasets=datasets, pages=[]).as_dict()),
    )
    ws.lakeview.get.return_value = dashboard
    crawler = LakeviewDashboardCrawler(ws, mock_backend, "test", include_query_ids=["qid1"])

    queries = list(crawler.list_queries(Dashboard("parent")))

    assert queries == [Query(id="qid1", name="First query", parent="parent", query="SELECT 42 AS count")]
    ws.lakeview.list.assert_not_called()
    ws.lakeview.get.assert_called_once_with("parent")


def test_lakeview_dashboard_crawler_snapshot_skips_dashboard_without_id(mock_backend) -> None:
    ws = create_autospec(WorkspaceClient)
    dashboards = [SdkLakeviewDashboard(dashboard_id="did1"), SdkLakeviewDashboard()]  # Second misses dashboard id
    ws.lakeview.list.side_effect = lambda: (dashboard for dashboard in dashboards)  # Expects an iterator
    crawler = LakeviewDashboardCrawler(ws, mock_backend, "test")

    crawler.snapshot()

    rows = mock_backend.rows_written_for("hive_metastore.test.lakeview_dashboards", "overwrite")
    assert rows == [Row(id="did1", name="UNKNOWN", parent="ORPHAN", query_ids=[], tags=[])]
    ws.lakeview.list.assert_called_once()


def test_lakeview_dashboard_crawler_list_queries(mock_backend) -> None:
    ws = create_autospec(WorkspaceClient)
    dashboards = [
        SdkLakeviewDashboard(
            dashboard_id="parent",
            serialized_dashboard=json.dumps(
                LsqlLakeviewDashboard(datasets=[Dataset("qid1", "SELECT 42 AS count", "Query")], pages=[]).as_dict()
            ),
        ),
    ]
    ws.lakeview.list.side_effect = lambda: (dashboard for dashboard in dashboards)  # Expects an iterator
    crawler = LakeviewDashboardCrawler(ws, mock_backend, "test")

    queries = list(crawler.list_queries())

    assert queries == [Query("qid1", "Query", "parent", "SELECT 42 AS count")]
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
    dashboards = [SdkLakeviewDashboard(dashboard_id="did", serialized_dashboard='{"invalid": "json}')]
    ws.lakeview.list.side_effect = lambda: (dashboard for dashboard in dashboards)  # Expects an iterator
    crawler = LakeviewDashboardCrawler(ws, mock_backend, "test")

    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.assessment.dashboards"):
        queries = list(crawler.list_queries())

    assert len(queries) == 0
    assert "Error when parsing Lakeview dashboard: did" in caplog.messages
    ws.lakeview.list.assert_called_once()


def test_lakeview_dashboard_crawler_list_queries_calls_query_api_get(mock_backend) -> None:
    ws = create_autospec(WorkspaceClient)
    dashboard = SdkLakeviewDashboard(
        dashboard_id="parent",
        serialized_dashboard=json.dumps(
            LsqlLakeviewDashboard(datasets=[Dataset("qid", "SELECT 42 AS count", "Query")], pages=[]).as_dict()
        ),
    )
    ws.lakeview.get.return_value = dashboard
    crawler = LakeviewDashboardCrawler(ws, mock_backend, "test")

    queries = list(crawler.list_queries(Dashboard("did")))

    assert queries == [Query("qid", "Query", "parent", "SELECT 42 AS count")]
    ws.lakeview.get.assert_called_once_with("did")


def test_lakeview_dashboard_crawler_list_queries_handles_not_found(caplog, mock_backend) -> None:
    ws = create_autospec(WorkspaceClient)
    ws.lakeview.get.side_effect = NotFound("Query not found: qid")
    crawler = LakeviewDashboardCrawler(ws, mock_backend, "test")

    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.assessment.dashboards"):
        queries = list(crawler.list_queries(Dashboard("did")))

    assert len(queries) == 0
    assert "Cannot get Lakeview dashboard: did" in caplog.messages
    ws.lakeview.get.assert_called_once_with("did")

from databricks.sdk.service.sql import Dashboard as SdkRedashDashboard
from databricks.sdk.service.dashboards import Dashboard as SdkLakeviewDashboard

from databricks.labs.ucx.assessment.dashboards import (
    LakeviewDashboardCrawler,
    Dashboard,
    RedashDashboardCrawler,
)


def test_redash_dashboard_crawler_crawls_dashboards(ws, make_dashboard, inventory_schema, sql_backend) -> None:
    dashboard: SdkRedashDashboard = make_dashboard()
    crawler = RedashDashboardCrawler(ws, sql_backend, inventory_schema)

    dashboards = list(crawler.snapshot())

    assert len(dashboards) >= 1
    assert dashboard.id in {d.id for d in dashboards}, f"Missing dashboard: {dashboard.id}"


def test_redash_dashboard_crawler_crawls_dashboard(ws, make_dashboard, inventory_schema, sql_backend) -> None:
    dashboard: SdkRedashDashboard = make_dashboard()
    assert dashboard.id
    make_dashboard()  # Ignore second dashboard
    crawler = RedashDashboardCrawler(ws, sql_backend, inventory_schema, include_dashboard_ids=[dashboard.id])

    dashboards = list(crawler.snapshot())

    assert len(dashboards) == 1
    assert dashboards[0] == Dashboard(id=dashboard.id)


def test_redash_dashboard_crawler_crawls_dashboards_with_debug_listing_upper_limit(
    ws, make_dashboard, inventory_schema, sql_backend
) -> None:
    for _ in range(2):  # Create two dashboards, expect one to be snapshotted due to upper limit below
        make_dashboard()
    crawler = RedashDashboardCrawler(ws, sql_backend, inventory_schema, debug_listing_upper_limit=1)

    dashboards = list(crawler.snapshot())

    assert len(dashboards) == 1


def test_lakeview_dashboard_crawler_crawls_dashboards(
    ws, make_lakeview_dashboard, inventory_schema, sql_backend
) -> None:
    dashboard: SdkLakeviewDashboard = make_lakeview_dashboard()
    crawler = LakeviewDashboardCrawler(ws, sql_backend, inventory_schema)

    dashboards = list(crawler.snapshot())

    assert len(dashboards) >= 1
    assert dashboard.dashboard_id in {d.id for d in dashboards}, f"Missing dashboard: {dashboard.dashboard_id}"


def test_lakeview_dashboard_crawler_crawls_dashboard(
    ws, make_lakeview_dashboard, inventory_schema, sql_backend
) -> None:
    dashboard: SdkLakeviewDashboard = make_lakeview_dashboard()
    assert dashboard.dashboard_id
    make_lakeview_dashboard()  # Ignore second dashboard
    crawler = LakeviewDashboardCrawler(
        ws, sql_backend, inventory_schema, include_dashboard_ids=[dashboard.dashboard_id]
    )

    dashboards = list(crawler.snapshot())

    assert len(dashboards) == 1
    assert dashboards[0] == Dashboard(id=dashboard.dashboard_id)

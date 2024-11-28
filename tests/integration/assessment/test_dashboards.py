from databricks.sdk.service.sql import Dashboard as SdkRedashDashboard
from databricks.sdk.service.dashboards import Dashboard as SdkLakeviewDashboard

from databricks.labs.ucx.assessment.dashboards import LakeviewDashboard, LakeviewDashboardCrawler, RedashDashboard, RedashDashBoardCrawler


def test_redash_dashboard_crawler_crawls_dashboards(ws, make_dashboard, inventory_schema, sql_backend) -> None:
    dashboard: SdkRedashDashboard = make_dashboard()
    crawler = RedashDashBoardCrawler(ws, sql_backend, inventory_schema)

    dashboards = crawler.snapshot()

    assert len(dashboards) >= 1
    assert dashboard.id in {d.id for d in dashboards}, f"Missing dashboard: {dashboard.id}"


def test_redash_dashboard_crawler_crawls_dashboard(ws, make_dashboard, inventory_schema, sql_backend) -> None:
    dashboard: SdkRedashDashboard = make_dashboard()
    make_dashboard()  # Ignore second dashboard
    crawler = RedashDashBoardCrawler(ws, sql_backend, inventory_schema, include_dashboard_ids=[dashboard.id])

    dashboards = crawler.snapshot()

    assert len(dashboards) == 1
    assert dashboards[0] == RedashDashboard(id=dashboard.id)


def test_lakeview_dashboard_crawler_crawls_dashboards(ws, make_lakeview_dashboard, inventory_schema, sql_backend) -> None:
    dashboard: SdkLakeviewDashboard = make_lakeview_dashboard()
    crawler = LakeviewDashboardCrawler(ws, sql_backend, inventory_schema)

    dashboards = crawler.snapshot()

    assert len(dashboards) >= 1
    assert dashboard.dashboard_id in {d.id for d in dashboards}, f"Missing dashboard: {dashboard.id}"


def test_lakeview_dashboard_crawler_crawls_dashboard(ws, make_lakeview_dashboard, inventory_schema, sql_backend) -> None:
    dashboard: SdkLakeviewDashboard = make_lakeview_dashboard()
    make_lakeview_dashboard()  # Ignore second dashboard
    crawler = LakeviewDashboardCrawler(ws, sql_backend, inventory_schema, include_dashboard_ids=[dashboard.dashboard_id])

    dashboards = crawler.snapshot()

    assert len(dashboards) == 1
    assert dashboards[0] == LakeviewDashboard(id=dashboard.dashboard_id)

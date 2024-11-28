from databricks.sdk.service.sql import Dashboard as SqlDashboard

from databricks.labs.ucx.assessment.dashboards import Dashboard, RedashDashBoardCrawler


def test_redash_dashboard_crawler_crawls_dashboards(ws, make_dashboard, inventory_schema, sql_backend) -> None:
    dashboard: SqlDashboard = make_dashboard()
    crawler = RedashDashBoardCrawler(ws, sql_backend, inventory_schema)

    dashboards = crawler.snapshot()

    assert len(dashboards) >= 1
    assert dashboard.id in {d.id for d in dashboards}, f"Missing dashboard: {dashboard.id}"


def test_redash_dashboard_crawler_crawls_dashboard(ws, make_dashboard, inventory_schema, sql_backend) -> None:
    dashboard: SqlDashboard = make_dashboard()
    make_dashboard()  # Ignore second dashboard
    crawler = RedashDashBoardCrawler(ws, sql_backend, inventory_schema, include_dashboard_ids=[dashboard.id])

    dashboards = crawler.snapshot()

    assert len(dashboards) == 1
    assert dashboards[0] == Dashboard(id=dashboard.id)

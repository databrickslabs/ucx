from databricks.sdk.service.sql import Dashboard

from databricks.labs.ucx.assessment.dashboards import RedashDashBoardCrawler


def test_redash_dashboard_crawler_crawls_dashboard(ws, make_dashboard, inventory_schema, sql_backend) -> None:
    dashboard: Dashboard = make_dashboard()
    job_crawler = RedashDashBoardCrawler(ws, sql_backend, inventory_schema)

    dashboards = job_crawler.snapshot()

    assert len(dashboards) >= 1
    assert dashboard.id in {d.id for d in dashboards}, f"Missing dashboard: {dashboard.id}"

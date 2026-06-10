from databricks.sdk.service.dashboards import Dashboard as SdkLakeviewDashboard

from databricks.labs.ucx.assessment.dashboards import (
    LakeviewDashboardCrawler,
    Dashboard,
)


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

    assert dashboards == [Dashboard.from_sdk_lakeview_dashboard(dashboard)]


def test_lakeview_dashboard_ownership_is_me(runtime_ctx, make_lakeview_dashboard) -> None:
    """Lakeview dashboard do not have a `creator` field, thus we fall back on the parent workspace path owner"""
    sdk_lakeview_dashboard = make_lakeview_dashboard()
    dashboard = Dashboard.from_sdk_lakeview_dashboard(sdk_lakeview_dashboard)

    owner = runtime_ctx.dashboard_ownership.owner_of(dashboard)

    current_user = runtime_ctx.workspace_client.current_user.me()
    assert owner == current_user.user_name

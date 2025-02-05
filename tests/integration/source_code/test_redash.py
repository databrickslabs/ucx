import datetime as dt

from databricks.sdk.retries import retried

from databricks.labs.ucx.source_code.linters.redash import Redash
from databricks.sdk.service.sql import Dashboard


def test_migrate_dashboards_sets_migration_tags(installation_ctx) -> None:
    query_in_dashboard, query_outside_dashboard = installation_ctx.make_query(), installation_ctx.make_query()
    assert query_in_dashboard.id and query_outside_dashboard.id, "Query from fixture misses id"
    dashboard: Dashboard = installation_ctx.make_dashboard(query=query_in_dashboard)
    assert dashboard.id, "Dashboard from fixture misses id"
    installation_ctx.workspace_installation.run()

    installation_ctx.redash.migrate_dashboards(dashboard.id)

    @retried(on=[ValueError], timeout=dt.timedelta(seconds=90))
    def wait_for_migrated_tag_in_dashboard(dashboard_id: str) -> None:
        dashboard_latest = installation_ctx.workspace_client.dashboards.get(dashboard_id)
        if Redash.MIGRATED_TAG not in (dashboard_latest.tags or []):
            raise ValueError(f"Missing group migration tag in dashboard: {dashboard_id}")

    wait_for_migrated_tag_in_dashboard(dashboard.id)

    query_migrated = installation_ctx.workspace_client.queries.get(query_in_dashboard.id)
    assert Redash.MIGRATED_TAG in (query_migrated.tags or [])

    query_not_migrated = installation_ctx.workspace_client.queries.get(query_outside_dashboard.id)
    assert Redash.MIGRATED_TAG not in (query_not_migrated.tags or [])

    installation_ctx.redash_crawler.snapshot(force_refresh=True)  # Update the dashboard tags
    installation_ctx.redash.revert_dashboards(dashboard.id)  # Revert removes migrated tag

    @retried(on=[ValueError], timeout=dt.timedelta(seconds=90))
    def wait_for_migrated_tag_not_in_dashboard(dashboard_id: str) -> None:
        dashboard_latest = installation_ctx.workspace_client.dashboards.get(dashboard_id)
        if Redash.MIGRATED_TAG in (dashboard_latest.tags or []):
            raise ValueError(f"Group migration tag still in dashboard: {dashboard_id}")

    wait_for_migrated_tag_not_in_dashboard(dashboard.id)

    query_reverted = installation_ctx.workspace_client.queries.get(query_in_dashboard.id)
    assert Redash.MIGRATED_TAG not in (query_reverted.tags or [])

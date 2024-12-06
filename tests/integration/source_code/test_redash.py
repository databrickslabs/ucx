from databricks.labs.ucx.source_code.redash import Redash
from databricks.sdk.service.sql import Dashboard


def test_migrate_dashboards_sets_migration_tags(installation_ctx) -> None:
    query_in_dashboard, query_outside_dashboard = installation_ctx.make_query(), installation_ctx.make_query()
    assert query_in_dashboard.id and query_outside_dashboard.id, "Query from fixture misses id"
    dashboard: Dashboard = installation_ctx.make_dashboard(query=query_in_dashboard)
    assert dashboard.id, "Dashboard from fixture misses id"
    installation_ctx.workspace_installation.run()

    installation_ctx.redash.migrate_dashboards(dashboard.id)

    query_migrated = installation_ctx.workspace_client.queries.get(query_in_dashboard.id)
    assert Redash.MIGRATED_TAG in (query_migrated.tags or [])

    query_not_migrated = installation_ctx.workspace_client.queries.get(query_outside_dashboard.id)
    assert Redash.MIGRATED_TAG not in (query_not_migrated.tags or [])

    installation_ctx.redash.revert_dashboards(dashboard.id)  # Revert removes migrated tag

    query_reverted = installation_ctx.workspace_client.queries.get(query_in_dashboard.id)
    assert Redash.MIGRATED_TAG not in (query_reverted.tags or [])

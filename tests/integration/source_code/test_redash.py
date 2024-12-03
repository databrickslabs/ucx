from databricks.labs.ucx.source_code.redash import Redash
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Dashboard

from ..conftest import MockInstallationContext


def test_fix_dashboard(ws: WorkspaceClient, installation_ctx: MockInstallationContext, make_dashboard, make_query):
    query_in_dashboard, query_outside_dashboard = make_query(), make_query()
    assert query_in_dashboard.id and query_outside_dashboard.id, "Query from fixture misses id"
    dashboard: Dashboard = installation_ctx.make_dashboard(query=query_in_dashboard)
    assert dashboard.id, "Dashboard from fixture misses id"
    installation_ctx.workspace_installation.run()

    installation_ctx.redash.migrate_dashboards(dashboard.id)

    query_migrated = installation_ctx.workspace_client.queries.get(query_in_dashboard.id)
    assert Redash.MIGRATED_TAG in (query_migrated.tags or [])

    query_not_migrated = ws.queries.get(query_outside_dashboard.id)
    assert Redash.MIGRATED_TAG not in (query_not_migrated.tags or [])

    installation_ctx.redash.revert_dashboards(dashboard.id)
    query_reverted = installation_ctx.workspace_client.queries.get(query_in_dashboard.id)
    assert Redash.MIGRATED_TAG in (query_reverted.tags or [])

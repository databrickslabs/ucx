from databricks.labs.ucx.source_code.redash import Redash
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import Query, Dashboard

from ..conftest import MockInstallationContext


def test_fix_dashboard(ws: WorkspaceClient, installation_ctx: MockInstallationContext, make_dashboard, make_query):
    dashboard: Dashboard = make_dashboard()
    another_query: Query = make_query()
    installation_ctx.workspace_installation.run()
    installation_ctx.redash.migrate_dashboards(dashboard.id)
    # make sure the query is marked as migrated
    queries = Redash.get_queries_from_dashboard(dashboard)
    for query in queries:
        assert query.id is not None
        content = ws.queries.get(query.id)
        assert content.tags is not None and Redash.MIGRATED_TAG in content.tags

    # make sure a different query does not get migrated
    assert another_query.id is not None
    another_query = ws.queries.get(another_query.id)
    assert another_query.tags is not None and len(another_query.tags) == 1
    assert Redash.MIGRATED_TAG not in another_query.tags

    # revert the dashboard, make sure the query has only a single tag
    installation_ctx.redash.revert_dashboards(dashboard.id)
    for query in queries:
        assert query.id is not None
        content = ws.queries.get(query.id)
        assert content.tags is not None and len(content.tags) == 1

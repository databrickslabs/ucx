from databricks.labs.ucx.source_code.redash import Redash
from databricks.sdk.service.sql import Query, Dashboard


def test_fix_dashboard(ws, installation_ctx, make_dashboard, make_query):
    dashboard: Dashboard = make_dashboard()
    another_query: Query = make_query()
    installation_ctx.workspace_installation.run()
    installation_ctx.redash.migrate_dashboards(dashboard.id)
    # make sure the query is marked as migrated
    queries = Redash.get_queries_from_dashboard(dashboard)
    for query in queries:
        content = ws.queries.get(query.id)
        assert Redash.MIGRATED_TAG in content.tags

    # make sure a different query does not get migrated
    another_query = ws.queries.get(another_query.id)
    assert len(another_query.tags) == 1
    assert Redash.MIGRATED_TAG not in another_query.tags

    # revert the dashboard, make sure the query has only a single tag
    installation_ctx.redash.revert_dashboards(dashboard.id)
    for query in queries:
        content = ws.queries.get(query.id)
        assert len(content.tags) == 1

    installation_ctx.redash.delete_backup_queries(installation_ctx.prompts)

from databricks.labs.ucx.source_code.redash import Redash


def test_fix_dashboard(ws, runtime_ctx, make_dashboard, make_query):
    dashboard, query = make_dashboard()
    another_query = make_query()
    runtime_ctx.redash.fix_dashboard(dashboard.id)
    # make sure the query is marked as migrated
    query = ws.queries.get(query.id)
    assert Redash.MIGRATED_TAG in query.tags

    # make sure a different query does not get migrated
    another_query = ws.queries.get(another_query.id)
    assert len(another_query.tags) == 0

    # revert the dashboard, make sure the query has no tag
    runtime_ctx.redash.revert_dashboard(dashboard.id)
    query = ws.queries.get(query.id)
    assert len(query.tags) == 0

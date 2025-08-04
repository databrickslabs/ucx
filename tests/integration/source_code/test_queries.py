import pytest
from databricks.labs.lsql.backends import Row

from databricks.labs.ucx.source_code.base import DirectFsAccess, LineageAtom, UsedTable

def test_query_linter_lints_queries_and_stores_dfsas_and_tables(simple_ctx) -> None:
    query_with_dfsa = simple_ctx.make_query(sql_query="SELECT * from csv.`dbfs://some_folder/some_file.csv`")
    # Lakeview dashboard expects a string, not a legacy query

    simple_ctx.query_linter.refresh_report()

    problems = list(simple_ctx.sql_backend.fetch("SELECT * FROM query_problems", schema=simple_ctx.inventory_database))
    assert problems == [
        Row(
            dashboard_id=None,
            dashboard_parent=None,
            dashboard_name=None,
            query_id=query_with_dfsa.id,
            query_parent=query_with_dfsa.parent,
            query_name=query_with_dfsa.name,
            code='direct-filesystem-access-in-sql-query',
            message='The use of direct filesystem references is deprecated: dbfs://some_folder/some_file.csv',
        )
    ]

    dfsas = list(simple_ctx.directfs_access_crawler_for_queries.snapshot())
    # By comparing the element instead of the list the `field(compare=False)` of the dataclass attributes take effect
    assert dfsas == [
        DirectFsAccess(
            source_id=f"no-dashboard-id/{query_with_dfsa.id}",
            source_lineage=[
                LineageAtom(
                    object_type="QUERY",
                    object_id=f"no-dashboard-id/{query_with_dfsa.id}",
                    other={"name": query_with_dfsa.name},
                )],
            path="dbfs://some_folder/some_file.csv",
            is_read=True,
            is_write=False,
        )
    ]

    used_tables = list(simple_ctx.used_tables_crawler_for_queries.snapshot())
    # By comparing the element instead of the list the `field(compare=False)` of the dataclass attributes take effect
    # The "query" in the source and object id, and "count" in the name are hardcoded in the
    # `make_lakeview_dashboard` fixture
    assert not used_tables

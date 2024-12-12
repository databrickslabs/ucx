from databricks.labs.lsql.backends import Row

from databricks.labs.ucx.source_code.base import DirectFsAccess, LineageAtom, UsedTable


def test_query_linter_lints_queries_and_stores_dfsas_and_tables(simple_ctx) -> None:
    query_with_dfsa = simple_ctx.make_query(sql_query="SELECT * from csv.`dbfs://some_folder/some_file.csv`")
    dashboard_with_dfsa = simple_ctx.make_dashboard(query=query_with_dfsa)
    # Lakeview dashboard expects a string, not a legacy query
    dashboard_with_used_table = simple_ctx.make_lakeview_dashboard(query="SELECT * FROM some_schema.some_table")

    simple_ctx.query_linter.refresh_report()

    problems = list(simple_ctx.sql_backend.fetch("SELECT * FROM query_problems", schema=simple_ctx.inventory_database))
    assert problems == [
        Row(
            dashboard_id=dashboard_with_dfsa.id,
            dashboard_parent=dashboard_with_dfsa.parent,
            dashboard_name=dashboard_with_dfsa.name,
            query_id=query_with_dfsa.id,
            query_parent=query_with_dfsa.parent,
            query_name=query_with_dfsa.name,
            code='direct-filesystem-access-in-sql-query',
            message='The use of direct filesystem references is deprecated: dbfs://some_folder/some_file.csv',
        )
    ]

    dfsas = list(simple_ctx.directfs_access_crawler_for_queries.snapshot())
    # By comparing the element instead of the list the `field(compare=False)` of the dataclass attributes take effect
    assert dfsas[0] == [
        DirectFsAccess(
            source_id=f"{dashboard_with_dfsa.id}/{query_with_dfsa.id}",
            source_lineage=[
                LineageAtom(
                    object_type="DASHBOARD",
                    object_id=dashboard_with_dfsa.id,
                    other={"parent": dashboard_with_dfsa.parent, "name": dashboard_with_dfsa.name},
                ),
                LineageAtom(
                    object_type="QUERY",
                    object_id=f"{dashboard_with_dfsa.id}/{query_with_dfsa.id}",
                    other={"name": query_with_dfsa.name},
                ),
            ],
            path="dbfs://some_folder/some_file.csv",
            is_read=True,
            is_write=False,
        )
    ]

    used_tables = list(simple_ctx.used_tables_crawler_for_queries.snapshot())
    # By comparing the element instead of the list the `field(compare=False)` of the dataclass attributes take effect
    # The "query" in the source and object id, and "count" in the name are hardcoded in the
    # `make_lakeview_dashboard` fixture
    assert used_tables == [
        UsedTable(
            source_id=f"{dashboard_with_used_table.dashboard_id}/query",
            source_lineage=[
                LineageAtom(
                    object_type="DASHBOARD",
                    object_id=dashboard_with_used_table.dashboard_id,
                    other={
                        "parent": dashboard_with_used_table.parent_path,
                        "name": dashboard_with_used_table.display_name,
                    },
                ),
                LineageAtom(
                    object_type="QUERY",
                    object_id=f"{dashboard_with_used_table.dashboard_id}/query",
                    other={"name": "count"},
                ),
            ],
            catalog_name="hive_metastore",
            schema_name="some_schema",
            table_name="some_table",
            is_read=True,
            is_write=False,
        )
    ]

from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationIndex
from databricks.labs.ucx.source_code.directfs_access import DirectFsAccessCrawler
from databricks.labs.ucx.source_code.queries import QueryLinter
from databricks.labs.ucx.source_code.used_table import UsedTablesCrawler


def test_query_linter_lints_queries_and_stores_dfsas_and_tables(simple_ctx, sql_backend, make_query, make_dashboard):
    queries = [make_query(sql_query="SELECT * from csv.`dbfs://some_folder/some_file.csv`")]
    dashboards = [make_dashboard(query=queries[0])]
    queries.append(make_query(sql_query="SELECT * from some_schema.some_table"))
    dashboards.append(make_dashboard(query=queries[1]))
    linter = QueryLinter(
        sql_backend,
        simple_ctx.inventory_database,
        TableMigrationIndex([]),
        simple_ctx.directfs_access_crawler_for_queries,
        simple_ctx.used_tables_crawler_for_queries,
        [],
    )
    linter.refresh_report()
    all_problems = sql_backend.fetch("SELECT * FROM query_problems", schema=simple_ctx.inventory_database)
    problems = [row for row in all_problems if row["query_name"] == queries[0].name]
    assert len(problems) == 1
    dfsa_crawler = DirectFsAccessCrawler.for_queries(sql_backend, simple_ctx.inventory_database)
    all_dfsas = dfsa_crawler.snapshot()
    source_id = f"{dashboards[0].id}/{queries[0].id}"
    dfsas = [dfsa for dfsa in all_dfsas if dfsa.source_id == source_id]
    assert len(dfsas) == 1
    assert len(dfsas[0].source_lineage) == 2
    lineage = dfsas[0].source_lineage[0]
    assert lineage.object_type == "DASHBOARD"
    assert lineage.object_id == dashboards[0].id
    assert lineage.other
    assert lineage.other.get("parent", None) == dashboards[0].parent
    assert lineage.other.get("name", None) == dashboards[0].name
    lineage = dfsas[0].source_lineage[1]
    assert lineage.object_type == "QUERY"
    assert lineage.object_id == source_id
    assert lineage.other
    assert lineage.other.get("name", None) == queries[0].name
    used_tables_crawler = UsedTablesCrawler.for_queries(sql_backend, simple_ctx.inventory_database)
    all_tables = used_tables_crawler.snapshot()
    source_id = f"{dashboards[1].id}/{queries[1].id}"
    tables = [table for table in all_tables if table.source_id == source_id]
    assert len(tables) == 1
    assert len(tables[0].source_lineage) == 2
    lineage = tables[0].source_lineage[0]
    assert lineage.object_type == "DASHBOARD"
    assert lineage.object_id == dashboards[1].id
    assert lineage.other
    assert lineage.other.get("parent", None) == dashboards[1].parent
    assert lineage.other.get("name", None) == dashboards[1].name
    lineage = tables[0].source_lineage[1]
    assert lineage.object_type == "QUERY"
    assert lineage.object_id == source_id
    assert lineage.other
    assert lineage.other.get("name", None) == queries[1].name

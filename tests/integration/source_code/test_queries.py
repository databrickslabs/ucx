from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationIndex
from databricks.labs.ucx.source_code.directfs_access import DirectFsAccessCrawler
from databricks.labs.ucx.source_code.queries import QueryLinter


def test_query_linter_lints_queries_and_stores_dfsas(simple_ctx, ws, sql_backend, make_query, make_dashboard):
    query = make_query(query="SELECT * from csv.`dbfs://some_folder/some_file.csv`")
    dashboard = make_dashboard(query=query)
    linter = QueryLinter(ws, TableMigrationIndex([]), simple_ctx.directfs_access_crawler_for_queries)
    linter.refresh_report(sql_backend, simple_ctx.inventory_database)
    all_problems = sql_backend.fetch("SELECT * FROM query_problems", schema=simple_ctx.inventory_database)
    problems = [row for row in all_problems if row["query_name"] == query.name]
    assert len(problems) == 1
    crawler = DirectFsAccessCrawler.for_queries(sql_backend, simple_ctx.inventory_database)
    all_dfsas = crawler.snapshot()
    dfsas = [dfsa for dfsa in all_dfsas if dfsa.source_id == query.id]
    assert len(dfsas) == 1

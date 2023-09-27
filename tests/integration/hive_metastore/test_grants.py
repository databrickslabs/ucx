import logging
import os

from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.framework.crawlers import StatementExecutionBackend
from databricks.labs.ucx.hive_metastore import GrantsCrawler, TablesCrawler

logger = logging.getLogger(__name__)


def test_all_grants_in_databases(ws: WorkspaceClient, sql_exec, make_catalog, make_schema, make_table, make_group):
    warehouse_id = os.environ["TEST_DEFAULT_WAREHOUSE_ID"]

    group_a = make_group()
    group_b = make_group()
    schema_a = make_schema()
    schema_b = make_schema()
    table_a = make_table(schema=schema_a)
    table_b = make_table(schema=schema_b)

    sql_exec(f"GRANT USAGE ON SCHEMA default TO `{group_a.display_name}`")
    sql_exec(f"GRANT USAGE ON SCHEMA default TO `{group_b.display_name}`")
    sql_exec(f"GRANT SELECT ON TABLE {table_a} TO `{group_a.display_name}`")
    sql_exec(f"GRANT SELECT ON TABLE {table_b} TO `{group_b.display_name}`")
    sql_exec(f"GRANT MODIFY ON SCHEMA {schema_b} TO `{group_b.display_name}`")

    inventory_schema = make_schema(catalog="hive_metastore")
    _, inventory_schema = inventory_schema.split(".")

    backend = StatementExecutionBackend(ws, warehouse_id)
    tables = TablesCrawler(backend)
    grants = GrantsCrawler(tables)

    all_grants = {}
    for grant in grants.snapshot():
        logging.info(f"grant:\n{grant}\n  hive: {grant.hive_grant_sql()}\n  uc: {grant.uc_grant_sql()}")
        all_grants[f"{grant.principal}.{grant.object_key}"] = grant.action_type

    assert len(all_grants) >= 3, "must have at least three grants"
    assert all_grants[f"{group_a.display_name}.{table_a}"] == "SELECT"
    assert all_grants[f"{group_b.display_name}.{table_b}"] == "SELECT"
    assert all_grants[f"{group_b.display_name}.{schema_b}"] == "MODIFY"

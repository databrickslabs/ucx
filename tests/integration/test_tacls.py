import os

from databricks.labs.ucx.providers.client import ImprovedWorkspaceClient
from databricks.labs.ucx.providers.logger import logger
from databricks.labs.ucx.toolkits.table_acls import TaclToolkit


def test_describe_all_tables(ws: ImprovedWorkspaceClient, make_catalog, make_schema, make_table):
    warehouse_id = os.environ["TEST_DEFAULT_WAREHOUSE_ID"]

    logger.info("setting up fixtures")
    schema = make_schema(catalog="hive_metastore")
    managed_table = make_table(schema=schema)
    external_table = make_table(schema=schema, external=True)
    tmp_table = make_table(schema=schema, ctas="SELECT 2+2 AS four")
    view = make_table(schema=schema, ctas="SELECT 2+2 AS four", view=True)
    non_delta = make_table(schema=schema, non_detla=True)

    logger.info(
        f"managed_table={managed_table}, "
        f"external_table={external_table}, "
        f"tmp_table={tmp_table}, "
        f"view={view}"
    )

    inventory_schema = make_schema(catalog=make_catalog())
    inventory_catalog, inventory_schema = inventory_schema.split(".")
    tak = TaclToolkit(ws, inventory_catalog, inventory_schema, warehouse_id)

    all_tables = {}
    for t in tak.database_snapshot(schema.split(".")[1]):
        all_tables[t.key] = t

    assert len(all_tables) == 5
    assert all_tables[non_delta].table_format == "JSON"
    assert all_tables[managed_table].object_type == "MANAGED"
    assert all_tables[tmp_table].object_type == "MANAGED"
    assert all_tables[external_table].object_type == "EXTERNAL"
    assert all_tables[view].object_type == "VIEW"
    assert all_tables[view].view_text == "SELECT 2+2 AS four"


def test_all_grants_in_database(
    ws: ImprovedWorkspaceClient, sql_exec, make_catalog, make_schema, make_table, make_group
):
    warehouse_id = os.environ["TEST_DEFAULT_WAREHOUSE_ID"]

    group_a = make_group()
    group_b = make_group()
    schema = make_schema()
    table = make_table(schema=schema, external=True)

    sql_exec(f"GRANT USAGE ON SCHEMA default TO {group_a.display_name}")
    sql_exec(f"GRANT USAGE ON SCHEMA default TO {group_b.display_name}")
    sql_exec(f"GRANT SELECT ON TABLE {table} TO {group_a.display_name}")
    sql_exec(f"GRANT MODIFY ON SCHEMA {schema} TO {group_b.display_name}")

    inventory_schema = make_schema(catalog=make_catalog())
    inventory_catalog, inventory_schema = inventory_schema.split(".")
    tak = TaclToolkit(ws, inventory_catalog, inventory_schema, warehouse_id)

    all_grants = {}
    for grant in tak.grants_snapshot(schema.split(".")[1]):
        logger.info(f"grant:\n{grant}\n  hive: {grant.hive_grant_sql()}\n  uc: {grant.uc_grant_sql()}")
        all_grants[f"{grant.principal}.{grant.object_key}"] = grant.action_type

    assert len(all_grants) >= 2, "must have at least two grants"
    assert all_grants[f"{group_a.display_name}.{table}"] == "SELECT"
    assert all_grants[f"{group_b.display_name}.{schema}"] == "MODIFY"

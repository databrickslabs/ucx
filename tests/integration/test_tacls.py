import logging
import os

from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.toolkits.table_acls import TaclToolkit

logging.getLogger("databricks.sdk").setLevel("DEBUG")


def test_describe_all_tables_in_databases(ws: WorkspaceClient, make_catalog, make_schema, make_table):
    warehouse_id = os.environ["TEST_DEFAULT_WAREHOUSE_ID"]

    logging.info("setting up fixtures")
    schema_a = make_schema(catalog="hive_metastore")
    schema_b = make_schema(catalog="hive_metastore")
    managed_table = make_table(schema=schema_a)
    external_table = make_table(schema=schema_b, external=True)
    tmp_table = make_table(schema=schema_a, ctas="SELECT 2+2 AS four")
    view = make_table(schema=schema_b, ctas="SELECT 2+2 AS four", view=True)
    non_delta = make_table(schema=schema_a, non_detla=True)

    logging.info(
        f"managed_table={managed_table}, "
        f"external_table={external_table}, "
        f"tmp_table={tmp_table}, "
        f"view={view}"
    )

    inventory_schema = make_schema(catalog=make_catalog())
    inventory_catalog, inventory_schema = inventory_schema.split(".")
    tak = TaclToolkit(ws, inventory_catalog, inventory_schema, warehouse_id)

    databases = [schema_a.split(".")[1], schema_b.split(".")[1]]

    all_tables = {}
    for t in tak.database_snapshot(databases):
        all_tables[t.key] = t

    assert len(all_tables) == 5
    assert all_tables[non_delta].table_format == "JSON"
    assert all_tables[managed_table].object_type == "MANAGED"
    assert all_tables[tmp_table].object_type == "MANAGED"
    assert all_tables[external_table].object_type == "EXTERNAL"
    assert all_tables[view].object_type == "VIEW"
    assert all_tables[view].view_text == "SELECT 2+2 AS four"


def test_all_grants_in_databases(ws: WorkspaceClient, sql_exec, make_catalog, make_schema, make_table, make_group):
    warehouse_id = os.environ["TEST_DEFAULT_WAREHOUSE_ID"]

    group_a = make_group(display_name="sdk_group_a")
    group_b = make_group(display_name="sdk_group_b")
    schema_a = make_schema()
    schema_b = make_schema()
    table_a = make_table(schema=schema_a)
    table_b = make_table(schema=schema_b)

    sql_exec(f"GRANT USAGE ON SCHEMA default TO {group_a.display_name}")
    sql_exec(f"GRANT USAGE ON SCHEMA default TO {group_b.display_name}")
    sql_exec(f"GRANT SELECT ON TABLE {table_a} TO {group_a.display_name}")
    sql_exec(f"GRANT SELECT ON TABLE {table_b} TO {group_b.display_name}")
    sql_exec(f"GRANT MODIFY ON SCHEMA {schema_b} TO {group_b.display_name}")

    inventory_schema = make_schema(catalog=make_catalog())
    inventory_catalog, inventory_schema = inventory_schema.split(".")
    tak = TaclToolkit(ws, inventory_catalog, inventory_schema, warehouse_id)

    databases = [schema_a.split(".")[1], schema_b.split(".")[1]]

    print(f"inventory_schema: {inventory_schema}")
    print(f"inventory_catalog: {inventory_catalog}")
    print(f"databases: {databases}")

    all_grants = {}
    for grant in tak.grants_snapshot(databases):
        logging.info(f"grant:\n{grant}\n  hive: {grant.hive_grant_sql()}\n  uc: {grant.uc_grant_sql()}")
        all_grants[f"{grant.principal}.{grant.object_key}"] = grant.action_type

    assert len(all_grants) >= 2, "must have at least two grants"
    assert all_grants[f"{group_a.display_name}.{table_a}"] == "SELECT"
    assert all_grants[f"{group_b.display_name}.{table_b}"] == "SELECT"
    assert all_grants[f"{group_b.display_name}.{schema_b}"] == "MODIFY"

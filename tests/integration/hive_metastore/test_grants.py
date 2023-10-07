import logging

from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.hive_metastore import GrantsCrawler, TablesCrawler

logger = logging.getLogger(__name__)


def test_all_grants_in_databases(ws: WorkspaceClient, sql_backend, inventory_schema,
                                 make_schema, make_table, make_group, env_or_skip):
    group_a = make_group()
    group_b = make_group()
    schema_a = make_schema()
    schema_b = make_schema()
    table_a = make_table(schema_name=schema_a.name)
    table_b = make_table(schema_name=schema_b.name)

    sql_backend.execute(f"GRANT USAGE ON SCHEMA default TO `{group_a.display_name}`")
    sql_backend.execute(f"GRANT USAGE ON SCHEMA default TO `{group_b.display_name}`")
    sql_backend.execute(f"GRANT SELECT ON TABLE {table_a.full_name} TO `{group_a.display_name}`")
    sql_backend.execute(f"GRANT SELECT ON TABLE {table_b.full_name} TO `{group_b.display_name}`")
    sql_backend.execute(f"GRANT MODIFY ON SCHEMA {schema_b.full_name} TO `{group_b.display_name}`")

    tables = TablesCrawler(sql_backend, inventory_schema)
    grants = GrantsCrawler(tables)

    all_grants = {}
    for grant in grants.snapshot():
        logging.info(f"grant:\n{grant}\n  hive: {grant.hive_grant_sql()}\n  uc: {grant.uc_grant_sql()}")
        all_grants[f"{grant.principal}.{grant.object_key}"] = grant.action_type

    assert len(all_grants) >= 3, "must have at least three grants"
    assert all_grants[f"{group_a.display_name}.{table_a.full_name}"] == "SELECT"
    assert all_grants[f"{group_b.display_name}.{table_b.full_name}"] == "SELECT"
    assert all_grants[f"{group_b.display_name}.{schema_b.full_name}"] == "MODIFY"

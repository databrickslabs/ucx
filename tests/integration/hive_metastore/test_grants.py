import logging
from datetime import timedelta

from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried

from databricks.labs.ucx.hive_metastore import GrantsCrawler, TablesCrawler

logger = logging.getLogger(__name__)


@retried(on=[NotFound], timeout=timedelta(minutes=10))
def test_all_grants_in_databases(sql_backend, inventory_schema, make_schema, make_table, make_group):
    group_a = make_group()
    group_b = make_group()
    schema_a = make_schema()
    schema_b = make_schema()
    empty_schema = make_schema()
    table_a = make_table(schema_name=schema_a.name)
    table_b = make_table(schema_name=schema_b.name)
    view_c = make_table(schema_name=schema_a.name, view=True, ctas="SELECT id FROM range(10)")
    view_d = make_table(schema_name=schema_a.name, view=True, ctas="SELECT id FROM range(10)")
    table_e = make_table(schema_name="default")

    sql_backend.execute(f"GRANT USAGE ON SCHEMA default TO `{group_a.display_name}`")
    sql_backend.execute(f"GRANT USAGE ON SCHEMA default TO `{group_b.display_name}`")
    sql_backend.execute(f"GRANT MODIFY ON TABLE {table_e.full_name} TO `{group_b.display_name}`")
    sql_backend.execute(f"GRANT SELECT ON TABLE {table_a.full_name} TO `{group_a.display_name}`")
    sql_backend.execute(f"GRANT SELECT ON TABLE {table_b.full_name} TO `{group_b.display_name}`")
    sql_backend.execute(f"GRANT MODIFY ON SCHEMA {schema_b.full_name} TO `{group_b.display_name}`")
    sql_backend.execute(f"GRANT MODIFY ON SCHEMA {empty_schema.full_name} TO `{group_b.display_name}`")
    sql_backend.execute(f"GRANT MODIFY ON VIEW {view_c.full_name} TO `{group_b.display_name}`")
    sql_backend.execute(f"GRANT MODIFY ON TABLE {view_d.full_name} TO `{group_b.display_name}`")

    tables = TablesCrawler(sql_backend, inventory_schema)
    grants = GrantsCrawler(tables)

    all_grants = {}
    for grant in grants.snapshot():
        logging.info(f"grant:\n{grant}\n  hive: {grant.hive_grant_sql()}\n  uc: {grant.uc_grant_sql()}")
        all_grants[f"{grant.principal}.{grant.object_key}"] = grant.action_type

    assert len(all_grants) >= 8, "must have at least three grants"
    # TODO: (nfx) KeyError: 'sdk-xH7i.hive_metastore.default'
    assert all_grants[f"{group_a.display_name}.hive_metastore.default"] == "USAGE"
    assert all_grants[f"{group_b.display_name}.hive_metastore.default"] == "USAGE"
    assert all_grants[f"{group_a.display_name}.{table_a.full_name}"] == "SELECT"
    assert all_grants[f"{group_b.display_name}.{table_b.full_name}"] == "SELECT"
    assert all_grants[f"{group_b.display_name}.{schema_b.full_name}"] == "MODIFY"
    assert all_grants[f"{group_b.display_name}.{empty_schema.full_name}"] == "MODIFY"
    assert all_grants[f"{group_b.display_name}.{view_c.full_name}"] == "MODIFY"
    assert all_grants[f"{group_b.display_name}.{view_d.full_name}"] == "MODIFY"
    assert all_grants[f"{group_b.display_name}.{table_e.full_name}"] == "MODIFY"

import logging
from collections import defaultdict
from datetime import timedelta

from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried

from databricks.labs.ucx.hive_metastore.grants import GrantsCrawler


logger = logging.getLogger(__name__)


@retried(on=[NotFound, TimeoutError], timeout=timedelta(minutes=3))
def test_all_grants_in_databases(runtime_ctx, sql_backend, make_group):
    group_a = make_group()
    group_b = make_group()
    schema_a = runtime_ctx.make_schema()
    schema_b = runtime_ctx.make_schema()
    schema_c = runtime_ctx.make_schema()
    empty_schema = runtime_ctx.make_schema()
    table_a = runtime_ctx.make_table(schema_name=schema_a.name)
    table_b = runtime_ctx.make_table(schema_name=schema_b.name)
    view_c = runtime_ctx.make_table(schema_name=schema_a.name, view=True, ctas="SELECT id FROM range(10)")
    view_d = runtime_ctx.make_table(schema_name=schema_a.name, view=True, ctas="SELECT id FROM range(10)")
    table_e = runtime_ctx.make_table(schema_name=schema_c.name)

    sql_backend.execute(f"GRANT USAGE ON SCHEMA {schema_c.name} TO `{group_a.display_name}`")
    sql_backend.execute(f"GRANT USAGE ON SCHEMA {schema_c.name} TO `{group_b.display_name}`")
    sql_backend.execute(f"GRANT MODIFY ON TABLE {table_e.full_name} TO `{group_b.display_name}`")
    sql_backend.execute(f"GRANT SELECT ON TABLE {table_a.full_name} TO `{group_a.display_name}`")
    sql_backend.execute(f"GRANT SELECT ON TABLE {table_b.full_name} TO `{group_b.display_name}`")
    sql_backend.execute(f"GRANT MODIFY ON SCHEMA {schema_b.full_name} TO `{group_b.display_name}`")
    sql_backend.execute(f"GRANT MODIFY ON SCHEMA {empty_schema.full_name} TO `{group_b.display_name}`")
    sql_backend.execute(f"GRANT MODIFY ON VIEW {view_c.full_name} TO `{group_b.display_name}`")
    sql_backend.execute(f"DENY MODIFY ON TABLE {view_d.full_name} TO `{group_b.display_name}`")

    # 20 seconds less than TablesCrawler(sql_backend, inventory_schema)
    grants = GrantsCrawler(runtime_ctx.tables_crawler, runtime_ctx.udfs_crawler)

    all_grants = {}
    for grant in grants.snapshot():
        logging.info(f"grant:\n{grant}\n  hive: {grant.hive_grant_sql()}\n  uc: {grant.uc_grant_sql()}")
        all_grants[f"{grant.principal}.{grant.object_key}"] = grant.action_type

    assert len(all_grants) >= 8, "must have at least three grants"
    assert all_grants[f"{group_a.display_name}.hive_metastore.{schema_c.name}"] == "USAGE"
    assert all_grants[f"{group_b.display_name}.hive_metastore.{schema_c.name}"] == "USAGE"
    assert all_grants[f"{group_a.display_name}.{table_a.full_name}"] == "SELECT"
    assert all_grants[f"{group_b.display_name}.{table_b.full_name}"] == "SELECT"
    assert all_grants[f"{group_b.display_name}.{schema_b.full_name}"] == "MODIFY"
    assert all_grants[f"{group_b.display_name}.{empty_schema.full_name}"] == "MODIFY"
    assert all_grants[f"{group_b.display_name}.{view_c.full_name}"] == "MODIFY"
    assert all_grants[f"{group_b.display_name}.{view_d.full_name}"] == "DENIED_MODIFY"
    assert all_grants[f"{group_b.display_name}.{table_e.full_name}"] == "MODIFY"


@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_all_grants_for_udfs_in_databases(runtime_ctx, sql_backend, make_group):
    group_a = make_group()
    group_b = make_group()
    schema = runtime_ctx.make_schema()
    udf_a = runtime_ctx.make_udf(schema_name=schema.name)
    udf_b = runtime_ctx.make_udf(schema_name=schema.name)

    sql_backend.execute(f"GRANT SELECT ON FUNCTION {udf_a.full_name} TO `{group_a.display_name}`")
    sql_backend.execute(f"GRANT READ_METADATA ON FUNCTION {udf_a.full_name} TO `{group_a.display_name}`")
    sql_backend.execute(f"ALTER FUNCTION {udf_a.full_name} OWNER TO `{group_a.display_name}`")
    sql_backend.execute(f"GRANT ALL PRIVILEGES ON FUNCTION {udf_b.full_name} TO `{group_a.display_name}`")
    sql_backend.execute(f"DENY READ_METADATA ON FUNCTION {udf_b.full_name} to `{group_b.display_name}`")

    grants = GrantsCrawler(runtime_ctx.tables_crawler, runtime_ctx.udfs_crawler)

    actual_grants = defaultdict(set)
    for grant in grants.snapshot():
        actual_grants[f"{grant.principal}.{grant.object_key}"].add(grant.action_type)

    assert {"SELECT", "READ_METADATA", "OWN"} == actual_grants[f"{group_a.display_name}.{udf_a.full_name}"]
    assert {"SELECT", "READ_METADATA"} == actual_grants[f"{group_a.display_name}.{udf_b.full_name}"]
    assert {"DENIED_READ_METADATA"} == actual_grants[f"{group_b.display_name}.{udf_b.full_name}"]

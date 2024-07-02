import logging
from collections import defaultdict
from datetime import timedelta

from databricks.sdk.errors import NotFound

from databricks.labs.lsql.backends import StatementExecutionBackend
from databricks.labs.ucx.hive_metastore.grants import GrantsCrawler

from ..conftest import TestRuntimeContext
from ..retries import retried

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
    for grant in list(grants.snapshot()):
        logging.info(f"grant:\n{grant}\n  hive: {grant.hive_grant_sql()}\n  uc: {grant.uc_grant_sql()}")
        object_type, object_key = grant.this_type_and_key()
        all_grants[f"{grant.principal}.{object_type}.{object_key}"] = grant.action_type

    assert len(all_grants) >= 9, "must have at least nine grants"
    assert all_grants[f"{group_a.display_name}.DATABASE.hive_metastore.{schema_c.name}"] == "USAGE"
    assert all_grants[f"{group_b.display_name}.DATABASE.hive_metastore.{schema_c.name}"] == "USAGE"
    assert all_grants[f"{group_a.display_name}.TABLE.{table_a.full_name}"] == "SELECT"
    assert all_grants[f"{group_b.display_name}.TABLE.{table_b.full_name}"] == "SELECT"
    assert all_grants[f"{group_b.display_name}.DATABASE.{schema_b.full_name}"] == "MODIFY"
    assert all_grants[f"{group_b.display_name}.DATABASE.{empty_schema.full_name}"] == "MODIFY"
    assert all_grants[f"{group_b.display_name}.VIEW.{view_c.full_name}"] == "MODIFY"
    assert all_grants[f"{group_b.display_name}.VIEW.{view_d.full_name}"] == "DENIED_MODIFY"
    assert all_grants[f"{group_b.display_name}.TABLE.{table_e.full_name}"] == "MODIFY"


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

    crawler_snapshot = list(grants.snapshot())
    actual_grants = defaultdict(set)
    for grant in crawler_snapshot:
        object_type, object_key = grant.this_type_and_key()
        actual_grants[f"{grant.principal}.{object_type}.{object_key}"].add(grant.action_type)

    assert {"SELECT", "READ_METADATA", "OWN"} == actual_grants[f"{group_a.display_name}.FUNCTION.{udf_a.full_name}"]
    assert {"SELECT", "READ_METADATA"} == actual_grants[f"{group_a.display_name}.FUNCTION.{udf_b.full_name}"]
    assert {"DENIED_READ_METADATA"} == actual_grants[f"{group_b.display_name}.FUNCTION.{udf_b.full_name}"]


@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_all_grants_for_other_objects(
    runtime_ctx: TestRuntimeContext, sql_backend: StatementExecutionBackend, make_group
) -> None:
    group_a = make_group()
    group_b = make_group()
    group_c = make_group()
    group_d = make_group()

    sql_backend.execute(f"GRANT SELECT ON ANY FILE TO `{group_a.display_name}`")
    sql_backend.execute(f"GRANT MODIFY ON ANY FILE TO `{group_a.display_name}`")
    sql_backend.execute(f"DENY SELECT ON ANY FILE TO `{group_b.display_name}`")
    sql_backend.execute(f"GRANT SELECT ON ANONYMOUS FUNCTION TO `{group_c.display_name}`")
    sql_backend.execute(f"DENY SELECT ON ANONYMOUS FUNCTION TO `{group_d.display_name}`")

    crawler = GrantsCrawler(runtime_ctx.tables_crawler, runtime_ctx.udfs_crawler)
    all_found_grants = list(crawler.snapshot())

    found_any_file_grants = defaultdict(set)
    found_anonymous_function_grants = defaultdict(set)
    for grant in all_found_grants:
        if grant.any_file:
            found_any_file_grants[grant.principal].add(grant.action_type)
        if grant.anonymous_function:
            found_anonymous_function_grants[grant.principal].add(grant.action_type)

    assert {"SELECT", "MODIFY"} == found_any_file_grants[group_a.display_name]
    assert {"DENIED_SELECT"} == found_any_file_grants[group_b.display_name]
    assert {"SELECT"} == found_anonymous_function_grants[group_c.display_name]
    assert {"DENIED_SELECT"} == found_anonymous_function_grants[group_d.display_name]

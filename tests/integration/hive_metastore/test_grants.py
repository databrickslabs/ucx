import logging
from collections import defaultdict
from dataclasses import replace
from datetime import timedelta

from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried

from databricks.labs.lsql.backends import StatementExecutionBackend

from databricks.labs.ucx.framework.utils import escape_sql_identifier
from databricks.labs.ucx.hive_metastore import TablesCrawler
from databricks.labs.ucx.hive_metastore.grants import GrantsCrawler, GrantOwnership
from databricks.labs.ucx.hive_metastore.udfs import UdfsCrawler
from ..conftest import MockRuntimeContext

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
    table_c = runtime_ctx.make_table(schema_name=schema_c.name)
    view_c = runtime_ctx.make_table(schema_name=schema_a.name, view=True, ctas="SELECT id FROM range(10)")
    view_d = runtime_ctx.make_table(schema_name=schema_a.name, view=True, ctas="SELECT id FROM range(10)")
    view_e = runtime_ctx.make_table(schema_name=schema_c.name, view=True, ctas="SELECT id FROM range(10)")
    table_e = runtime_ctx.make_table(schema_name=schema_c.name)

    sql_backend.execute(f"GRANT USAGE ON SCHEMA {schema_c.name} TO `{group_a.display_name}`")
    sql_backend.execute(f"GRANT USAGE ON SCHEMA {schema_c.name} TO `{group_b.display_name}`")
    sql_backend.execute(f"GRANT MODIFY ON TABLE {table_e.full_name} TO `{group_b.display_name}`")
    sql_backend.execute(f"GRANT SELECT ON TABLE {table_a.full_name} TO `{group_a.display_name}`")
    sql_backend.execute(f"GRANT SELECT ON TABLE {table_b.full_name} TO `{group_b.display_name}`")
    sql_backend.execute(f"GRANT READ_METADATA ON TABLE {table_c.full_name} TO `{group_b.display_name}`")
    sql_backend.execute(f"GRANT MODIFY ON SCHEMA {schema_b.full_name} TO `{group_b.display_name}`")
    sql_backend.execute(f"GRANT READ_METADATA ON SCHEMA {empty_schema.full_name} TO `{group_b.display_name}`")
    sql_backend.execute(f"GRANT MODIFY ON VIEW {view_c.full_name} TO `{group_b.display_name}`")
    sql_backend.execute(f"GRANT READ_METADATA ON VIEW {view_e.full_name} TO `{group_b.display_name}`")
    sql_backend.execute(f"DENY MODIFY ON TABLE {view_d.full_name} TO `{group_b.display_name}`")

    all_grants = {}
    for grant in list(runtime_ctx.grants_crawler.snapshot()):
        logging.info(f"grant:\n{grant}\n  hive: {grant.hive_grant_sql()}\n  uc: {grant.uc_grant_sql()}")
        all_grants[f"{grant.principal}.{grant.this_type_and_key()[0]}.{grant.this_type_and_key()[1]}"] = (
            grant.action_type
        )

    assert len(all_grants) >= 12, "must have at least twelve grants"
    assert all_grants[f"{group_a.display_name}.DATABASE.hive_metastore.{schema_c.name}"] == "USAGE"
    assert all_grants[f"{group_b.display_name}.DATABASE.hive_metastore.{schema_c.name}"] == "USAGE"
    assert all_grants[f"{group_a.display_name}.TABLE.{table_a.full_name}"] == "SELECT"
    assert all_grants[f"{group_b.display_name}.TABLE.{table_b.full_name}"] == "SELECT"
    assert all_grants[f"{group_b.display_name}.TABLE.{table_c.full_name}"] == "READ_METADATA"
    assert all_grants[f"{group_b.display_name}.DATABASE.{schema_b.full_name}"] == "MODIFY"
    assert all_grants[f"{group_b.display_name}.DATABASE.{empty_schema.full_name}"] == "READ_METADATA"
    assert all_grants[f"{group_b.display_name}.VIEW.{view_c.full_name}"] == "MODIFY"
    assert all_grants[f"{group_b.display_name}.VIEW.{view_e.full_name}"] == "READ_METADATA"
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

    crawler_snapshot = list(runtime_ctx.grants_crawler.snapshot())
    actual_grants = defaultdict(set)
    for grant in crawler_snapshot:
        object_type, object_key = grant.this_type_and_key()
        actual_grants[f"{grant.principal}.{object_type}.{object_key}"].add(grant.action_type)

    assert {"SELECT", "READ_METADATA", "OWN"} == actual_grants[f"{group_a.display_name}.FUNCTION.{udf_a.full_name}"]
    assert {"SELECT", "READ_METADATA"} == actual_grants[f"{group_a.display_name}.FUNCTION.{udf_b.full_name}"]
    assert {"DENIED_READ_METADATA"} == actual_grants[f"{group_b.display_name}.FUNCTION.{udf_b.full_name}"]


@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_all_grants_for_other_objects(
    runtime_ctx: MockRuntimeContext, sql_backend: StatementExecutionBackend, make_group
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

    runtime_ctx.make_schema()  # optimization to avoid crawling all databases
    all_found_grants = list(runtime_ctx.grants_crawler.snapshot())

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


def test_grant_ownership(ws, runtime_ctx, inventory_schema, sql_backend, make_random, make_acc_group) -> None:
    """Verify the ownership can be determined for crawled grants.
    This currently isn't very useful: we can't locate specific owners for grants"""

    schema = runtime_ctx.make_schema()
    this_user = ws.current_user.me()
    sql_backend.execute(f"GRANT SELECT ON SCHEMA {escape_sql_identifier(schema.full_name)} TO `{this_user.user_name}`")
    table_crawler = TablesCrawler(sql_backend, schema=inventory_schema, include_databases=[schema.name])
    udf_crawler = UdfsCrawler(sql_backend, schema=inventory_schema, include_databases=[schema.name])
    current_user = ws.current_user.me()
    admin_group_name = f"admin_group_{make_random()}"
    make_acc_group(display_name=admin_group_name, members=[current_user.id], wait_for_provisioning=True)

    # Produce the crawled records.
    crawler = GrantsCrawler(
        sql_backend, inventory_database, table_crawler, udf_crawler, include_databases=[schema.name]
    )
    records = crawler.snapshot(force_refresh=True)

    # Find the crawled record for the grant we made.
    grant_record = next(record for record in records if record.this_type_and_key() == ("DATABASE", schema.full_name))

    # Verify ownership can be made.
    ownership = GrantOwnership(runtime_ctx.administrator_locator)
    assert ownership.owner_of(grant_record) == runtime_ctx.administrator_locator.get_workspace_administrator()

    # Test default group ownership
    test_config = replace(runtime_ctx.config, default_owner_group=admin_group_name)
    default_owner_group = runtime_ctx.replace(config=test_config).config.default_owner_group
    assert default_owner_group == admin_group_name

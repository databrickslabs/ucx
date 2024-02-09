import json
import logging
from collections import defaultdict

from databricks.labs.ucx.hive_metastore import GrantsCrawler
from databricks.labs.ucx.workspace_access.base import Permissions
from databricks.labs.ucx.workspace_access.groups import MigratedGroup
from databricks.labs.ucx.workspace_access.tacl import TableAclSupport

from ..conftest import StaticTablesCrawler, StaticUdfsCrawler
from . import apply_tasks

logger = logging.getLogger(__name__)


# @retried(on=[AssertionError], timeout=timedelta(minutes=3))
def test_permission_for_files_anonymous_func(sql_backend, inventory_schema, make_group):
    old = make_group()
    new = make_group()
    logger.debug(f"old={old.display_name}, new={new.display_name}")

    sql_backend.execute(f"GRANT READ_METADATA ON ANY FILE TO `{old.display_name}`")
    sql_backend.execute(f"GRANT SELECT ON ANONYMOUS FUNCTION TO `{old.display_name}`")

    tables = StaticTablesCrawler(sql_backend, inventory_schema, [])
    udfs = StaticUdfsCrawler(sql_backend, inventory_schema, [])
    grants = GrantsCrawler(tables, udfs)

    tacl_support = TableAclSupport(grants, sql_backend)
    apply_tasks(tacl_support, [MigratedGroup.partial_info(old, new)])

    any_file_actual = {}
    for any_file_grant in grants._grants(any_file=True):
        any_file_actual[any_file_grant.principal] = any_file_grant.action_type

    # both old and new group have permissions
    assert old.display_name in any_file_actual
    assert new.display_name in any_file_actual
    assert any_file_actual[old.display_name] == any_file_actual[new.display_name]

    anonymous_function_actual = {}
    for ano_func_grant in grants._grants(anonymous_function=True):
        anonymous_function_actual[ano_func_grant.principal] = ano_func_grant.action_type

    assert old.display_name in anonymous_function_actual
    assert new.display_name in anonymous_function_actual
    assert anonymous_function_actual[new.display_name] == "SELECT"
    assert anonymous_function_actual[old.display_name] == anonymous_function_actual[new.display_name]


def test_hms2hms_owner_permissions(sql_backend, inventory_schema, make_schema, make_table, make_group_pair):
    first = make_group_pair()
    second = make_group_pair()
    third = make_group_pair()

    schema_a = make_schema()
    schema_b = make_schema()
    schema_c = make_schema()
    table_a = make_table(schema_name=schema_a.name)
    table_b = make_table(schema_name=schema_b.name)
    table_c = make_table(schema_name=schema_b.name, external=True)

    sql_backend.execute(f"GRANT USAGE ON SCHEMA {schema_a.name} TO `{first.name_in_workspace}`")
    sql_backend.execute(f"ALTER SCHEMA {schema_a.name} OWNER TO `{first.name_in_workspace}`")
    sql_backend.execute(f"GRANT ALL PRIVILEGES ON SCHEMA {schema_b.name} TO `{second.name_in_workspace}`")
    sql_backend.execute(
        f"GRANT USAGE, SELECT, MODIFY, CREATE, READ_METADATA, CREATE_NAMED_FUNCTION ON SCHEMA {schema_c.name} TO "
        f"`{third.name_in_workspace}`"
    )
    sql_backend.execute(f"GRANT SELECT ON TABLE {table_a.full_name} TO `{first.name_in_workspace}`")
    sql_backend.execute(
        f"GRANT SELECT, MODIFY, READ_METADATA ON TABLE {table_b.full_name} TO `{second.name_in_workspace}`"
    )
    sql_backend.execute(f"ALTER TABLE {table_b.full_name} OWNER TO `{second.name_in_workspace}`")
    sql_backend.execute(f"GRANT SELECT, MODIFY ON TABLE {table_c.full_name} TO `{third.name_in_workspace}`")

    tables = StaticTablesCrawler(sql_backend, inventory_schema, [table_a, table_b, table_c])
    udfs = StaticUdfsCrawler(sql_backend, inventory_schema, [])
    grants = GrantsCrawler(tables, udfs)

    original_table_grants = {
        "a": grants.for_table_info(table_a),
        "b": grants.for_table_info(table_b),
        "c": grants.for_table_info(table_c),
    }
    assert {"SELECT"} == original_table_grants["a"][first.name_in_workspace]
    assert {"MODIFY", "OWN", "READ_METADATA", "SELECT"} == original_table_grants["b"][second.name_in_workspace]
    assert {"MODIFY", "SELECT"} == original_table_grants["c"][third.name_in_workspace]

    original_schema_grants = {
        "a": grants.for_schema_info(schema_a),
        "b": grants.for_schema_info(schema_b),
    }
    assert {"OWN", "USAGE"} == original_schema_grants["a"][first.name_in_workspace]
    assert {"CREATE", "CREATE_NAMED_FUNCTION", "MODIFY", "READ_METADATA", "SELECT", "USAGE"} == original_schema_grants[
        "b"
    ][second.name_in_workspace]

    tacl_support = TableAclSupport(grants, sql_backend)

    apply_tasks(tacl_support, [first, second, third])

    new_table_grants = {
        "a": grants.for_table_info(table_a),
        "b": grants.for_table_info(table_b),
        "c": grants.for_table_info(table_c),
    }
    assert new_table_grants["a"][first.name_in_account] == {"SELECT"}, first.name_in_account
    assert new_table_grants["b"][second.name_in_account] == {
        "MODIFY",
        "OWN",
        "READ_METADATA",
        "SELECT",
    }, second.name_in_account
    assert new_table_grants["c"][third.name_in_account] == {"MODIFY", "SELECT"}, third.name_in_account

    new_schema_grants = {
        "a": grants.for_schema_info(schema_a),
        "b": grants.for_schema_info(schema_b),
    }
    assert new_schema_grants["a"][first.name_in_account] == {"OWN", "USAGE"}, first.name_in_account
    assert new_schema_grants["b"][second.name_in_account] == {
        "CREATE",
        "CREATE_NAMED_FUNCTION",
        "MODIFY",
        "READ_METADATA",
        "SELECT",
        "USAGE",
    }, second.name_in_account


def test_permission_for_udfs(sql_backend, inventory_schema, make_schema, make_udf, make_group_pair):
    group = make_group_pair()
    schema = make_schema()
    udf_a = make_udf(schema_name=schema.name)
    udf_b = make_udf(schema_name=schema.name)

    sql_backend.execute(f"GRANT SELECT ON FUNCTION {udf_a.full_name} TO `{group.name_in_workspace}`")
    sql_backend.execute(f"ALTER FUNCTION {udf_a.full_name} OWNER TO `{group.name_in_workspace}`")
    sql_backend.execute(f"GRANT READ_METADATA ON FUNCTION {udf_b.full_name} TO `{group.name_in_workspace}`")

    tables = StaticTablesCrawler(sql_backend, inventory_schema, [])
    udfs = StaticUdfsCrawler(sql_backend, inventory_schema, [udf_a, udf_b])
    grants = GrantsCrawler(tables, udfs)

    all_initial_grants = set()
    for grant in grants.snapshot():
        all_initial_grants.add(f"{grant.principal}.{grant.object_key}:{grant.action_type}")

    assert f"{group.name_in_workspace}.{udf_a.full_name}:SELECT" in all_initial_grants
    assert f"{group.name_in_workspace}.{udf_a.full_name}:OWN" in all_initial_grants
    assert f"{group.name_in_workspace}.{udf_b.full_name}:READ_METADATA" in all_initial_grants

    tacl_support = TableAclSupport(grants, sql_backend)
    apply_tasks(tacl_support, [group])

    actual_udf_a_grants = defaultdict(set)
    for grant in grants._grants(catalog=schema.catalog_name, database=schema.name, udf=udf_a.name):
        actual_udf_a_grants[grant.principal].add(grant.action_type)
    assert {"SELECT", "OWN"} == actual_udf_a_grants[group.name_in_account]

    actual_udf_b_grants = defaultdict(set)
    for grant in grants._grants(catalog=schema.catalog_name, database=schema.name, udf=udf_b.name):
        actual_udf_b_grants[grant.principal].add(grant.action_type)
    assert {"READ_METADATA"} == actual_udf_b_grants[group.name_in_account]


def test_verify_permission_for_udfs(sql_backend, inventory_schema, make_schema, make_udf, make_group):
    group = make_group()
    schema = make_schema()

    sql_backend.execute(f"GRANT SELECT ON SCHEMA {schema.name} TO `{group.display_name}`")

    item = Permissions(
        object_type="DATABASE",
        object_id=schema.full_name,
        raw=json.dumps(
            {
                "principal": group.display_name,
                "action_type": "SELECT",
                "catalog": schema.catalog_name,
                "database": schema.name,
            }
        ),
    )

    tables = StaticTablesCrawler(sql_backend, inventory_schema, [])
    udfs = StaticUdfsCrawler(sql_backend, inventory_schema, [])
    grants = GrantsCrawler(tables, udfs)

    tacl_support = TableAclSupport(grants, sql_backend)
    task = tacl_support.get_verify_task(item)
    result = task()

    assert result

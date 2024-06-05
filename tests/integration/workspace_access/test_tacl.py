import json
import logging
from collections import defaultdict

from databricks.labs.ucx.workspace_access.base import Permissions
from databricks.labs.ucx.workspace_access.groups import MigratedGroup, MigrationState
from databricks.labs.ucx.workspace_access.tacl import TableAclSupport

from . import apply_tasks

logger = logging.getLogger(__name__)


def test_grants_with_permission_migration_api(runtime_ctx, ws, migrated_group, sql_backend):
    schema_a = runtime_ctx.make_schema()
    table_a = runtime_ctx.make_table(schema_name=schema_a.name)
    sql_backend.execute(f"GRANT USAGE ON SCHEMA {schema_a.name} TO `{migrated_group.name_in_workspace}`")
    sql_backend.execute(f"ALTER SCHEMA {schema_a.name} OWNER TO `{migrated_group.name_in_workspace}`")
    sql_backend.execute(f"GRANT SELECT ON TABLE {table_a.full_name} TO `{migrated_group.name_in_workspace}`")

    grants = runtime_ctx.grants_crawler

    original_table_grants = {"a": grants.for_table_info(table_a)}
    assert {"SELECT"} == original_table_grants["a"][migrated_group.name_in_workspace]

    original_schema_grants = {"a": grants.for_schema_info(schema_a)}
    assert {"USAGE", "OWN"} == original_schema_grants["a"][migrated_group.name_in_workspace]

    MigrationState([migrated_group]).apply_to_groups_with_different_names(ws)

    new_table_grants = {"a": grants.for_table_info(table_a)}
    assert {"SELECT"} == new_table_grants["a"][migrated_group.name_in_account]

    new_schema_grants = {"a": grants.for_schema_info(schema_a)}
    assert {"USAGE", "OWN"} == new_schema_grants["a"][migrated_group.name_in_account]


def test_permission_for_files_anonymous_func_migration_api(ws, sql_backend, runtime_ctx, migrated_group):
    sql_backend.execute(f"GRANT READ_METADATA ON ANY FILE TO `{migrated_group.name_in_workspace}`")
    sql_backend.execute(f"GRANT SELECT ON ANONYMOUS FUNCTION TO `{migrated_group.name_in_workspace}`")

    grants = runtime_ctx.grants_crawler

    MigrationState([migrated_group]).apply_to_groups_with_different_names(ws)

    any_file_actual = {}
    for any_file_grant in grants.grants(any_file=True):
        any_file_actual[any_file_grant.principal] = any_file_grant.action_type

    # both old and new group have permissions
    assert migrated_group.name_in_workspace not in any_file_actual
    assert migrated_group.name_in_account in any_file_actual

    anonymous_function_actual = {}
    for ano_func_grant in grants.grants(anonymous_function=True):
        anonymous_function_actual[ano_func_grant.principal] = ano_func_grant.action_type

    assert migrated_group.name_in_workspace not in anonymous_function_actual
    assert migrated_group.name_in_account in anonymous_function_actual
    assert anonymous_function_actual[migrated_group.name_in_account] == "SELECT"


def test_permission_for_udfs_migration_api(ws, sql_backend, runtime_ctx, migrated_group):
    schema = runtime_ctx.make_schema()
    udf_a = runtime_ctx.make_udf(schema_name=schema.name)
    udf_b = runtime_ctx.make_udf(schema_name=schema.name)

    sql_backend.execute(f"GRANT SELECT ON FUNCTION {udf_a.full_name} TO `{migrated_group.name_in_workspace}`")
    sql_backend.execute(f"ALTER FUNCTION {udf_a.full_name} OWNER TO `{migrated_group.name_in_workspace}`")
    sql_backend.execute(f"GRANT READ_METADATA ON FUNCTION {udf_b.full_name} TO `{migrated_group.name_in_workspace}`")

    grants = runtime_ctx.grants_crawler

    all_initial_grants = set()
    for grant in grants.snapshot():
        all_initial_grants.add(f"{grant.principal}.{grant.object_key}:{grant.action_type}")

    assert f"{migrated_group.name_in_workspace}.{udf_a.full_name}:SELECT" in all_initial_grants
    assert f"{migrated_group.name_in_workspace}.{udf_a.full_name}:OWN" in all_initial_grants
    assert f"{migrated_group.name_in_workspace}.{udf_b.full_name}:READ_METADATA" in all_initial_grants

    MigrationState([migrated_group]).apply_to_groups_with_different_names(ws)

    actual_udf_a_grants = defaultdict(set)
    for grant in grants.grants(catalog=schema.catalog_name, database=schema.name, udf=udf_a.name):
        actual_udf_a_grants[grant.principal].add(grant.action_type)
    assert {"SELECT", "OWN"} == actual_udf_a_grants[migrated_group.name_in_account]

    actual_udf_b_grants = defaultdict(set)
    for grant in grants.grants(catalog=schema.catalog_name, database=schema.name, udf=udf_b.name):
        actual_udf_b_grants[grant.principal].add(grant.action_type)
    assert {"READ_METADATA"} == actual_udf_b_grants[migrated_group.name_in_account]


def test_permission_for_files_anonymous_func(sql_backend, runtime_ctx, make_group):
    old = make_group()
    new = make_group()
    logger.debug(f"old={old.display_name}, new={new.display_name}")

    sql_backend.execute(f"GRANT READ_METADATA ON ANY FILE TO `{old.display_name}`")
    sql_backend.execute(f"GRANT SELECT ON ANONYMOUS FUNCTION TO `{old.display_name}`")

    grants = runtime_ctx.grants_crawler

    tacl_support = TableAclSupport(grants, sql_backend)
    apply_tasks(tacl_support, [MigratedGroup.partial_info(old, new)])

    any_file_actual = {}
    for any_file_grant in grants.grants(any_file=True):
        any_file_actual[any_file_grant.principal] = any_file_grant.action_type

    # both old and new group have permissions
    assert old.display_name in any_file_actual
    assert new.display_name in any_file_actual
    assert any_file_actual[old.display_name] == any_file_actual[new.display_name]

    anonymous_function_actual = {}
    for ano_func_grant in grants.grants(anonymous_function=True):
        anonymous_function_actual[ano_func_grant.principal] = ano_func_grant.action_type

    assert old.display_name in anonymous_function_actual
    assert new.display_name in anonymous_function_actual
    assert anonymous_function_actual[new.display_name] == "SELECT"
    assert anonymous_function_actual[old.display_name] == anonymous_function_actual[new.display_name]


def test_hms2hms_owner_permissions(sql_backend, runtime_ctx, make_group_pair):
    first = make_group_pair()
    second = make_group_pair()
    third = make_group_pair()

    schema_a = runtime_ctx.make_schema()
    schema_b = runtime_ctx.make_schema()
    schema_c = runtime_ctx.make_schema()
    table_a = runtime_ctx.make_table(schema_name=schema_a.name)
    table_b = runtime_ctx.make_table(schema_name=schema_b.name)
    table_c = runtime_ctx.make_table(schema_name=schema_b.name, external=True)

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

    grants = runtime_ctx.grants_crawler

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


def test_permission_for_udfs(sql_backend, runtime_ctx, make_group_pair):
    group = make_group_pair()
    schema = runtime_ctx.make_schema()
    udf_a = runtime_ctx.make_udf(schema_name=schema.name)
    udf_b = runtime_ctx.make_udf(schema_name=schema.name)

    sql_backend.execute(f"GRANT SELECT ON FUNCTION {udf_a.full_name} TO `{group.name_in_workspace}`")
    sql_backend.execute(f"ALTER FUNCTION {udf_a.full_name} OWNER TO `{group.name_in_workspace}`")
    sql_backend.execute(f"GRANT READ_METADATA ON FUNCTION {udf_b.full_name} TO `{group.name_in_workspace}`")
    sql_backend.execute(f"DENY `SELECT` ON FUNCTION {udf_b.full_name} TO `{group.name_in_workspace}`")

    grants = runtime_ctx.grants_crawler

    all_initial_grants = set()
    for grant in grants.snapshot():
        all_initial_grants.add(f"{grant.principal}.{grant.object_key}:{grant.action_type}")

    assert f"{group.name_in_workspace}.{udf_a.full_name}:SELECT" in all_initial_grants
    assert f"{group.name_in_workspace}.{udf_a.full_name}:OWN" in all_initial_grants
    assert f"{group.name_in_workspace}.{udf_b.full_name}:READ_METADATA" in all_initial_grants
    assert f"{group.name_in_workspace}.{udf_b.full_name}:DENIED_SELECT" in all_initial_grants

    tacl_support = TableAclSupport(grants, sql_backend)
    apply_tasks(tacl_support, [group])

    actual_udf_a_grants = defaultdict(set)
    for grant in grants.grants(catalog=schema.catalog_name, database=schema.name, udf=udf_a.name):
        actual_udf_a_grants[grant.principal].add(grant.action_type)
    assert {"SELECT", "OWN"} == actual_udf_a_grants[group.name_in_account]

    actual_udf_b_grants = defaultdict(set)
    for grant in grants.grants(catalog=schema.catalog_name, database=schema.name, udf=udf_b.name):
        actual_udf_b_grants[grant.principal].add(grant.action_type)
    assert {"READ_METADATA", "DENIED_SELECT"} == actual_udf_b_grants[group.name_in_account]


def test_verify_permission_for_udfs(sql_backend, runtime_ctx, make_group):
    group = make_group()
    schema = runtime_ctx.make_schema()

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

    grants = runtime_ctx.grants_crawler

    tacl_support = TableAclSupport(grants, sql_backend)
    task = tacl_support.get_verify_task(item)
    result = task()

    assert result

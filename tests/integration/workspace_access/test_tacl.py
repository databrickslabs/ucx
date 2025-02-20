import json
import logging
from collections import defaultdict
from collections.abc import Callable

import pytest
from databricks.labs.lsql.backends import CommandExecutionBackend, SqlBackend

from databricks.labs.ucx.workspace_access.base import Permissions
from databricks.labs.ucx.workspace_access.groups import MigratedGroup, MigrationState
from databricks.labs.ucx.workspace_access.tacl import TableAclSupport

from . import apply_tasks
from ..conftest import MockRuntimeContext


logger = logging.getLogger(__name__)


@pytest.fixture
def _sql_backend_acl(ws, env_or_skip) -> SqlBackend:
    """Ensure the SQL backend in this module is table access control list enabled.

    We have a separate (legacy) cluster for table access control list. This
    ensures the tests in this module to be closer to the assessment workflow.
    """
    cluster_id = env_or_skip("TEST_LEGACY_TABLE_ACL_CLUSTER_ID")
    return CommandExecutionBackend(ws, cluster_id)


@pytest.fixture
def _prepare_context(_sql_backend_acl) -> Callable[[MockRuntimeContext], MockRuntimeContext]:
    """See inner :func:`prepare` function"""

    def prepare(context: MockRuntimeContext):
        """Prepare the context to crawl grants for testing access control lists.

        The grants crawler uses the table and UDF crawler. In the assessment workflow, the grants are crawled on a TACL
        cluster with access to (legacy) table access control lists, while the tables and UDFs are crawled on a "regular"
        cluster. We simulate this flow by crawling the tables and UDFs first, persist them in the inventory, then we
        crawl the grants on the TACL cluster.
        """
        # We need at least one table and UDF to store in the inventory to be fetched by the grants crawler
        context.make_table()
        context.make_udf()
        context.tables_crawler.snapshot(force_refresh=True)
        context.udfs_crawler.snapshot(force_refresh=True)
        return context.replace(sql_backend=_sql_backend_acl)

    return prepare


def test_grants_with_permission_migration_api(
    runtime_ctx: MockRuntimeContext,
    migrated_group,
    _prepare_context: Callable[[MockRuntimeContext], MockRuntimeContext],
) -> None:
    # TODO: Move migrated group into `runtime_ctx` and follow the `make_` pattern
    ctx = _prepare_context(runtime_ctx)
    schema = ctx.make_schema()
    table = ctx.make_table(schema_name=schema.name)
    ctx.sql_backend.execute(f"GRANT USAGE ON SCHEMA {schema.name} TO `{migrated_group.name_in_workspace}`")
    ctx.sql_backend.execute(f"ALTER SCHEMA {schema.name} OWNER TO `{migrated_group.name_in_workspace}`")
    ctx.sql_backend.execute(f"GRANT SELECT ON TABLE {table.full_name} TO `{migrated_group.name_in_workspace}`")

    original_table_grants = ctx.grants_crawler.for_table_info(table)
    assert {"SELECT"} == original_table_grants[migrated_group.name_in_workspace]

    original_schema_grants = ctx.grants_crawler.for_schema_info(schema)
    assert {"USAGE", "OWN"} == original_schema_grants[migrated_group.name_in_workspace]

    MigrationState([migrated_group]).apply_to_groups_with_different_names(ctx.workspace_client)

    new_table_grants = ctx.grants_crawler.for_table_info(table)
    assert {"SELECT"} == new_table_grants[migrated_group.name_in_account]

    new_schema_grants = ctx.grants_crawler.for_schema_info(schema)
    assert {"USAGE", "OWN"} == new_schema_grants[migrated_group.name_in_account]


def test_permission_for_files_anonymous_func_migration_api(
    runtime_ctx: MockRuntimeContext,
    migrated_group,
    _prepare_context: Callable[[MockRuntimeContext], MockRuntimeContext],
) -> None:
    # TODO: Move migrated group into `runtime_ctx` and follow the `make_` pattern
    ctx = _prepare_context(runtime_ctx)
    ctx.sql_backend.execute(f"GRANT READ_METADATA ON ANY FILE TO `{migrated_group.name_in_workspace}`")
    ctx.sql_backend.execute(f"GRANT SELECT ON ANONYMOUS FUNCTION TO `{migrated_group.name_in_workspace}`")

    MigrationState([migrated_group]).apply_to_groups_with_different_names(ctx.workspace_client)

    any_file_actual = {g.principal: g.action_type for g in ctx.grants_crawler.grants(any_file=True)}
    # Since the using the migrate permissions API, the group name in workspace should not have the permissions anymore
    assert migrated_group.name_in_workspace not in any_file_actual
    assert migrated_group.name_in_account in any_file_actual

    anonymous_function_actual = {g.principal: g.action_type for g in ctx.grants_crawler.grants(anonymous_function=True)}
    # Since the using the migrate permissions API, the group name in workspace should not have the permissions anymore
    assert migrated_group.name_in_workspace not in anonymous_function_actual
    assert migrated_group.name_in_account in anonymous_function_actual
    assert anonymous_function_actual[migrated_group.name_in_account] == "SELECT"


def test_permission_for_udfs_migration_api(
    runtime_ctx: MockRuntimeContext,
    migrated_group,
    _prepare_context: Callable[[MockRuntimeContext], MockRuntimeContext],
) -> None:
    # TODO: Move migrated group into `runtime_ctx` and follow the `make_` pattern
    ctx = _prepare_context(runtime_ctx)
    schema = ctx.make_schema()
    udf_a = ctx.make_udf(schema_name=schema.name)
    udf_b = ctx.make_udf(schema_name=schema.name)

    ctx.sql_backend.execute(f"GRANT SELECT ON FUNCTION {udf_a.full_name} TO `{migrated_group.name_in_workspace}`")
    ctx.sql_backend.execute(f"ALTER FUNCTION {udf_a.full_name} OWNER TO `{migrated_group.name_in_workspace}`")
    ctx.sql_backend.execute(
        f"GRANT READ_METADATA ON FUNCTION {udf_b.full_name} TO `{migrated_group.name_in_workspace}`"
    )

    all_initial_grants = {f"{g.principal}.{g.object_key}:{g.action_type}" for g in ctx.grants_crawler.snapshot()}
    assert f"{migrated_group.name_in_workspace}.{udf_a.full_name}:SELECT" in all_initial_grants
    assert f"{migrated_group.name_in_workspace}.{udf_a.full_name}:OWN" in all_initial_grants
    assert f"{migrated_group.name_in_workspace}.{udf_b.full_name}:READ_METADATA" in all_initial_grants

    MigrationState([migrated_group]).apply_to_groups_with_different_names(ctx.workspace_client)

    actual_udf_a_grants = defaultdict(set)
    for grant in ctx.grants_crawler.grants(catalog=schema.catalog_name, database=schema.name, udf=udf_a.name):
        actual_udf_a_grants[grant.principal].add(grant.action_type)
    assert {"SELECT", "OWN"} == actual_udf_a_grants[migrated_group.name_in_account]

    actual_udf_b_grants = defaultdict(set)
    for grant in ctx.grants_crawler.grants(catalog=schema.catalog_name, database=schema.name, udf=udf_b.name):
        actual_udf_b_grants[grant.principal].add(grant.action_type)
    assert {"READ_METADATA"} == actual_udf_b_grants[migrated_group.name_in_account]


def test_permission_for_files_anonymous_func(
    runtime_ctx: MockRuntimeContext,
    _prepare_context: Callable[[MockRuntimeContext], MockRuntimeContext],
) -> None:
    """Test permission for any file and anonymous function to be migrated."""
    ctx = _prepare_context(runtime_ctx)

    old, new = ctx.make_group(), ctx.make_group()
    ctx.sql_backend.execute(f"GRANT READ_METADATA ON ANY FILE TO `{old.display_name}`")
    ctx.sql_backend.execute(f"GRANT SELECT ON ANONYMOUS FUNCTION TO `{old.display_name}`")

    tacl_support = TableAclSupport(ctx.grants_crawler, ctx.sql_backend)
    apply_tasks(tacl_support, [MigratedGroup.partial_info(old, new)])

    any_file_actual = {g.principal: g.action_type for g in ctx.grants_crawler.grants(any_file=True)}
    assert old.display_name in any_file_actual, "Old group misses ANY FILE permission"
    assert new.display_name in any_file_actual, "New group misses ANY FILE permission"
    assert any_file_actual[old.display_name] == any_file_actual[new.display_name], "ANY FILE permissions differ"

    anonymous_function_actual = {g.principal: g.action_type for g in ctx.grants_crawler.grants(anonymous_function=True)}
    assert old.display_name in anonymous_function_actual, "Old group misses ANONYMOUS FUNCTION permission"
    assert new.display_name in anonymous_function_actual, "New group misses ANONYMOUS FUNCTION permission"
    assert anonymous_function_actual[new.display_name] == "SELECT", "New group misses SELECT permission"
    assert (
        anonymous_function_actual[old.display_name] == anonymous_function_actual[new.display_name]
    ), "ANONYMOUS FUNCTION permissions differ"


def test_hms2hms_owner_permissions(
    runtime_ctx: MockRuntimeContext,
    make_group_pair,
    _prepare_context: Callable[[MockRuntimeContext], MockRuntimeContext],
) -> None:
    # TODO: Move `make_group_pair` into `runtime_ctx`
    ctx = _prepare_context(runtime_ctx)
    first = make_group_pair()
    second = make_group_pair()
    third = make_group_pair()

    schema_a = ctx.make_schema()
    schema_b = ctx.make_schema()
    schema_c = ctx.make_schema()
    table_a = ctx.make_table(schema_name=schema_a.name)
    table_b = ctx.make_table(schema_name=schema_b.name)
    table_c = ctx.make_table(schema_name=schema_b.name, external=True)

    ctx.sql_backend.execute(f"GRANT USAGE ON SCHEMA {schema_a.name} TO `{first.name_in_workspace}`")
    ctx.sql_backend.execute(f"ALTER SCHEMA {schema_a.name} OWNER TO `{first.name_in_workspace}`")
    ctx.sql_backend.execute(f"GRANT ALL PRIVILEGES ON SCHEMA {schema_b.name} TO `{second.name_in_workspace}`")
    ctx.sql_backend.execute(
        f"GRANT USAGE, SELECT, MODIFY, CREATE, READ_METADATA, CREATE_NAMED_FUNCTION ON SCHEMA {schema_c.name} TO "
        f"`{third.name_in_workspace}`"
    )
    ctx.sql_backend.execute(f"GRANT SELECT ON TABLE {table_a.full_name} TO `{first.name_in_workspace}`")
    ctx.sql_backend.execute(
        f"GRANT SELECT, MODIFY, READ_METADATA ON TABLE {table_b.full_name} TO `{second.name_in_workspace}`"
    )
    ctx.sql_backend.execute(f"ALTER TABLE {table_b.full_name} OWNER TO `{second.name_in_workspace}`")
    ctx.sql_backend.execute(f"GRANT SELECT, MODIFY ON TABLE {table_c.full_name} TO `{third.name_in_workspace}`")

    original_table_grants = {
        "a": ctx.grants_crawler.for_table_info(table_a),
        "b": ctx.grants_crawler.for_table_info(table_b),
        "c": ctx.grants_crawler.for_table_info(table_c),
    }
    assert {"SELECT"} == original_table_grants["a"][first.name_in_workspace]
    assert {"MODIFY", "OWN", "READ_METADATA", "SELECT"} == original_table_grants["b"][second.name_in_workspace]
    assert {"MODIFY", "SELECT"} == original_table_grants["c"][third.name_in_workspace]

    original_schema_grants = {
        "a": ctx.grants_crawler.for_schema_info(schema_a),
        "b": ctx.grants_crawler.for_schema_info(schema_b),
    }
    assert {"OWN", "USAGE"} == original_schema_grants["a"][first.name_in_workspace]
    assert {"CREATE", "CREATE_NAMED_FUNCTION", "MODIFY", "READ_METADATA", "SELECT", "USAGE"} == original_schema_grants[
        "b"
    ][second.name_in_workspace]

    tacl_support = TableAclSupport(ctx.grants_crawler, ctx.sql_backend)

    apply_tasks(tacl_support, [first, second, third])

    new_table_grants = {
        "a": ctx.grants_crawler.for_table_info(table_a),
        "b": ctx.grants_crawler.for_table_info(table_b),
        "c": ctx.grants_crawler.for_table_info(table_c),
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
        "a": ctx.grants_crawler.for_schema_info(schema_a),
        "b": ctx.grants_crawler.for_schema_info(schema_b),
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


def test_permission_for_udfs(
    runtime_ctx: MockRuntimeContext,
    make_group_pair,
    _prepare_context: Callable[[MockRuntimeContext], MockRuntimeContext],
) -> None:
    # TODO: Move `make_group_pair` into `runtime_ctx`
    ctx = _prepare_context(runtime_ctx)
    group = make_group_pair()
    schema = ctx.make_schema()
    udf_a = ctx.make_udf(schema_name=schema.name)
    udf_b = ctx.make_udf(schema_name=schema.name)

    ctx.sql_backend.execute(f"GRANT SELECT ON FUNCTION {udf_a.full_name} TO `{group.name_in_workspace}`")
    ctx.sql_backend.execute(f"ALTER FUNCTION {udf_a.full_name} OWNER TO `{group.name_in_workspace}`")
    ctx.sql_backend.execute(f"GRANT READ_METADATA ON FUNCTION {udf_b.full_name} TO `{group.name_in_workspace}`")
    ctx.sql_backend.execute(f"DENY `SELECT` ON FUNCTION {udf_b.full_name} TO `{group.name_in_workspace}`")

    all_initial_grants = {f"{g.principal}.{g.object_key}:{g.action_type}" for g in ctx.grants_crawler.snapshot()}
    assert f"{group.name_in_workspace}.{udf_a.full_name}:SELECT" in all_initial_grants
    assert f"{group.name_in_workspace}.{udf_a.full_name}:OWN" in all_initial_grants
    assert f"{group.name_in_workspace}.{udf_b.full_name}:READ_METADATA" in all_initial_grants
    assert f"{group.name_in_workspace}.{udf_b.full_name}:DENIED_SELECT" in all_initial_grants

    tacl_support = TableAclSupport(ctx.grants_crawler, ctx.sql_backend)
    apply_tasks(tacl_support, [group])

    actual_udf_a_grants = defaultdict(set)
    for grant in ctx.grants_crawler.grants(catalog=schema.catalog_name, database=schema.name, udf=udf_a.name):
        actual_udf_a_grants[grant.principal].add(grant.action_type)
    assert {"SELECT", "OWN"} == actual_udf_a_grants[group.name_in_account]

    actual_udf_b_grants = defaultdict(set)
    for grant in ctx.grants_crawler.grants(catalog=schema.catalog_name, database=schema.name, udf=udf_b.name):
        actual_udf_b_grants[grant.principal].add(grant.action_type)
    assert {"READ_METADATA", "DENIED_SELECT"} == actual_udf_b_grants[group.name_in_account]


def test_verify_permission_for_udfs(
    runtime_ctx: MockRuntimeContext,
    _prepare_context: Callable[[MockRuntimeContext], MockRuntimeContext],
) -> None:
    ctx = _prepare_context(runtime_ctx)
    group = ctx.make_group()
    schema = ctx.make_schema()

    ctx.sql_backend.execute(f"GRANT SELECT ON SCHEMA {schema.name} TO `{group.display_name}`")

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

    tacl_support = TableAclSupport(ctx.grants_crawler, ctx.sql_backend)
    task = tacl_support.get_verify_task(item)
    result = task()

    assert result

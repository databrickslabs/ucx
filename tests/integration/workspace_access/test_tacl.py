import logging

from databricks.labs.ucx.hive_metastore import GrantsCrawler
from databricks.labs.ucx.workspace_access.groups import MigratedGroup, MigrationState
from databricks.labs.ucx.workspace_access.tacl import TableAclSupport

from ..conftest import StaticTablesCrawler

logger = logging.getLogger(__name__)


def test_permission_for_files_anonymous_func(sql_backend, inventory_schema, make_group):
    group_a = make_group()
    group_b = make_group()
    group_c = make_group()
    group_d = make_group()

    sql_backend.execute(f"GRANT READ_METADATA ON ANY FILE TO `{group_a.display_name}`")
    sql_backend.execute(f"GRANT SELECT ON ANONYMOUS FUNCTION TO `{group_c.display_name}`")

    tables = StaticTablesCrawler(sql_backend, inventory_schema, [])
    grants = GrantsCrawler(tables)

    tacl_support = TableAclSupport(grants, sql_backend)

    migration_state = MigrationState(
        [
            MigratedGroup.partial_info(group_a, group_b),
            MigratedGroup.partial_info(group_c, group_d),
        ]
    )
    for crawler_task in tacl_support.get_crawler_tasks():
        permission = crawler_task()
        apply_task = tacl_support.get_apply_task(permission, migration_state)
        if not apply_task:
            continue
        apply_task()

    any_file_actual = {}
    for any_file_grant in grants._grants(any_file=True):
        any_file_actual[any_file_grant.principal] = any_file_grant.action_type

    assert group_b.display_name in any_file_actual
    assert any_file_actual[group_b.display_name] == "READ_METADATA"
    assert any_file_actual[group_a.display_name] == any_file_actual[group_b.display_name]

    anonymous_function_actual = {}
    for ano_func_grant in grants._grants(anonymous_function=True):
        anonymous_function_actual[ano_func_grant.principal] = ano_func_grant.action_type

    assert group_d.display_name in anonymous_function_actual
    assert anonymous_function_actual[group_d.display_name] == "SELECT"
    assert anonymous_function_actual[group_c.display_name] == anonymous_function_actual[group_d.display_name]


def test_owner_permissions_for_tables_and_schemas(sql_backend, inventory_schema, make_schema, make_table, make_group):
    group_a = make_group()
    group_b = make_group()
    group_c = make_group()
    group_d = make_group()

    schema_info = make_schema()
    table_info = make_table(schema_name=schema_info.name)
    sql_backend.execute(f"ALTER TABLE {table_info.full_name} OWNER TO `{group_a.display_name}`")
    sql_backend.execute(f"ALTER DATABASE {schema_info.full_name} OWNER TO `{group_b.display_name}`")

    tables = StaticTablesCrawler(sql_backend, inventory_schema, [table_info])
    grants = GrantsCrawler(tables)

    original_table_grants = grants.for_table_info(table_info)
    assert "OWN" in original_table_grants[group_a.display_name]

    original_schema_grants = grants.for_schema_info(schema_info)
    assert "OWN" in original_schema_grants[group_b.display_name]

    tacl_support = TableAclSupport(grants, sql_backend)

    migration_state = MigrationState(
        [
            MigratedGroup.partial_info(group_a, group_c),
            MigratedGroup.partial_info(group_b, group_d),
        ]
    )

    for crawler_task in tacl_support.get_crawler_tasks():
        permission = crawler_task()
        apply_task = tacl_support.get_apply_task(permission, migration_state)
        if not apply_task:
            continue
        apply_task()

    table_grants = grants.for_table_info(table_info)
    assert group_a.display_name not in table_grants
    assert "OWN" in table_grants[group_c.display_name]

    schema_grants = grants.for_schema_info(schema_info)
    assert group_b.display_name not in schema_grants
    assert "OWN" in schema_grants[group_d.display_name]

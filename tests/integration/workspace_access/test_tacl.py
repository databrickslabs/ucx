import logging

from databricks.labs.ucx.hive_metastore import GrantsCrawler, TablesCrawler
from databricks.labs.ucx.workspace_access.groups import MigratedGroup, MigrationState
from databricks.labs.ucx.workspace_access.tacl import TableAclSupport

logger = logging.getLogger(__name__)


def test_owner_permissions_for_tables_and_schemas(sql_backend, inventory_schema, make_schema, make_table, make_group):
    group_a = make_group()
    group_b = make_group()
    group_c = make_group()
    group_d = make_group()

    schema_info = make_schema()
    table_info = make_table(schema_name=schema_info.name)
    sql_backend.execute(f"ALTER TABLE {table_info.full_name} OWNER TO `{group_a.display_name}`")
    sql_backend.execute(f"ALTER DATABASE {schema_info.full_name} OWNER TO `{group_b.display_name}`")

    tables = TablesCrawler(sql_backend, inventory_schema)
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

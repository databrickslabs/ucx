import json
import logging
from datetime import timedelta

from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service.iam import PermissionLevel

from databricks.labs.ucx.hive_metastore import GrantsCrawler, TablesCrawler
from databricks.labs.ucx.hive_metastore.grants import Grant
from databricks.labs.ucx.workspace_access.generic import (
    GenericPermissionsSupport,
    Listing,
)
from databricks.labs.ucx.workspace_access.groups import MigratedGroup, MigrationState
from databricks.labs.ucx.workspace_access.manager import PermissionManager
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


@retried(on=[NotFound, TimeoutError], timeout=timedelta(minutes=15))
def test_recover_permissions_from_grants(
    ws,
    sql_backend,
    inventory_schema,
    make_ucx_group,
    make_cluster_policy,
    make_cluster_policy_permissions,
    make_table,
):
    ws_group, _ = make_ucx_group()
    cluster_policy = make_cluster_policy()
    make_cluster_policy_permissions(
        object_id=cluster_policy.policy_id,
        permission_level=PermissionLevel.CAN_USE,
        group_name=ws_group.display_name,
    )

    dummy_table = make_table()
    sql_backend.execute(f"GRANT SELECT, MODIFY ON TABLE {dummy_table.full_name} TO `{ws_group.display_name}`")

    generic_permissions = GenericPermissionsSupport(
        ws, [Listing(ws.cluster_policies.list, "policy_id", "cluster-policies")]
    )
    tables = TablesCrawler(sql_backend, inventory_schema)
    grants = GrantsCrawler(tables)
    tacl = TableAclSupport(grants, sql_backend)

    # simulate: Table ACLs were not part of $inventory.permissions
    permission_manager = PermissionManager(sql_backend, inventory_schema, [generic_permissions])
    permission_manager.inventorize_permissions()

    object_types = set()
    for perm in permission_manager.load_all():
        object_types.add(perm.object_type)
    assert {"cluster-policies"} == object_types

    # simulate: recovery of Table ACLs as part of $inventory.permissions
    permission_manager = PermissionManager(sql_backend, inventory_schema, [tacl])
    permission_manager.inventorize_permissions()

    # simulate: normal flow
    permission_manager = PermissionManager(sql_backend, inventory_schema, [generic_permissions, tacl])
    object_types = set()
    permissions_list = permission_manager.load_all()
    for perm in permissions_list:
        object_types.add(perm.object_type)

    assert "TABLE" in object_types
    assert "DATABASE" in object_types
    assert "cluster-policies" in object_types

    actual_raw_permissions = next(
        obj.raw
        for obj in permissions_list
        if obj.object_id == dummy_table.full_name
        and obj.object_type == "TABLE"
        and json.loads(obj.raw)["principal"] == ws_group.display_name
    )

    assert Grant(
        principal=ws_group.display_name,
        action_type="MODIFY, SELECT",
        catalog=dummy_table.catalog_name.lower(),
        database=dummy_table.schema_name.lower(),
        table=dummy_table.name.lower(),
        view=None,
        any_file=False,
        anonymous_function=False,
    ) == Grant(**json.loads(actual_raw_permissions))

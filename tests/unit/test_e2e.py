from databricks.sdk.service.compute import ClusterDetails
from databricks.sdk.service.iam import (
    AccessControlResponse,
    Group,
    ObjectPermissions,
    Permission,
    PermissionLevel,
    ResourceMeta,
)
from pytest_mock import MockerFixture

from uc_migration_toolkit.config import (
    GroupsConfig,
    InventoryConfig,
    InventoryTable,
    MigrationConfig,
)
from uc_migration_toolkit.providers.client import provider
from uc_migration_toolkit.providers.spark import SparkMixin
from uc_migration_toolkit.toolkits.group_migration import GroupMigrationToolkit


def test_e2e(mocker: MockerFixture, spark):
    config = MigrationConfig(
        inventory=InventoryConfig(table=InventoryTable(catalog="test", database="default", name="ucx_inventory")),
        groups=GroupsConfig(auto=True),
        with_table_acls=False,
    )

    test_table_name = "default.ucx_inventory"
    # mock spark ops
    spark_mock = mocker.patch.object(SparkMixin, "_initialize_spark")
    spark_mock.return_value = spark
    # mocking it for local run

    spark_str = mocker.patch.object(InventoryTable, "__repr__")
    spark_str.return_value = test_table_name

    mocker.patch.object(provider, "set_ws_client")
    ws_client_mock = mocker.patch.object(provider, "_ws_client")

    ws_client_mock.groups.list.return_value = [
        Group(display_name="group1", meta=ResourceMeta(resource_type="WorkspaceGroup"), id="1"),
        Group(display_name="group2", meta=ResourceMeta(resource_type="WorkspaceGroup"), id="2"),
        Group(display_name="group3", meta=ResourceMeta(resource_type="WorkspaceGroup"), id="3"),
        # account group
        Group(display_name="group3", meta=ResourceMeta(resource_type="Group"), id="3"),
    ]

    ws_client_mock.clusters.list.return_value = [
        ClusterDetails(cluster_id="1", cluster_name="cluster1"),
        ClusterDetails(cluster_id="2", cluster_name="cluster2"),
        ClusterDetails(cluster_id="3", cluster_name="cluster3"),
    ]

    ws_client_mock.permissions.get.side_effect = [
        ObjectPermissions(
            object_type="cluster",
            object_id="1",
            access_control_list=[
                AccessControlResponse(
                    group_name="group1",
                    all_permissions=[Permission(inherited=False, permission_level=PermissionLevel.CAN_MANAGE)],
                )
            ],
        ),
        ObjectPermissions(
            object_type="cluster",
            object_id="2",
            access_control_list=[
                AccessControlResponse(
                    group_name="group1",
                    all_permissions=[Permission(inherited=False, permission_level=PermissionLevel.CAN_MANAGE)],
                )
            ],
        ),
        ObjectPermissions(
            object_type="cluster",
            object_id="3",
            access_control_list=[
                AccessControlResponse(
                    group_name="group1",
                    all_permissions=[Permission(inherited=False, permission_level=PermissionLevel.CAN_MANAGE)],
                )
            ],
        ),
    ]

    toolkit = GroupMigrationToolkit(config)
    toolkit.validate_groups()
    toolkit.cleanup_inventory_table()
    toolkit.inventorize_permissions()

    assert spark.table(test_table_name).count() == 3
    assert spark.table(test_table_name).where("object_id = '1'").count() == 1

    toolkit.create_or_update_backup_groups()
    toolkit.apply_backup_group_permissions()
    toolkit.replace_workspace_groups_with_account_groups()
    toolkit.apply_account_group_permissions()
    toolkit.delete_backup_groups()
    toolkit.cleanup_inventory_table()

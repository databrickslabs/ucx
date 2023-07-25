import pytest
from conftest import EnvironmentInfo
from databricks.sdk.service.compute import ClusterDetails
from pyspark.errors import AnalysisException

from uc_migration_toolkit.config import (
    GroupsConfig,
    InventoryConfig,
    InventoryTable,
    MigrationConfig,
)
from uc_migration_toolkit.managers.inventory.types import RequestObjectType
from uc_migration_toolkit.providers.client import ImprovedWorkspaceClient
from uc_migration_toolkit.providers.logger import logger
from uc_migration_toolkit.toolkits.group_migration import GroupMigrationToolkit


def test_e2e(
    env: EnvironmentInfo, inventory_table: InventoryTable, ws: ImprovedWorkspaceClient, clusters: list[ClusterDetails]
):
    logger.info(f"Test environment: {env.test_uid}")

    config = MigrationConfig(
        with_table_acls=False,
        inventory=InventoryConfig(table=inventory_table),
        groups=GroupsConfig(selected=[g[0].display_name for g in env.groups]),
        auth=None,
    )
    logger.info(f"Starting e2e with config: {config.to_json()}")
    toolkit = GroupMigrationToolkit(config)
    toolkit.prepare_groups_in_environment()

    logger.info("Verifying that the groups were created")

    assert len(ws.groups.list(filter=f"displayName sw '{config.groups.backup_group_prefix}{env.test_uid}'")) == len(
        toolkit.group_manager.migration_groups_provider.groups
    )

    assert len(ws.groups.list(filter=f"displayName sw '{env.test_uid}'")) == len(
        toolkit.group_manager.migration_groups_provider.groups
    )

    assert len(ws.list_account_level_groups(filter=f"displayName sw '{env.test_uid}'")) == len(
        toolkit.group_manager.migration_groups_provider.groups
    )

    logger.info("Verifying that the groups were created - done")

    toolkit.cleanup_inventory_table()

    with pytest.raises(AnalysisException):
        toolkit.table_manager.spark.catalog.getTable(toolkit.table_manager.config.table.to_spark())

    toolkit.inventorize_permissions()

    logger.info("Verifying that the permissions were inventorized correctly")
    saved_permissions = toolkit.table_manager.load_all()

    for cluster in clusters:
        cluster_permissions = ws.permissions.get(RequestObjectType.CLUSTERS, cluster.cluster_id)
        relevant_permission = next(filter(lambda p: p.object_id == cluster.cluster_id, saved_permissions), None)
        assert relevant_permission is not None, f"Cluster {cluster.cluster_id} permissions were not inventorized"
        assert relevant_permission.typed_object_permissions == cluster_permissions

    logger.info("Permissions were inventorized properly")

    toolkit.apply_permissions_to_backup_groups()
    toolkit.replace_workspace_groups_with_account_groups()
    # toolkit.apply_permissions_to_backup_groups()
    # toolkit.delete_backup_groups()

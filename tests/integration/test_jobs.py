import pytest
from pyspark.errors import AnalysisException

from databricks.labs.ucx.config import (
    ConnectConfig,
    GroupsConfig,
    InventoryConfig,
    InventoryTable,
    MigrationConfig,
)
from databricks.labs.ucx.inventory.types import RequestObjectType
from databricks.labs.ucx.providers.client import ImprovedWorkspaceClient
from databricks.labs.ucx.providers.logger import logger
from databricks.labs.ucx.toolkits import GroupMigrationToolkit

from .test_e2e import _verify_group_permissions, _verify_roles_and_entitlements
from .utils import EnvironmentInfo


def test_jobs(
    env: EnvironmentInfo,
    inventory_table: InventoryTable,
    ws: ImprovedWorkspaceClient,
    jobs,
):
    logger.debug(f"Test environment: {env.test_uid}")

    config = MigrationConfig(
        connect=ConnectConfig.from_databricks_config(ws.config),
        with_table_acls=False,
        inventory=InventoryConfig(table=inventory_table),
        groups=GroupsConfig(selected=[g[0].display_name for g in env.groups]),
        auth=None,
        log_level="TRACE",
    )
    toolkit = GroupMigrationToolkit(config)
    toolkit.prepare_environment()

    logger.debug("Verifying that the groups were created")

    assert len(ws.groups.list(filter=f"displayName sw '{config.groups.backup_group_prefix}{env.test_uid}'")) == len(
        toolkit.group_manager.migration_groups_provider.groups
    )

    assert len(ws.groups.list(filter=f"displayName sw '{env.test_uid}'")) == len(
        toolkit.group_manager.migration_groups_provider.groups
    )

    assert len(ws.list_account_level_groups(filter=f"displayName sw '{env.test_uid}'")) == len(
        toolkit.group_manager.migration_groups_provider.groups
    )

    for _info in toolkit.group_manager.migration_groups_provider.groups:
        _ws = ws.groups.get(id=_info.workspace.id)
        _backup = ws.groups.get(id=_info.backup.id)
        _ws_members = sorted([m.value for m in _ws.members])
        _backup_members = sorted([m.value for m in _backup.members])
        assert _ws_members == _backup_members

    logger.debug("Verifying that the groups were created - done")

    toolkit.cleanup_inventory_table()

    with pytest.raises(AnalysisException):
        toolkit.table_manager.spark.catalog.getTable(toolkit.table_manager.config.table.to_spark())

    toolkit.inventorize_permissions()

    toolkit.apply_permissions_to_backup_groups()

    verifiable_objects = [
        (jobs, "job_id", RequestObjectType.JOBS),
    ]
    for _objects, id_attribute, request_object_type in verifiable_objects:
        _verify_group_permissions(_objects, id_attribute, request_object_type, ws, toolkit, "backup")

    _verify_roles_and_entitlements(toolkit.group_manager.migration_groups_provider, ws, "backup")

    toolkit.replace_workspace_groups_with_account_groups()

    new_groups = list(ws.groups.list(filter=f"displayName sw '{env.test_uid}'", attributes="displayName,meta"))
    assert len(new_groups) == len(toolkit.group_manager.migration_groups_provider.groups)
    assert all(g.meta.resource_type == "Group" for g in new_groups)

    toolkit.apply_permissions_to_account_groups()

    for _objects, id_attribute, request_object_type in verifiable_objects:
        _verify_group_permissions(_objects, id_attribute, request_object_type, ws, toolkit, "account")

    _verify_roles_and_entitlements(toolkit.group_manager.migration_groups_provider, ws, "account")

    toolkit.delete_backup_groups()

    backup_groups = list(
        ws.groups.list(
            filter=f"displayName sw '{config.groups.backup_group_prefix}{env.test_uid}'", attributes="displayName,meta"
        )
    )
    assert len(backup_groups) == 0

    toolkit.cleanup_inventory_table()

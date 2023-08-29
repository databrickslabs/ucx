import logging
from typing import Literal

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import (
    AccessControlRequest,
    AccessControlResponse,
    ObjectPermissions,
    Permission,
)
from databricks.sdk.service.workspace import SecretScope
from pyspark.errors import AnalysisException

from databricks.labs.ucx.config import (
    ConnectConfig,
    GroupsConfig,
    InventoryConfig,
    InventoryTable,
    MigrationConfig,
)
from databricks.labs.ucx.inventory.types import RequestObjectType
from databricks.labs.ucx.providers.groups_info import GroupMigrationState
from databricks.labs.ucx.toolkits.group_migration import GroupMigrationToolkit
from databricks.labs.ucx.utils import safe_get_acls

from .utils import EnvironmentInfo, WorkspaceObjects

logger = logging.getLogger(__name__)


def _verify_group_permissions(
    objects: list | WorkspaceObjects | None,
    id_attribute: str,
    request_object_type: RequestObjectType | None,
    ws: WorkspaceClient,
    toolkit: GroupMigrationToolkit,
    target: Literal["backup", "account"],
):
    logger.debug(
        f"Verifying that the permissions of object "
        f"{request_object_type or id_attribute} were applied to {target} groups"
    )

    if id_attribute == "workspace_objects":
        _workspace_objects: WorkspaceObjects = objects

        # list of groups that source the permissions
        comparison_base = [
            getattr(mi, "workspace" if target == "backup" else "backup")
            for mi in toolkit.group_manager.migration_groups_provider.groups
        ]
        # list of groups that are the target of the permissions
        comparison_target = [getattr(mi, target) for mi in toolkit.group_manager.migration_groups_provider.groups]

        root_permissions = ws.permissions.get(
            request_object_type=RequestObjectType.DIRECTORIES, request_object_id=_workspace_objects.root_dir.object_id
        )
        base_group_names = [g.display_name for g in comparison_base]
        target_group_names = [g.display_name for g in comparison_target]

        base_acls = [a for a in root_permissions.access_control_list if a.group_name in base_group_names]

        target_acls = [a for a in root_permissions.access_control_list if a.group_name in target_group_names]

        assert len(base_acls) == len(target_acls)

    elif id_attribute == "secret_scopes":
        _scopes: list[SecretScope] = objects
        comparison_base = [
            getattr(mi, "workspace" if target == "backup" else "backup")
            for mi in toolkit.group_manager.migration_groups_provider.groups
        ]

        comparison_target = [getattr(mi, target) for mi in toolkit.group_manager.migration_groups_provider.groups]

        for scope in _scopes:
            for base_group, target_group in zip(comparison_base, comparison_target, strict=True):
                base_acl = safe_get_acls(ws, scope.name, base_group.display_name)
                target_acl = safe_get_acls(ws, scope.name, target_group.display_name)

                if base_acl:
                    if not target_acl:
                        msg = "Target ACL is empty, while base ACL is not"
                        raise AssertionError(msg)

                    assert (
                        base_acl.permission == target_acl.permission
                    ), f"Target permissions were not applied correctly for scope {scope.name}"

    elif id_attribute in ("tokens", "passwords"):
        _typed_objects: list[AccessControlRequest] = objects
        ws_permissions = [
            AccessControlResponse(
                all_permissions=[
                    Permission(permission_level=o.permission_level, inherited=False, inherited_from_object=None)
                ],
                group_name=o.group_name,
            )
            for o in _typed_objects
        ]

        target_permissions = list(
            filter(
                lambda p: p.group_name
                in [getattr(g, target).display_name for g in toolkit.group_manager.migration_groups_provider.groups],
                ws.permissions.get(
                    request_object_type=request_object_type, request_object_id=id_attribute
                ).access_control_list,
            )
        )

        sorted_ws = sorted(ws_permissions, key=lambda p: p.group_name)
        sorted_target = sorted(target_permissions, key=lambda p: p.group_name)

        assert [p.all_permissions for p in sorted_ws] == [p.all_permissions for p in sorted_target]
    else:
        for _object in objects:
            _object_permissions: ObjectPermissions = ws.permissions.get(
                request_object_type, getattr(_object, id_attribute)
            )
            for migration_info in toolkit.group_manager.migration_groups_provider.groups:
                target_permissions = sorted(
                    [
                        p
                        for p in _object_permissions.access_control_list
                        if p.group_name == getattr(migration_info, target).display_name
                    ],
                    key=lambda p: p.group_name,
                )

                source_permissions = sorted(
                    [
                        p
                        for p in _object_permissions.access_control_list
                        if p.group_name == migration_info.workspace.display_name
                    ],
                    key=lambda p: p.group_name,
                )

                assert len(target_permissions) == len(
                    source_permissions
                ), f"Target permissions were not applied correctly for object {_object}"

                assert [t.all_permissions for t in target_permissions] == [
                    s.all_permissions for s in source_permissions
                ], f"Target permissions were not applied correctly for object {_object}"


def _verify_roles_and_entitlements(
    migration_state: GroupMigrationState,
    ws: WorkspaceClient,
    target: Literal["backup", "account"],
):
    for el in migration_state.groups:
        comparison_base = getattr(el, "workspace" if target == "backup" else "backup")
        comparison_target = getattr(el, target)

        base_group_info = ws.groups.get(comparison_base.id)
        target_group_info = ws.groups.get(comparison_target.id)

        assert base_group_info.roles == target_group_info.roles
        assert base_group_info.entitlements == target_group_info.entitlements


def test_e2e(
    env: EnvironmentInfo,
    inventory_table: InventoryTable,
    ws: WorkspaceClient,
    verifiable_objects: list[tuple[list, str, RequestObjectType | None]],
):
    logger.debug(f"Test environment: {env.test_uid}")

    config = MigrationConfig(
        connect=ConnectConfig.from_databricks_config(ws.config),
        with_table_acls=False,
        inventory=InventoryConfig(table=inventory_table),
        groups=GroupsConfig(selected=[g[0].display_name for g in env.groups]),
        auth=None,
        log_level="DEBUG",
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

    assert len(toolkit.group_manager._list_account_level_groups(filter=f"displayName sw '{env.test_uid}'")) == len(
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

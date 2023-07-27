from typing import Literal

import pytest
from databricks.sdk.core import DatabricksError
from databricks.sdk.service.iam import (
    AccessControlRequest,
    AccessControlResponse,
    Group,
    ObjectPermissions,
    Permission,
)
from databricks.sdk.service.workspace import AclItem, SecretScope
from pyspark.errors import AnalysisException
from utils import EnvironmentInfo

from uc_migration_toolkit.config import (
    GroupsConfig,
    InventoryConfig,
    InventoryTable,
    MigrationConfig,
)
from uc_migration_toolkit.managers.group import MigrationGroupInfo
from uc_migration_toolkit.managers.inventory.types import RequestObjectType
from uc_migration_toolkit.providers.client import ImprovedWorkspaceClient
from uc_migration_toolkit.providers.logger import logger
from uc_migration_toolkit.toolkits.group_migration import GroupMigrationToolkit


def _verify_group_permissions(
    objects: list | None,
    id_attribute: str,
    request_object_type: RequestObjectType | None,
    ws: ImprovedWorkspaceClient,
    toolkit: GroupMigrationToolkit,
    target: Literal["backup", "account"],
):
    logger.debug(f"Verifying that the permissions of object {request_object_type} were applied to {target} groups")

    if id_attribute == "secret_scopes":
        _scopes: list[SecretScope] = objects
        comparison_base = [
            getattr(mi, "workspace" if target == "backup" else "backup")
            for mi in toolkit.group_manager.migration_groups_provider.groups
        ]

        comparison_target = [getattr(mi, target) for mi in toolkit.group_manager.migration_groups_provider.groups]

        def _safe_get_acls(_scope: SecretScope, _group: Group) -> AclItem | None:
            try:
                return ws.secrets.get_acl(scope=_scope.name, principal=_group.display_name)
            except DatabricksError:
                return None

        for scope in _scopes:
            for base_group, target_group in zip(comparison_base, comparison_target, strict=True):
                base_acl = _safe_get_acls(scope, base_group)
                target_acl = _safe_get_acls(scope, target_group)

                if not base_acl:
                    assert not target_acl
                else:
                    assert base_acl.permission == target_acl.permission

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
    groups: list[MigrationGroupInfo], ws: ImprovedWorkspaceClient, target: Literal["backup", "account"]
):
    for migration_info in groups:
        workspace_group = migration_info.workspace
        target_group = ws.groups.get(getattr(migration_info, target).id)

        assert workspace_group.roles == target_group.roles
        assert workspace_group.entitlements == target_group.entitlements


def test_e2e(
    env: EnvironmentInfo,
    inventory_table: InventoryTable,
    ws: ImprovedWorkspaceClient,
    verifiable_objects: list[tuple[list, str, RequestObjectType | None]],
):
    logger.debug(f"Test environment: {env.test_uid}")

    config = MigrationConfig(
        with_table_acls=False,
        inventory=InventoryConfig(table=inventory_table),
        groups=GroupsConfig(selected=[g[0].display_name for g in env.groups]),
        auth=None,
    )
    logger.debug(f"Starting e2e with config: {config.to_json()}")
    toolkit = GroupMigrationToolkit(config)
    toolkit.prepare_groups_in_environment()

    logger.debug("Verifying that the groups were created")
    _verify_roles_and_entitlements(toolkit.group_manager.migration_groups_provider.groups, ws, "backup")

    assert len(ws.groups.list(filter=f"displayName sw '{config.groups.backup_group_prefix}{env.test_uid}'")) == len(
        toolkit.group_manager.migration_groups_provider.groups
    )

    assert len(ws.groups.list(filter=f"displayName sw '{env.test_uid}'")) == len(
        toolkit.group_manager.migration_groups_provider.groups
    )

    assert len(ws.list_account_level_groups(filter=f"displayName sw '{env.test_uid}'")) == len(
        toolkit.group_manager.migration_groups_provider.groups
    )

    logger.debug("Verifying that the groups were created - done")

    toolkit.cleanup_inventory_table()

    with pytest.raises(AnalysisException):
        toolkit.table_manager.spark.catalog.getTable(toolkit.table_manager.config.table.to_spark())

    toolkit.inventorize_permissions()

    toolkit.apply_permissions_to_backup_groups()

    for _objects, id_attribute, request_object_type in verifiable_objects:
        _verify_group_permissions(_objects, id_attribute, request_object_type, ws, toolkit, "backup")

    toolkit.replace_workspace_groups_with_account_groups()

    new_groups = list(ws.groups.list(filter=f"displayName sw '{env.test_uid}'", attributes="displayName,meta"))
    assert len(new_groups) == len(toolkit.group_manager.migration_groups_provider.groups)
    assert all(g.meta.resource_type == "Group" for g in new_groups)
    _verify_roles_and_entitlements(toolkit.group_manager.migration_groups_provider.groups, ws, "account")

    toolkit.apply_permissions_to_account_groups()

    for _objects, id_attribute, request_object_type in verifiable_objects:
        _verify_group_permissions(_objects, id_attribute, request_object_type, ws, toolkit, "account")

    toolkit.delete_backup_groups()

    backup_groups = list(
        ws.groups.list(
            filter=f"displayName sw '{config.groups.backup_group_prefix}{env.test_uid}'", attributes="displayName,meta"
        )
    )
    assert len(backup_groups) == 0

    toolkit.cleanup_inventory_table()

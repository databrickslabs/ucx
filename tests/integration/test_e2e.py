import logging
import os
import random
from typing import Literal

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace
from databricks.sdk.service.iam import (
    AccessControlRequest,
    AccessControlResponse,
    Permission,
    PermissionLevel,
)
from pyspark.errors import AnalysisException

from databricks.labs.ucx.config import (
    ConnectConfig,
    GroupsConfig,
    InventoryConfig,
    InventoryTable,
    MigrationConfig,
    TaclConfig,
)
from databricks.labs.ucx.inventory.types import RequestObjectType
from databricks.labs.ucx.providers.groups_info import GroupMigrationState
from databricks.labs.ucx.toolkits.group_migration import GroupMigrationToolkit

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
        for scope_name in objects:
            toolkit.permissions_manager.verify_applied_scope_acls(
                scope_name, toolkit.group_manager.migration_groups_provider, target
            )

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
            toolkit.permissions_manager.verify_applied_permissions(
                request_object_type,
                getattr(_object, id_attribute),
                toolkit.group_manager.migration_groups_provider,
                target,
            )


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
    make_instance_pool,
    make_instance_pool_permissions,
    make_cluster,
    make_cluster_permissions,
    make_model,
    make_model_permissions,
    make_secret_scope,
    make_secret_scope_acl,
):
    logger.debug(f"Test environment: {env.test_uid}")
    ws_group = env.groups[0][0]

    pool = make_instance_pool()
    make_instance_pool_permissions(
        object_id=pool.instance_pool_id,
        permission_level=random.choice([PermissionLevel.CAN_ATTACH_TO, PermissionLevel.CAN_MANAGE]),
        group_name=ws_group.display_name,
    )
    verifiable_objects.append(([pool], "instance_pool_id", RequestObjectType.INSTANCE_POOLS))

    cluster = make_cluster(instance_pool_id=os.environ["TEST_INSTANCE_POOL_ID"], single_node=True)
    make_cluster_permissions(
        object_id=cluster.cluster_id,
        permission_level=random.choice(
            [PermissionLevel.CAN_ATTACH_TO, PermissionLevel.CAN_MANAGE, PermissionLevel.CAN_RESTART]
        ),
        group_name=ws_group.display_name,
    )
    verifiable_objects.append(
        ([cluster], "cluster_id", RequestObjectType.CLUSTERS),
    )

    model = make_model()
    make_model_permissions(
        object_id=model.model_id,
        permission_level=random.choice(
            [
                PermissionLevel.CAN_READ,
                PermissionLevel.CAN_MANAGE,
                PermissionLevel.CAN_MANAGE_PRODUCTION_VERSIONS,
                PermissionLevel.CAN_MANAGE_STAGING_VERSIONS,
            ]
        ),
        group_name=ws_group.display_name,
    )
    verifiable_objects.append(
        ([model], "model_id", RequestObjectType.REGISTERED_MODELS),
    )

    scope = make_secret_scope()
    make_secret_scope_acl(scope=scope, principal=ws_group.display_name, permission=workspace.AclPermission.WRITE)
    verifiable_objects.append(([scope], "secret_scopes", None))

    config = MigrationConfig(
        connect=ConnectConfig.from_databricks_config(ws.config),
        inventory=InventoryConfig(table=inventory_table),
        groups=GroupsConfig(selected=[g[0].display_name for g in env.groups]),
        tacl=TaclConfig(auto=True),
        log_level="DEBUG",
    )
    toolkit = GroupMigrationToolkit(config)
    toolkit.prepare_environment()

    group_migration_state = toolkit.group_manager.migration_groups_provider
    for _info in group_migration_state.groups:
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

    _verify_roles_and_entitlements(group_migration_state, ws, "backup")

    toolkit.replace_workspace_groups_with_account_groups()

    new_groups = [
        _ for _ in ws.groups.list(attributes="displayName,meta") if group_migration_state.is_in_scope("account", _)
    ]
    assert len(new_groups) == len(group_migration_state.groups)
    assert all(g.meta.resource_type == "Group" for g in new_groups)

    toolkit.apply_permissions_to_account_groups()

    for _objects, id_attribute, request_object_type in verifiable_objects:
        _verify_group_permissions(_objects, id_attribute, request_object_type, ws, toolkit, "account")

    _verify_roles_and_entitlements(group_migration_state, ws, "account")

    toolkit.delete_backup_groups()

    backup_groups = [
        _ for _ in ws.groups.list(attributes="displayName,meta") if group_migration_state.is_in_scope("backup", _)
    ]
    assert len(backup_groups) == 0

    toolkit.cleanup_inventory_table()

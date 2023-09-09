import logging
import os
import random
from typing import Literal

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import iam, workspace
from databricks.sdk.service.iam import PermissionLevel
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
    toolkit: GroupMigrationToolkit,
    target: Literal["backup", "account"],
):
    logger.debug(
        f"Verifying that the permissions of object "
        f"{request_object_type or id_attribute} were applied to {target} groups"
    )

    if id_attribute == "secret_scopes":
        for scope_name in objects:
            toolkit.permissions_manager.verify_applied_scope_acls(
                scope_name, toolkit.group_manager.migration_groups_provider, target
            )
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
    make_instance_pool,
    make_instance_pool_permissions,
    make_cluster,
    make_cluster_permissions,
    make_cluster_policy,
    make_cluster_policy_permissions,
    make_model,
    make_registered_model_permissions,
    make_experiment,
    make_experiment_permissions,
    make_job,
    make_job_permissions,
    make_notebook,
    make_notebook_permissions,
    make_directory,
    make_directory_permissions,
    make_pipeline,
    make_pipeline_permissions,
    make_secret_scope,
    make_secret_scope_acl,
    make_authorization_permissions,
    make_warehouse,
    make_warehouse_permissions,
):
    logger.debug(f"Test environment: {env.test_uid}")
    ws_group = env.groups[0][0]

    verifiable_objects = []

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

    cluster_policy = make_cluster_policy()
    make_cluster_policy_permissions(
        object_id=cluster_policy.policy_id,
        permission_level=random.choice([PermissionLevel.CAN_USE]),
        group_name=ws_group.display_name,
    )
    verifiable_objects.append(
        ([cluster_policy], "policy_id", RequestObjectType.CLUSTER_POLICIES),
    )

    model = make_model()
    make_registered_model_permissions(
        object_id=model.id,
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
        ([model], "id", RequestObjectType.REGISTERED_MODELS),
    )

    experiment = make_experiment()
    make_experiment_permissions(
        object_id=experiment.experiment_id,
        permission_level=random.choice(
            [PermissionLevel.CAN_MANAGE, PermissionLevel.CAN_READ, PermissionLevel.CAN_EDIT]
        ),
        group_name=ws_group.display_name,
    )
    verifiable_objects.append(
        ([experiment], "experiment_id", RequestObjectType.EXPERIMENTS),
    )

    directory = make_directory()
    make_directory_permissions(
        object_id=directory,
        permission_level=random.choice(
            [PermissionLevel.CAN_READ, PermissionLevel.CAN_MANAGE, PermissionLevel.CAN_EDIT, PermissionLevel.CAN_RUN]
        ),
        group_name=ws_group.display_name,
    )

    verifiable_objects.append(
        ([ws.workspace.get_status(directory)], "object_id", RequestObjectType.DIRECTORIES),
    )

    notebook = make_notebook(path=f"{directory}/sample.py")
    make_notebook_permissions(
        object_id=notebook,
        permission_level=random.choice(
            [PermissionLevel.CAN_READ, PermissionLevel.CAN_MANAGE, PermissionLevel.CAN_EDIT, PermissionLevel.CAN_RUN]
        ),
        group_name=ws_group.display_name,
    )
    verifiable_objects.append(
        ([ws.workspace.get_status(notebook)], "object_id", RequestObjectType.NOTEBOOKS),
    )

    job = make_job()
    make_job_permissions(
        object_id=job.job_id,
        permission_level=random.choice(
            [PermissionLevel.CAN_VIEW, PermissionLevel.CAN_MANAGE_RUN, PermissionLevel.CAN_MANAGE]
        ),
        group_name=ws_group.display_name,
    )
    verifiable_objects.append(
        ([job], "job_id", RequestObjectType.JOBS),
    )

    pipeline = make_pipeline()
    make_pipeline_permissions(
        object_id=pipeline.pipeline_id,
        permission_level=random.choice([PermissionLevel.CAN_VIEW, PermissionLevel.CAN_RUN, PermissionLevel.CAN_MANAGE]),
        group_name=ws_group.display_name,
    )
    verifiable_objects.append(
        ([pipeline], "pipeline_id", RequestObjectType.PIPELINES),
    )

    scope = make_secret_scope()
    make_secret_scope_acl(scope=scope, principal=ws_group.display_name, permission=workspace.AclPermission.WRITE)
    verifiable_objects.append(([scope], "secret_scopes", None))

    make_authorization_permissions(
        object_id="tokens",
        permission_level=PermissionLevel.CAN_USE,
        group_name=ws_group.display_name,
    )
    verifiable_objects.append(
        ([iam.ObjectPermissions(object_id="tokens")], "object_id", RequestObjectType.AUTHORIZATION)
    )

    warehouse = make_warehouse()
    make_warehouse_permissions(
        object_id=warehouse.id,
        permission_level=random.choice([PermissionLevel.CAN_USE, PermissionLevel.CAN_MANAGE]),
        group_name=ws_group.display_name,
    )
    verifiable_objects.append(
        ([warehouse], "id", RequestObjectType.SQL_WAREHOUSES),
    )

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
        _verify_group_permissions(_objects, id_attribute, request_object_type, toolkit, "backup")

    _verify_roles_and_entitlements(group_migration_state, ws, "backup")

    toolkit.replace_workspace_groups_with_account_groups()

    new_groups = [
        _ for _ in ws.groups.list(attributes="displayName,meta") if group_migration_state.is_in_scope("account", _)
    ]
    assert len(new_groups) == len(group_migration_state.groups)
    assert all(g.meta.resource_type == "Group" for g in new_groups)

    toolkit.apply_permissions_to_account_groups()

    for _objects, id_attribute, request_object_type in verifiable_objects:
        _verify_group_permissions(_objects, id_attribute, request_object_type, toolkit, "account")

    _verify_roles_and_entitlements(group_migration_state, ws, "account")

    toolkit.delete_backup_groups()

    backup_groups = [
        _ for _ in ws.groups.list(attributes="displayName,meta") if group_migration_state.is_in_scope("backup", _)
    ]
    assert len(backup_groups) == 0

    toolkit.cleanup_inventory_table()

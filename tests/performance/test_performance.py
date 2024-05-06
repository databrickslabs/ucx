import json
import logging
from collections.abc import Callable
from dataclasses import dataclass
from functools import partial
from time import process_time

import pytest
from databricks.labs.blueprint.parallel import Threads
from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import iam

from databricks.labs.ucx.contexts.workspace_cli import WorkspaceContext
from databricks.labs.ucx.workspace_access.base import Permissions
from databricks.labs.ucx.workspace_access.groups import MigratedGroup, MigrationState

logger = logging.getLogger(__name__)

NB_OF_TEST_WS_OBJECTS = 100


@dataclass
class WorkspaceObject:
    fixture: Callable
    permissions: list[iam.PermissionLevel]
    id_attribute: str
    type: str


@pytest.fixture
def migrated_group_experimental(acc, ws, make_group, make_acc_group):
    """Create a pair of groups in workspace and account. Assign account group to workspace."""
    ws_group = make_group()
    acc_group = make_acc_group()
    acc.workspace_assignment.update(ws.get_workspace_id(), acc_group.id, [iam.WorkspacePermission.USER])
    return MigratedGroup.partial_info(ws_group, acc_group)


def test_apply_group_permissions_experimental_performance(
    ws: WorkspaceClient,
    sql_backend: SqlBackend,
    inventory_schema,
    migrated_group,
    migrated_group_experimental,
    make_experiment,
    make_model,
    make_cluster_policy,
    env_or_skip,
):
    # Making sure this test can only be launched from local
    env_or_skip("IDE_PROJECT_ROOTS")
    ws_objects = [
        WorkspaceObject(partial(make_experiment), [iam.PermissionLevel.CAN_MANAGE], "experiment_id", "experiments"),
        WorkspaceObject(
            partial(make_model), [iam.PermissionLevel.CAN_MANAGE_PRODUCTION_VERSIONS], "id", "registered-models"
        ),
        WorkspaceObject(partial(make_cluster_policy), [iam.PermissionLevel.CAN_USE], "policy_id", "cluster-policies"),
    ]
    for ws_object in ws_objects:
        create_ws_objects_parallel(
            ws,
            sql_backend,
            inventory_schema,
            NB_OF_TEST_WS_OBJECTS,
            ws_object.fixture,
            ws_object.permissions,
            ws_object.id_attribute,
            ws_object.type,
            [migrated_group.name_in_workspace, migrated_group_experimental.name_in_workspace],
        )

    start = process_time()
    MigrationState([migrated_group_experimental]).apply_to_groups_with_different_names(ws)
    logger.info(f"Migration using experimental API takes {process_time() - start}s")

    start = process_time()
    ctx = WorkspaceContext(ws).replace(inventory_schema=inventory_schema, sql_backend=sql_backend)
    ctx.permission_manager.apply_group_permissions(MigrationState([migrated_group]))
    logger.info(f"Migration using normal approach takes {process_time() - start}s")


def set_permissions(ws: WorkspaceClient, ws_object, id_attribute, object_type, acls):
    request_object_id = getattr(ws_object, id_attribute)
    ws.permissions.update(object_type, request_object_id, access_control_list=acls)


def create_ws_objects_parallel(
    ws: WorkspaceClient,
    sql_backend: SqlBackend,
    inventory_schema,
    num_objects,
    fixture,
    all_permissions,
    id_attribute,
    object_type,
    group_names,
):
    fixture_tasks = []
    for _ in range(num_objects):
        fixture_tasks.append(partial(fixture))

    success, failure = Threads.gather(f"Creation of {object_type} ", fixture_tasks)  # type: ignore[var-annotated]
    logger.warning(f"Had {len(failure)} failures when creating objects")

    acls = [
        iam.AccessControlRequest(group_name=group_name, permission_level=permission)
        for permission in all_permissions
        for group_name in group_names
    ]
    ws_object_permissions_tasks = []
    to_persist = []
    for ws_object in success:
        ws_object_permissions_tasks.append(partial(set_permissions, ws, ws_object, id_attribute, object_type, acls))
        for object_permission in _generate_object_permissions(
            ws_object, id_attribute, object_type, all_permissions, group_names
        ):
            to_persist.append(
                Permissions(getattr(ws_object, id_attribute), object_type, json.dumps(object_permission.as_dict()))
            )
    sql_backend.save_table(f"{inventory_schema}.permissions", to_persist, Permissions)
    success, failure = Threads.gather(f"{object_type} permissions", ws_object_permissions_tasks)
    logger.warning(f"Had {len(failure)} failures when applying object permissions")


def _generate_object_permissions(ws_object, id_attribute, object_type, all_permissions, group_names):
    return [
        iam.ObjectPermissions(
            access_control_list=[
                iam.AccessControlResponse(
                    group_name=group_name,
                    all_permissions=[iam.Permission(permission_level=permission) for permission in all_permissions],
                )
            ],
            object_id=getattr(ws_object, id_attribute),
            object_type=object_type,
        )
        for group_name in group_names
    ]

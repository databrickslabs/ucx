import json
import logging
from functools import partial
from time import process_time

from databricks.labs.blueprint.parallel import Threads
from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import iam

from databricks.labs.ucx.workspace_access.base import Permissions
from databricks.labs.ucx.workspace_access.groups import MigrationState
from databricks.labs.ucx.workspace_access.manager import PermissionManager

logger = logging.getLogger(__name__)

NB_OF_TEST_WS_OBJECTS = 1000


def test_apply_group_permissions_experimental_performance(
    ws: WorkspaceClient,
    sql_backend: SqlBackend,
    inventory_schema,
    permission_manager: PermissionManager,
    make_migrated_group,
    make_experiment,
):
    migrated_group_experimental, _ = make_migrated_group()
    migrated_group, _ = make_migrated_group()

    create_ws_objects_parallel(
        ws,
        sql_backend,
        inventory_schema,
        NB_OF_TEST_WS_OBJECTS,
        make_experiment,
        [iam.PermissionLevel.CAN_MANAGE],
        "experiment_id",
        "experiments",
        [migrated_group.name_in_workspace, migrated_group_experimental.name_in_workspace],
    )

    start = process_time()
    permission_manager.apply_group_permissions_experimental(MigrationState([migrated_group_experimental]))
    logger.info(f"Migration using experimental API takes {process_time() - start}s")

    start = process_time()
    permission_manager.apply_group_permissions(MigrationState([migrated_group]))
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

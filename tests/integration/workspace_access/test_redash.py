import json
import logging
from collections.abc import Iterable
from datetime import timedelta

from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service import iam, sql

from databricks.labs.ucx.workspace_access import redash
from databricks.labs.ucx.workspace_access.base import Permissions
from databricks.labs.ucx.workspace_access.groups import MigratedGroup, MigrationState
from databricks.labs.ucx.workspace_access.redash import RedashPermissionsSupport

from . import apply_tasks, apply_tasks_appliers, apply_tasks_crawlers

logger = logging.getLogger(__name__)


@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_permissions_for_redash(
    ws,
    sql_backend,
    inventory_schema,
    make_ucx_group,
    make_group,
    make_user,
    make_query,
    make_query_policy_permissions,
):
    ws_group = make_group()
    user = make_user()

    query = make_query()
    make_query_policy_permissions(
        object_id=query.id,
        permission_level=sql.PermissionLevel.CAN_EDIT,
        group_name=ws_group.display_name,
        user_name=user.display_name,
    )

    group_to_migrate = MigratedGroup.partial_info(ws_group, ws_group)
    group_to_migrate.temporary_name = None  # don't apply permissions for the temp group

    redash_permissions = RedashPermissionsSupport(
        ws,
        [redash.Listing(ws.queries.list, sql.ObjectTypePlural.QUERIES)],
    )
    apply_tasks(redash_permissions, [group_to_migrate])

    query_permissions = redash_permissions.load_as_dict(sql.ObjectTypePlural.QUERIES, query.id)
    assert sql.PermissionLevel.CAN_EDIT == query_permissions[ws_group.display_name]
    assert sql.PermissionLevel.CAN_EDIT == query_permissions[user.display_name]


# Redash group permissions are cached for up to 10 mins.
# Therefore, in the timeout we need to allow at least 10 mins for checking the permissions after group rename.
@retried(on=[NotFound], timeout=timedelta(minutes=13))
def test_permissions_for_redash_after_group_is_renamed(
    ws,
    sql_backend,
    inventory_schema,
    make_group,
    make_query,
    make_query_policy_permissions,
):
    """
    Redash permissions are cached for up to 10 mins. See: https://databricks.atlassian.net/browse/ES-992619
    Therefore, when a group is renamed, get redash permissions API can return the old group name for some time.
    Note that the update/set API is strongly consistent and is not affected by this behaviour.
    This test validates that Redash Permissions Support is able to apply permissions correctly after rename operation.
    """
    ws_group = make_group()

    query = make_query()
    make_query_policy_permissions(
        object_id=query.id,
        permission_level=sql.PermissionLevel.CAN_EDIT,
        group_name=ws_group.display_name,
    )
    redash_permissions = RedashPermissionsSupport(
        ws,
        [redash.Listing(ws.queries.list, sql.ObjectTypePlural.QUERIES)],
    )
    permissions = apply_tasks_crawlers(redash_permissions)

    def rename_group_in_permissions_obj(
        group_name: str, new_group_name: str, permissions_iter: Iterable[Permissions]
    ) -> Iterable[Permissions]:
        for p in permissions_iter:
            permission = sql.GetResponse.from_dict(json.loads(p.raw))
            if permission.access_control_list is not None:
                for acl in permission.access_control_list:
                    if acl.group_name == group_name:
                        acl.group_name = new_group_name
                p.raw = json.dumps(permission.as_dict())
        return permissions_iter

    def rename_group(group: iam.Group, new_group_name: str) -> iam.Group:
        ws.groups.patch(ws_group.id, operations=[iam.Patch(iam.PatchOp.REPLACE, "displayName", new_group_name)])
        group.display_name = new_group_name
        return group

    ws_new_group_name = ws_group.display_name + "_new"
    permissions = rename_group_in_permissions_obj(ws_group.display_name, ws_new_group_name, permissions)
    ws_group = rename_group(ws_group, ws_new_group_name)

    group_to_migrate = MigratedGroup.partial_info(ws_group, ws_group)
    group_to_migrate.temporary_name = None  # don't apply permissions for the temp group

    apply_tasks_appliers(redash_permissions, permissions, MigrationState([group_to_migrate]))

    query_permissions = redash_permissions.load_as_dict(sql.ObjectTypePlural.QUERIES, query.id)
    assert sql.PermissionLevel.CAN_EDIT == query_permissions[ws_group.display_name]

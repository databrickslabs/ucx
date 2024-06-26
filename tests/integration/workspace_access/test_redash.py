import json
import logging
from datetime import timedelta
from unittest import skip

import pytest
from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service import iam, sql

from databricks.labs.ucx.workspace_access import redash
from databricks.labs.ucx.workspace_access.base import Permissions
from databricks.labs.ucx.workspace_access.groups import MigratedGroup, MigrationState
from databricks.labs.ucx.workspace_access.redash import RedashPermissionsSupport

from . import apply_tasks, apply_tasks_appliers, apply_tasks_crawlers

logger = logging.getLogger(__name__)


@pytest.mark.parametrize("use_permission_migration_api", [True, False])
@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_permissions_for_redash(
    ws,
    make_group,
    migrated_group,
    make_user,
    make_query,
    make_query_permissions,
    use_permission_migration_api,
):
    ws_group_temp = make_group()  # simulate temp/backup group
    user = make_user()

    query = make_query()
    make_query_permissions(
        object_id=query.id,
        permission_level=sql.PermissionLevel.CAN_EDIT,
        group_name=migrated_group.name_in_workspace,
        user_name=user.display_name,
    )

    # Note that Redash support replaces all permissions and apply it on the temp/backup group instead of original group.
    # We don't rename the original group as part of this test therefore we need to set the temp group explicitly here.
    migrated_group.temporary_name = ws_group_temp.display_name

    redash_permissions = RedashPermissionsSupport(
        ws,
        [redash.Listing(ws.queries.list, sql.ObjectTypePlural.QUERIES)],
    )

    if use_permission_migration_api:
        MigrationState([migrated_group]).apply_to_groups_with_different_names(ws)
    else:
        apply_tasks(redash_permissions, [migrated_group])

    query_permissions = redash_permissions.load_as_dict(sql.ObjectTypePlural.QUERIES, query.id)
    if not use_permission_migration_api:
        # Note that we don't validate the original group permissions here because Redash support apply the permissions
        # on the temp/backup group instead of the original group.
        # Permission migration API skips this step
        assert sql.PermissionLevel.CAN_EDIT == query_permissions[ws_group_temp.display_name]
    assert sql.PermissionLevel.CAN_EDIT == query_permissions[migrated_group.name_in_account]
    assert sql.PermissionLevel.CAN_EDIT == query_permissions[user.display_name]


# Redash group permissions are cached for up to 10 mins. If a group is renamed, redash permissions api returns
# the old name for some time. Therefore, we need to allow at least 10 mins in the timeout for checking the permissions
# after group rename.
@skip("skipping as it takes 5-10 mins to execute")
@retried(on=[NotFound], timeout=timedelta(minutes=5))
def test_permissions_for_redash_after_group_is_renamed(
    ws,
    make_group,
    make_query,
    make_query_permissions,
):
    """
    Redash permissions are cached for up to 10 mins. See: https://databricks.atlassian.net/browse/ES-992619
    Therefore, when a group is renamed, get redash permissions API can return the old group name for some time.
    This test validates that Redash Permissions Support is able to apply and validate permissions correctly
    after rename operation.
    """
    ws_group = make_group()
    acc_group = make_group()

    query = make_query()
    make_query_permissions(
        object_id=query.id,
        permission_level=sql.PermissionLevel.CAN_EDIT,
        group_name=ws_group.display_name,
    )
    redash_permissions = RedashPermissionsSupport(
        ws,
        [redash.Listing(ws.queries.list, sql.ObjectTypePlural.QUERIES)],
    )
    permissions = apply_tasks_crawlers(redash_permissions)

    group_to_migrate = MigratedGroup.partial_info(ws_group, acc_group)

    def rename_group(group: iam.Group, new_group_name: str) -> iam.Group:
        ws.groups.patch(group.id, operations=[iam.Patch(iam.PatchOp.REPLACE, "displayName", new_group_name)])
        group.display_name = new_group_name
        return group

    # simulate creating temp/backup group by renaming the original workspace-local group
    ws_group_a_temp_name = "tmp-" + ws_group.display_name
    ws_group = rename_group(ws_group, ws_group_a_temp_name)

    apply_tasks_appliers(redash_permissions, permissions, MigrationState([group_to_migrate]))

    query_permissions = redash_permissions.load_as_dict(sql.ObjectTypePlural.QUERIES, query.id)
    assert sql.PermissionLevel.CAN_EDIT == query_permissions[ws_group.display_name]
    assert sql.PermissionLevel.CAN_EDIT == query_permissions[acc_group.display_name]


@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_verify_permissions_for_redash(
    ws,
    make_group,
    make_query,
    make_query_permissions,
):
    ws_group = make_group()

    query = make_query()
    make_query_permissions(
        object_id=query.id,
        permission_level=sql.PermissionLevel.CAN_EDIT,
        group_name=ws_group.display_name,
    )

    redash_permissions = RedashPermissionsSupport(
        ws,
        [redash.Listing(ws.queries.list, sql.ObjectTypePlural.QUERIES)],
    )

    item = Permissions(
        object_id=query.id,
        object_type=sql.ObjectTypePlural.QUERIES.value,
        raw=json.dumps(
            sql.GetResponse(
                object_type=sql.ObjectType.QUERY,
                object_id="test",
                access_control_list=[
                    sql.AccessControl(
                        group_name=ws_group.display_name,
                        permission_level=sql.PermissionLevel.CAN_EDIT,
                    )
                ],
            ).as_dict()
        ),
    )

    task = redash_permissions.get_verify_task(item)
    result = task()

    assert result

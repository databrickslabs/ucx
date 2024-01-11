import logging
from datetime import timedelta

from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service import sql

from databricks.labs.ucx.workspace_access import redash
from databricks.labs.ucx.workspace_access.groups import GroupManager
from databricks.labs.ucx.workspace_access.redash import RedashPermissionsSupport

from . import apply_tasks_appliers, apply_tasks_crawlers

logger = logging.getLogger(__name__)


# Redash group permissions are cached for up to 10 mins.
# therefore we need to allow at least 10 mins for checking the permissions after group rename.
# On top, we have to allow some time to execute the rest of the test.
@retried(on=[NotFound], timeout=timedelta(minutes=13))
def test_redash_replace_workspace_groups_with_account_groups(
    ws,
    sql_backend,
    inventory_schema,
    make_ucx_group,
    make_user,
    make_query,
    make_query_policy_permissions,
):
    ws_group, _ = make_ucx_group()
    user = make_user()

    query = make_query()
    make_query_policy_permissions(
        object_id=query.id,
        permission_level=sql.PermissionLevel.CAN_EDIT,
        group_name=ws_group.display_name,
        user_name=user.display_name,
    )

    group_manager = GroupManager(sql_backend, ws, inventory_schema, [ws_group.display_name], "ucx-temp-")
    redash_permissions = RedashPermissionsSupport(
        ws,
        [redash.Listing(ws.queries.list, sql.ObjectTypePlural.QUERIES)],
    )
    permissions = apply_tasks_crawlers(redash_permissions)
    group_manager.rename_groups()
    group_manager.reflect_account_groups_on_workspace()
    state = group_manager.get_migration_state()
    apply_tasks_appliers(redash_permissions, permissions, state)

    group_info = state.groups[0]

    @retried(on=[AssertionError], timeout=timedelta(minutes=1))
    def check_permissions_after_migration():
        logger.info("check_permissions_for_account_group()")

        query_permissions = redash_permissions.load_as_dict(sql.ObjectTypePlural.QUERIES, query.id)
        assert group_info.name_in_account in query_permissions
        assert group_info.temporary_name in query_permissions
        assert user.display_name in query_permissions
        assert sql.PermissionLevel.CAN_EDIT == query_permissions[group_info.name_in_account]
        assert sql.PermissionLevel.CAN_EDIT == query_permissions[group_info.temporary_name]
        assert sql.PermissionLevel.CAN_EDIT == query_permissions[user.display_name]

    check_permissions_after_migration()

    group_manager.delete_original_workspace_groups()

    @retried(on=[AssertionError], timeout=timedelta(minutes=1))
    def check_permissions_after_backup_delete():
        logger.info("check_table_permissions_after_backup_delete()")

        query_permissions = redash_permissions.load_as_dict(sql.ObjectTypePlural.QUERIES, query.id)
        assert group_info.temporary_name not in query_permissions
        assert group_info.name_in_account in query_permissions
        assert user.display_name in query_permissions
        assert sql.PermissionLevel.CAN_EDIT == query_permissions[group_info.name_in_account]
        assert sql.PermissionLevel.CAN_EDIT == query_permissions[user.display_name]

    check_permissions_after_backup_delete()

import logging
from datetime import timedelta

from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service import sql

from databricks.labs.ucx.workspace_access import redash
from databricks.labs.ucx.workspace_access.groups import GroupManager
from databricks.labs.ucx.workspace_access.manager import PermissionManager
from databricks.labs.ucx.workspace_access.redash import RedashPermissionsSupport

logger = logging.getLogger(__name__)


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
    """

    Args:
        sql_backend (object):
    """
    ws_group, _ = make_ucx_group()
    user = make_user()

    query = make_query()
    make_query_policy_permissions(
        object_id=query.id,
        permission_level=sql.PermissionLevel.CAN_RUN,
        group_name=ws_group.display_name,
        user_name=user.display_name,
    )
    logger.info(f"Query: {ws.config.host}/sql/editor/{query.id}")

    group_manager = GroupManager(sql_backend, ws, inventory_schema, [ws_group.display_name], "ucx-temp-")

    redash_permissions = RedashPermissionsSupport(
        ws,
        [redash.Listing(ws.queries.list, sql.ObjectTypePlural.QUERIES)],
    )

    permission_manager = PermissionManager(sql_backend, inventory_schema, [redash_permissions])

    permission_manager.inventorize_permissions()
    state = group_manager.get_migration_state()
    group_manager.rename_groups()
    group_info = state.groups[0]

    # Group information in Redash are cached for up to 10 minutes.
    # After the group is renamed, the old name may still be returned by the dbsql permissions api.
    # More details here: https://databricks.atlassian.net/browse/ES-992619
    @retried(on=[AssertionError], timeout=timedelta(minutes=10))
    def check_permissions_for_backup_group():
        logger.info("check_permissions_for_backup_group()")

        query_permissions = redash_permissions.load_as_dict(sql.ObjectTypePlural.QUERIES, query.id)
        assert group_info.temporary_name in query_permissions
        assert user.display_name in query_permissions
        assert sql.PermissionLevel.CAN_RUN == query_permissions[group_info.temporary_name]
        assert sql.PermissionLevel.CAN_RUN == query_permissions[user.display_name]

    check_permissions_for_backup_group()

    group_manager.reflect_account_groups_on_workspace()

    @retried(on=[AssertionError], timeout=timedelta(minutes=1))
    def check_permissions_after_replace():
        logger.info("check_permissions_after_replace()")

        query_permissions = redash_permissions.load_as_dict(sql.ObjectTypePlural.QUERIES, query.id)
        assert group_info.name_in_account not in query_permissions
        assert group_info.temporary_name in query_permissions
        assert user.display_name in query_permissions
        assert sql.PermissionLevel.CAN_RUN == query_permissions[group_info.temporary_name]
        assert sql.PermissionLevel.CAN_RUN == query_permissions[user.display_name]

    check_permissions_after_replace()

    permission_manager.apply_group_permissions(state)

    @retried(on=[AssertionError], timeout=timedelta(minutes=1))
    def check_permissions_for_account_group():
        logger.info("check_permissions_for_account_group()")

        query_permissions = redash_permissions.load_as_dict(sql.ObjectTypePlural.QUERIES, query.id)
        assert group_info.name_in_account in query_permissions
        assert user.display_name in query_permissions
        assert sql.PermissionLevel.CAN_RUN == query_permissions[group_info.name_in_account]
        assert sql.PermissionLevel.CAN_RUN == query_permissions[group_info.temporary_name]
        assert sql.PermissionLevel.CAN_RUN == query_permissions[user.display_name]

    check_permissions_for_account_group()

    group_manager.delete_original_workspace_groups()

    @retried(on=[AssertionError], timeout=timedelta(minutes=1))
    def check_permissions_after_backup_delete():
        logger.info("check_table_permissions_after_backup_delete()")

        query_permissions = redash_permissions.load_as_dict(sql.ObjectTypePlural.QUERIES, query.id)
        assert group_info.temporary_name not in query_permissions
        assert group_info.name_in_account in query_permissions
        assert user.display_name in query_permissions
        assert sql.PermissionLevel.CAN_RUN == query_permissions[group_info.name_in_account]
        assert sql.PermissionLevel.CAN_RUN == query_permissions[user.display_name]

    check_permissions_after_backup_delete()

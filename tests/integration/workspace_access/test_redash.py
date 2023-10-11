from databricks.labs.ucx.workspace_access.scim import ScimSupport
import pytest
from databricks.sdk.service import iam
from databricks.labs.ucx.mixins.fixtures import ws, make_query, make_user, make_group
from databricks.labs.ucx.workspace_access.groups import GroupMigrationState, MigrationGroupInfo
from databricks.labs.ucx.workspace_access.redash import SqlPermissionsSupport
from databricks.labs.ucx.workspace_access import generic, redash, scim, secrets
from databricks.sdk.service import sql
from databricks.sdk.service.sql import ObjectTypePlural

def test_one_asset_should_have_permission_recplicated_to_backup_group(ws,make_query, make_ucx_group, make_group):
    ws_group, acc_group = make_ucx_group()
    backup_group_name = ws_group.display_name + "-backup"
    backup_group = make_group(display_name=backup_group_name)
    query = make_query()

    migration_state = GroupMigrationState()
    migration_state.add(
        group=MigrationGroupInfo(
            workspace=iam.Group(display_name=ws_group.display_name, id=ws_group.id),
            account=iam.Group(display_name=acc_group.display_name, id=acc_group.id),
            backup=iam.Group(display_name=backup_group_name, id=backup_group.id),
        )
    )

    ws.dbsql_permissions.set(object_type=ObjectTypePlural.QUERIES,
                      object_id=query.id,
                      access_control_list=[
                          sql.AccessControl(group_name=ws_group.display_name, permission_level=sql.PermissionLevel.CAN_RUN)
                      ])

    redash_acl_listing = [redash.redash_listing_wrapper(ws.queries.list, sql.ObjectTypePlural.QUERIES)]
    sql_support = redash.SqlPermissionsSupport(ws, redash_acl_listing)

    tasks = list(sql_support.get_crawler_tasks())
    # Only one query exist in the workspace, so one crawler task
    assert len(tasks) == 1
    permission = tasks[0]()
    apply_task = sql_support.get_apply_task(permission, migration_state, "backup")
    value = apply_task()

    # Validate that no errors has been thrown when applying permission to backup group
    assert value

    applied_permissions = ws.dbsql_permissions.get(object_type=ObjectTypePlural.QUERIES,object_id=query.id)

    # Validate that permissions has been applied properly to the backup group and the old group permission has been revoked
    assert len(applied_permissions.access_control_list) == 3
    assert applied_permissions.access_control_list == [
        sql.AccessControl(user_name=ws.current_user.me().user_name, permission_level=sql.PermissionLevel.CAN_MANAGE),
        sql.AccessControl(group_name=backup_group_name, permission_level=sql.PermissionLevel.CAN_RUN),
        sql.AccessControl(group_name="admins", permission_level=sql.PermissionLevel.CAN_MANAGE)
    ]


def test_one_asset_should_have_permission_recplicated_to_account_group(ws, make_query, make_ucx_group, make_group):
    ws_group, acc_group = make_ucx_group()
    backup_group_name = ws_group.display_name + "-backup"
    backup_group = make_group(display_name=backup_group_name)
    query = make_query()

    migration_state = GroupMigrationState()
    migration_state.add(
        group=MigrationGroupInfo(
            workspace=iam.Group(display_name=ws_group.display_name, id=ws_group.id),
            account=iam.Group(display_name=acc_group.display_name, id=acc_group.id),
            backup=iam.Group(display_name=backup_group_name, id=backup_group.id),
        )
    )

    ws.dbsql_permissions.set(object_type=ObjectTypePlural.QUERIES,
                             object_id=query.id,
                             access_control_list=[
                                 sql.AccessControl(group_name=ws_group.display_name,
                                                   permission_level=sql.PermissionLevel.CAN_RUN)
                             ])

    redash_acl_listing = [redash.redash_listing_wrapper(ws.queries.list, sql.ObjectTypePlural.QUERIES)]
    sql_support = redash.SqlPermissionsSupport(ws, redash_acl_listing)

    tasks = list(sql_support.get_crawler_tasks())
    # Only one query exist in the workspace, so one crawler task
    assert len(tasks) == 1
    permission = tasks[0]()
    apply_task = sql_support.get_apply_task(permission, migration_state, "account")
    value = apply_task()

    # Validate that no errors has been thrown when applying permission to backup group
    assert value

    applied_permissions = ws.dbsql_permissions.get(object_type=ObjectTypePlural.QUERIES, object_id=query.id)

    # Validate that permissions has been applied properly to the account group and the old group permission has been revoked
    assert len(applied_permissions.access_control_list) == 3
    assert applied_permissions.access_control_list == [
        sql.AccessControl(user_name=ws.current_user.me().user_name, permission_level=sql.PermissionLevel.CAN_MANAGE),
        sql.AccessControl(group_name=acc_group.display_name, permission_level=sql.PermissionLevel.CAN_RUN),
        sql.AccessControl(group_name="admins", permission_level=sql.PermissionLevel.CAN_MANAGE)
    ]

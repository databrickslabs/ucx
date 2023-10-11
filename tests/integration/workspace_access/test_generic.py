from databricks.sdk.service import iam
from integration.conftest import list_equal_unordered

from databricks.labs.ucx.workspace_access import generic
from databricks.labs.ucx.workspace_access.groups import (
    GroupMigrationState,
    MigrationGroupInfo,
)


def test_one_experiment_should_have_permission_replicated_to_backup_group(
    ws, make_experiment, make_ucx_group, make_group
):
    ws_group, acc_group = make_ucx_group()
    backup_group_name = ws_group.display_name + "-backup"
    backup_group = make_group(display_name=backup_group_name)

    migration_state = GroupMigrationState()
    migration_state.add(group=MigrationGroupInfo(workspace=ws_group, account=acc_group, backup=backup_group))

    exp = make_experiment()
    ws.permissions.update(
        request_object_type="experiments",
        request_object_id=exp.experiment_id,
        access_control_list=[
            iam.AccessControlRequest(group_name=ws_group.display_name, permission_level=iam.PermissionLevel.CAN_EDIT)
        ],
    )

    generic_acl_listing = [generic.listing_wrapper(generic.experiments_listing(ws), "experiment_id", "experiments")]
    generic_support = generic.GenericPermissionsSupport(ws, generic_acl_listing)

    tasks = list(generic_support.get_crawler_tasks())

    # Only one experiment exist in the workspace, so one crawler task
    assert len(tasks) == 1
    permission = tasks[0]()
    apply_task = generic_support.get_apply_task(permission, migration_state, "backup")

    # Validate that no errors has been thrown when applying permission to backup group
    value = apply_task()
    assert value

    # Validate that permissions has been applied properly to the backup group, the old group isn't revoked from object
    applied_permissions = ws.permissions.get(request_object_type="experiments", request_object_id=exp.experiment_id)
    assert len(applied_permissions.access_control_list) == 4
    assert list_equal_unordered(
        applied_permissions.access_control_list,
        [
            # TODO: Find what /directories/4218545820690421 corresponds to
            iam.AccessControlResponse(
                all_permissions=[
                    iam.Permission(
                        inherited=True,
                        inherited_from_object=["/directories/4218545820690421"],
                        permission_level=iam.PermissionLevel.CAN_MANAGE,
                    )
                ],
                display_name=ws.current_user.me().display_name,
                service_principal_name=ws.current_user.me().user_name,
            ),
            iam.AccessControlResponse(
                all_permissions=[
                    iam.Permission(
                        inherited=True,
                        inherited_from_object=["/directories/"],
                        permission_level=iam.PermissionLevel.CAN_MANAGE,
                    )
                ],
                group_name="admins",
            ),
            iam.AccessControlResponse(
                all_permissions=[
                    iam.Permission(
                        inherited=False, inherited_from_object=None, permission_level=iam.PermissionLevel.CAN_EDIT
                    )
                ],
                group_name=backup_group_name,
            ),
            iam.AccessControlResponse(
                all_permissions=[
                    iam.Permission(
                        inherited=False, inherited_from_object=None, permission_level=iam.PermissionLevel.CAN_EDIT
                    )
                ],
                group_name=ws_group.display_name,
            ),
        ],
    )


def test_one_experiment_should_have_permission_recplicated_to_account_group(
    ws, make_experiment, make_ucx_group, make_group
):
    ws_group, acc_group = make_ucx_group()

    backup_group_name = ws_group.display_name + "-backup"
    backup_group = make_group(display_name=backup_group_name)

    migration_state = GroupMigrationState()
    migration_state.add(group=MigrationGroupInfo(workspace=ws_group, account=acc_group, backup=backup_group))

    exp = make_experiment()

    ws.permissions.update(
        request_object_type="experiments",
        request_object_id=exp.experiment_id,
        access_control_list=[
            iam.AccessControlRequest(group_name=ws_group.display_name, permission_level=iam.PermissionLevel.CAN_EDIT)
        ],
    )

    generic_acl_listing = [generic.listing_wrapper(generic.experiments_listing(ws), "experiment_id", "experiments")]
    generic_support = generic.GenericPermissionsSupport(ws, generic_acl_listing)

    tasks = list(generic_support.get_crawler_tasks())
    # Only one experiment exist in the workspace, so one crawler task
    assert len(tasks) == 1
    permission = tasks[0]()
    apply_task = generic_support.get_apply_task(permission, migration_state, "account")
    value = apply_task()

    # Validate that no errors has been thrown when applying permission to the account group
    assert value
    applied_permissions = ws.permissions.get(request_object_type="experiments", request_object_id=exp.experiment_id)

    # Validate that permissions has been applied properly to the account group, and the old group should be revoked
    assert len(applied_permissions.access_control_list) == 3
    assert list_equal_unordered(
        applied_permissions.access_control_list,
        [
            # TODO: Find what /directories/4218545820690421 corresponds to
            # TODO: Why this test have 3 acl as output while the previous one has 4 ?
            iam.AccessControlResponse(
                all_permissions=[
                    iam.Permission(
                        inherited=True,
                        inherited_from_object=["/directories/4218545820690421"],
                        permission_level=iam.PermissionLevel.CAN_MANAGE,
                    )
                ],
                display_name=ws.current_user.me().display_name,
                service_principal_name=ws.current_user.me().user_name,
            ),
            iam.AccessControlResponse(
                all_permissions=[
                    iam.Permission(
                        inherited=False, inherited_from_object=None, permission_level=iam.PermissionLevel.CAN_EDIT
                    )
                ],
                group_name=acc_group.display_name,
            ),
            iam.AccessControlResponse(
                all_permissions=[
                    iam.Permission(
                        inherited=True,
                        inherited_from_object=["/directories/"],
                        permission_level=iam.PermissionLevel.CAN_MANAGE,
                    )
                ],
                group_name="admins",
            ),
        ],
    )

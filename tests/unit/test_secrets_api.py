import json
from databricks.sdk.service.iam import Group

from databricks.labs.ucx.inventory.permissions import PermissionManager
from databricks.labs.ucx.inventory.types import (
    LogicalObjectType,
    PermissionsInventoryItem,
)
from databricks.labs.ucx.providers.groups_info import (
    GroupMigrationState,
    MigrationGroupInfo,
)


def test_secrets_api():
    item = PermissionsInventoryItem(
        object_id="scope-1",
        logical_object_type=LogicalObjectType.SECRET_SCOPE,
        request_object_type=None,
        raw_object_permissions="""{"acls": [
            {"principal": "g1", "permission": "READ"},
            {"principal": "unrelated-group", "permission": "READ"},
            {"principal": "admins", "permission": "MANAGE"}
        ]}""",
    )

    migration_state = GroupMigrationState()
    migration_state.groups = [
        MigrationGroupInfo(
            account=Group(display_name="g1"),
            workspace=Group(display_name="g1"),
            backup=Group(display_name="some-prefix-g1"),
        )
    ]

    apply_backup = PermissionManager._prepare_permission_request_for_secrets_api(item, migration_state, "backup")

    assert len(apply_backup.access_control_list) == 1
    assert apply_backup.access_control_list[0].principal == "some-prefix-g1"

    apply_account = PermissionManager._prepare_permission_request_for_secrets_api(item, migration_state, "account")

    assert len(apply_account.access_control_list) == 1
    assert apply_account.access_control_list[0].principal == "g1"

def test_prepare_request_for_roles_and_entitlements():
    item = PermissionsInventoryItem(
        object_id="group1",
        logical_object_type=LogicalObjectType.ROLES,
        request_object_type=None,
        raw_object_permissions=json.dumps({"roles": [{"value": "arn:aws:iam::123456789:instance-profile/test-uc-role"},
                                                     {"value": "arn:aws:iam::123456789:instance-profile/test-uc-role2"}]
                                                     , "entitlements": [{"value": "workspace-access"}]}),
            )

    migration_state = GroupMigrationState()
    migration_state.groups = [
        MigrationGroupInfo(
            account=Group(display_name="group1", id="group1"),
            workspace=Group(display_name="group1", id="group1"),
            backup=Group(display_name="some-prefix-group1",id="some-prefix-group1"),
        )
    ]

    apply_backup = PermissionManager._prepare_request_for_roles_and_entitlements(item, migration_state, "backup")

    assert len(apply_backup.payload.roles) == 2
    assert len(apply_backup.payload.entitlements) == 1
    assert apply_backup.group_id == "some-prefix-group1"

    apply_account = PermissionManager._prepare_request_for_roles_and_entitlements(item, migration_state, "account")

    assert len(apply_account.payload.roles) == 2
    assert len(apply_account.payload.entitlements) == 1
    assert apply_account.group_id == "group1"

from databricks.sdk.service.iam import Group

from databricks.labs.ucx.inventory.permissions import PermissionManager
from databricks.labs.ucx.inventory.table import WorkspacePermissions
from databricks.labs.ucx.inventory.workspace import LogicalObjectType
from databricks.labs.ucx.providers.groups_info import (
    GroupMigrationState,
    MigrationGroupInfo,
)


def test_secrets_api():
    item = WorkspacePermissions(
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

import json

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import AccessControlResponse, Group, ObjectPermissions

from databricks.labs.ucx.inventory.permissions import (
    PermissionManager,
    PermissionRequestPayload,
    RolesAndEntitlementsRequestPayload,
    SecretsPermissionRequestPayload,
)
from databricks.labs.ucx.inventory.types import (
    AclItem,
    LogicalObjectType,
    PermissionsInventoryItem,
    RequestObjectType,
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
        raw_object_permissions=json.dumps(
            {
                "roles": [
                    {"value": "arn:aws:iam::123456789:instance-profile/test-uc-role"},
                    {"value": "arn:aws:iam::123456789:instance-profile/test-uc-role2"},
                ],
                "entitlements": [{"value": "workspace-access"}],
            }
        ),
    )

    migration_state = GroupMigrationState()
    migration_state.groups = [
        MigrationGroupInfo(
            account=Group(display_name="group1", id="group1"),
            workspace=Group(display_name="group1", id="group1"),
            backup=Group(display_name="some-prefix-group1", id="some-prefix-group1"),
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


@pytest.mark.parametrize(
    "item,acl_length,object_type, object_id",
    [
        (
            PermissionsInventoryItem(
                object_id="group1",
                logical_object_type=LogicalObjectType.CLUSTER,
                request_object_type=RequestObjectType.CLUSTERS,
                raw_object_permissions=json.dumps(
                    ObjectPermissions(
                        object_id="clusterid1",
                        object_type="clusters",
                        access_control_list=[
                            AccessControlResponse.from_dict(
                                {"all_permissions": [{"permission_level": "CAN_MANAGE"}], "group_name": "group1"}
                            ),
                            AccessControlResponse.from_dict(
                                {"all_permissions": [{"permission_level": "CAN_MANAGE"}], "group_name": "admin"}
                            ),
                        ],
                    ).as_dict()
                ),
            ),
            1,
            LogicalObjectType.CLUSTER,
            "group1",
        ),
        (
            PermissionsInventoryItem(
                object_id="group1",
                logical_object_type=LogicalObjectType.PASSWORD,
                request_object_type=RequestObjectType.AUTHORIZATION,
                raw_object_permissions=json.dumps(
                    ObjectPermissions(
                        object_id="passwords",
                        object_type="authorization",
                        access_control_list=[
                            AccessControlResponse.from_dict(
                                {"all_permissions": [{"permission_level": "CAN_MANAGE"}], "group_name": "group1"}
                            )
                        ],
                    ).as_dict()
                ),
            ),
            1,
            LogicalObjectType.PASSWORD,
            "group1",
        ),
        (
            PermissionsInventoryItem(
                object_id="group1",
                logical_object_type=LogicalObjectType.TOKEN,
                request_object_type=RequestObjectType.AUTHORIZATION,
                raw_object_permissions=json.dumps(
                    ObjectPermissions(
                        object_id="tokens",
                        object_type="authorization",
                        access_control_list=[
                            AccessControlResponse.from_dict(
                                {"all_permissions": [{"permission_level": "CAN_MANAGE"}], "group_name": "group1"}
                            )
                        ],
                    ).as_dict()
                ),
            ),
            1,
            LogicalObjectType.TOKEN,
            "group1",
        ),
        (
            PermissionsInventoryItem(
                object_id="group1",
                logical_object_type=LogicalObjectType.NOTEBOOK,
                request_object_type=RequestObjectType.NOTEBOOKS,
                raw_object_permissions=json.dumps(
                    ObjectPermissions(
                        object_id="notebook1",
                        object_type="notebooks",
                        access_control_list=[
                            AccessControlResponse.from_dict(
                                {"all_permissions": [{"permission_level": "CAN_EDIT"}], "group_name": "group1"}
                            ),
                            AccessControlResponse.from_dict(
                                {"all_permissions": [{"permission_level": "CAN_MANAGE"}], "group_name": "admin"}
                            ),
                        ],
                    ).as_dict()
                ),
            ),
            1,
            LogicalObjectType.NOTEBOOK,
            "group1",
        ),
    ],
)
def test_prepare_request_for_permissions_api(item, acl_length, object_type, object_id):
    migration_state = GroupMigrationState()
    migration_state.groups = [
        MigrationGroupInfo(
            account=Group(display_name="group1", id="group1"),
            workspace=Group(display_name="group1", id="group1"),
            backup=Group(display_name="some-prefix-group1", id="some-prefix-group1"),
        )
    ]

    apply_backup = PermissionManager._prepare_request_for_permissions_api(item, migration_state, "backup")

    assert len(apply_backup.access_control_list) == acl_length
    assert apply_backup.logical_object_type == object_type
    assert apply_backup.object_id == object_id

    apply_account = PermissionManager._prepare_request_for_permissions_api(item, migration_state, "account")

    assert len(apply_account.access_control_list) == acl_length
    assert apply_account.logical_object_type == object_type
    assert apply_account.object_id == object_id


@pytest.mark.parametrize(
    "item,object_type",
    [
        (
            PermissionsInventoryItem(
                object_id="group1",
                logical_object_type=LogicalObjectType.CLUSTER,
                request_object_type=RequestObjectType.CLUSTERS,
                raw_object_permissions=json.dumps(
                    ObjectPermissions(
                        object_id="clusterid1",
                        object_type="clusters",
                        access_control_list=[
                            AccessControlResponse.from_dict(
                                {"all_permissions": [{"permission_level": "CAN_MANAGE"}], "group_name": "group1"}
                            ),
                            AccessControlResponse.from_dict(
                                {"all_permissions": [{"permission_level": "CAN_MANAGE"}], "group_name": "admin"}
                            ),
                        ],
                    ).as_dict()
                ),
            ),
            PermissionRequestPayload,
        ),
        (
            PermissionsInventoryItem(
                object_id="group1",
                logical_object_type=LogicalObjectType.ROLES,
                request_object_type=None,
                raw_object_permissions=json.dumps(
                    {
                        "roles": [
                            {"value": "arn:aws:iam::123456789:instance-profile/test-uc-role"},
                            {"value": "arn:aws:iam::123456789:instance-profile/test-uc-role2"},
                        ],
                        "entitlements": [{"value": "workspace-access"}],
                    }
                ),
            ),
            RolesAndEntitlementsRequestPayload,
        ),
        (
            PermissionsInventoryItem(
                object_id="scope-1",
                logical_object_type=LogicalObjectType.SECRET_SCOPE,
                request_object_type=None,
                raw_object_permissions="""{"acls": [
                    {"principal": "g1", "permission": "READ"},
                    {"principal": "unrelated-group", "permission": "READ"},
                    {"principal": "admins", "permission": "MANAGE"}
                ]}""",
            ),
            SecretsPermissionRequestPayload,
        ),
    ],
)
def test_prepare_new_permission_request(item, object_type):
    migration_state = GroupMigrationState()
    migration_state.groups = [
        MigrationGroupInfo(
            account=Group(display_name="group1", id="group1"),
            workspace=Group(display_name="group1", id="group1"),
            backup=Group(display_name="some-prefix-group1", id="some-prefix-group1"),
        )
    ]
    perm_obj = PermissionManager(None, None)
    apply_backup = perm_obj._prepare_new_permission_request(item, migration_state, "backup")

    assert isinstance(apply_backup, object_type) is True


def test_update_permissions(mocker):
    apply_perm = mocker.patch("databricks.sdk.service.iam.PermissionsAPI.update", return_value=ObjectPermissions())
    perm_obj = PermissionManager(WorkspaceClient(), None)
    output = perm_obj._update_permissions(RequestObjectType.CLUSTERS, "clusterid1", None)
    apply_perm.assert_called_with(
        request_object_type="clusters", request_object_id="clusterid1", access_control_list=None
    )
    assert isinstance(output, ObjectPermissions) is True


def test_standard_permissions_applicator(mocker):
    apply_perm = mocker.patch("databricks.sdk.service.iam.PermissionsAPI.update", return_value=ObjectPermissions())
    perm_obj = PermissionManager(WorkspaceClient(), None)
    perm_obj._standard_permissions_applicator(
        PermissionRequestPayload(None, RequestObjectType.CLUSTERS, "clusterid1", None)
    )
    apply_perm.assert_called_with(
        request_object_type="clusters", request_object_id="clusterid1", access_control_list=None
    )


def test_scope_permissions_applicator(mocker):
    apply_secret_acl = mocker.patch("databricks.sdk.service.workspace.SecretsAPI.put_acl")
    list_secret_acl = mocker.patch(
        "databricks.sdk.service.workspace.SecretsAPI.list_acls",
        return_value=[AclItem(principal="group1", permission="READ"), AclItem(principal="group2", permission="MANAGE")],
    )
    perm_obj = PermissionManager(WorkspaceClient(), None)
    request_payload = SecretsPermissionRequestPayload(
        object_id="scope-1",
        access_control_list=[
            AclItem(principal="group1", permission="READ"),
            AclItem(principal="group2", permission="MANAGE"),
        ],
    )
    perm_obj._scope_permissions_applicator(request_payload=request_payload)
    apply_secret_acl.assert_any_call(scope="scope-1", principal="group1", permission="READ")
    apply_secret_acl.assert_any_call(scope="scope-1", principal="group2", permission="MANAGE")
    list_secret_acl.assert_called_with(scope="scope-1")


def test_patch_workspace_group(mocker):
    group_perm = mocker.patch("databricks.sdk.core.ApiClient.do")
    payload = {
        "schemas": "urn:ietf:params:scim:api:messages:2.0:PatchOp",
        "Operations": {
            "op": "add",
            "path": "entitlements",
            "value": [{"value": "workspace-access"}],
        },
    }
    perm_obj = PermissionManager(WorkspaceClient(), None)
    perm_obj._patch_workspace_group("group1", payload)
    group_perm.assert_any_call("PATCH", "/api/2.0/preview/scim/v2/Groups/group1", data=json.dumps(payload))
    payload = {
        "schemas": "urn:ietf:params:scim:api:messages:2.0:PatchOp",
        "Operations": {
            "op": "add",
            "path": "roles",
            "value": [{"value": "arn:aws:iam::123456789:instance-profile/test-uc-role"}],
        },
    }
    perm_obj = PermissionManager(WorkspaceClient(), None)
    perm_obj._patch_workspace_group("group2", payload)
    group_perm.assert_any_call("PATCH", "/api/2.0/preview/scim/v2/Groups/group2", data=json.dumps(payload))

import json
from unittest.mock import MagicMock

import pytest
from databricks.sdk.service.iam import AccessControlResponse, Group, ObjectPermissions
from databricks.sdk.service.sql import AccessControl as SqlAccessControl
from databricks.sdk.service.sql import GetResponse as SqlPermissions
from databricks.sdk.service.sql import ObjectType, PermissionLevel

from databricks.labs.ucx.inventory.applicator import (
    Applicators,
    ObjectPermissionsApplicator,
    RolesAndEntitlementsApplicator,
    SecretScopeApplicator,
    SqlPermissionsApplicator,
)
from databricks.labs.ucx.inventory.types import (
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

    backup_app = SecretScopeApplicator(ws=MagicMock(), migration_state=migration_state, destination="backup", item=item)
    account_app = SecretScopeApplicator(
        ws=MagicMock(), migration_state=migration_state, destination="account", item=item
    )

    # args[0] is the scope name, args[1] is the ACL
    apply_backup_acl = backup_app.func.args[1]
    apply_account_acl = account_app.func.args[1]

    assert len(apply_backup_acl) == 1
    assert apply_backup_acl[0].principal == "some-prefix-g1"

    assert len(apply_account_acl) == 1
    assert apply_account_acl[0].principal == "g1"


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

    backup_app = RolesAndEntitlementsApplicator(
        ws=MagicMock(), migration_state=migration_state, destination="backup", item=item
    )
    account_app = RolesAndEntitlementsApplicator(
        ws=MagicMock(), migration_state=migration_state, destination="account", item=item
    )

    apply_backup_payload = backup_app.func.args[1]
    apply_backup_gid = backup_app.func.args[0]

    assert len(apply_backup_payload.roles) == 2
    assert len(apply_backup_payload.entitlements) == 1
    assert apply_backup_gid == "some-prefix-group1"

    apply_account_payload = account_app.func.args[1]
    apply_account_gid = account_app.func.args[0]

    assert len(apply_account_payload.roles) == 2
    assert len(apply_account_payload.entitlements) == 1
    assert apply_account_gid == "group1"


test_cluster_item = PermissionsInventoryItem(
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
)


@pytest.mark.parametrize(
    "item,acl_length,object_type, object_id",
    [
        (
            test_cluster_item,
            1,
            RequestObjectType.CLUSTERS,
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
            RequestObjectType.AUTHORIZATION,
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
            RequestObjectType.AUTHORIZATION,
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
            RequestObjectType.NOTEBOOKS,
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

    backup_app = ObjectPermissionsApplicator(
        ws=MagicMock(), migration_state=migration_state, destination="backup", item=item
    )
    account_app = ObjectPermissionsApplicator(
        ws=MagicMock(), migration_state=migration_state, destination="account", item=item
    )

    apply_backup_obj_type = backup_app.func.args[0]
    apply_backup_obj_id = backup_app.func.args[1]
    apply_backup_acl = backup_app.func.args[2]

    assert len(apply_backup_acl) == acl_length
    assert apply_backup_obj_type == object_type
    assert apply_backup_obj_id == object_id

    apply_account_obj_type = account_app.func.args[0]
    apply_account_obj_id = account_app.func.args[1]
    apply_account_acl = account_app.func.args[2]

    assert len(apply_account_acl) == acl_length
    assert apply_account_obj_type == object_type
    assert apply_account_obj_id == object_id


@pytest.mark.parametrize(
    "item,applicator_type",
    [
        (
            test_cluster_item,
            ObjectPermissionsApplicator,
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
            RolesAndEntitlementsApplicator,
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
            SecretScopeApplicator,
        ),
        (
            PermissionsInventoryItem(
                object_id="q1",
                logical_object_type=LogicalObjectType.QUERY,
                request_object_type=RequestObjectType.QUERIES,
                raw_object_permissions=json.dumps(
                    SqlPermissions(
                        object_id="q1",
                        object_type=ObjectType.QUERY,
                        access_control_list=[
                            SqlAccessControl(group_name="g1", permission_level=PermissionLevel.CAN_MANAGE)
                        ],
                    ).as_dict()
                ),
            ),
            SqlPermissionsApplicator,
        ),
    ],
)
def test_applicator_identification(item, applicator_type):
    migration_state = GroupMigrationState()
    migration_state.groups = [
        MigrationGroupInfo(
            account=Group(display_name="group1", id="group1"),
            workspace=Group(display_name="group1", id="group1"),
            backup=Group(display_name="some-prefix-group1", id="some-prefix-group1"),
        )
    ]
    applicator = Applicators(ws=MagicMock(), migration_state=migration_state, destination="backup")._get_applicator(
        item
    )

    assert isinstance(applicator, applicator_type) is True

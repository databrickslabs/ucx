from unittest.mock import MagicMock, create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, PermissionDenied, ResourceDoesNotExist
from databricks.sdk.service.catalog import MetastoreAssignment
from databricks.sdk.service.iam import (
    AccessControlResponse,
    ComplexValue,
    Group,
    ObjectPermissions,
    Permission,
    PermissionLevel,
)

from databricks.labs.ucx.workspace_access.groups import MigratedGroup, MigrationState
from databricks.labs.ucx.workspace_access.secrets import SecretScopesSupport
from databricks.labs.ucx.workspace_access.verification import (
    MetastoreNotFoundError,
    VerificationManager,
    VerifyHasMetastore,
)

METASTORE_ASSIGNMENT = MetastoreAssignment(
    metastore_id="21fwef-b2345-sdas-2343-sddsvv332", workspace_id=1234567890, default_catalog_name="hive_metastore"
)


def test_validate_metastore_exists():
    ws = create_autospec(WorkspaceClient)
    ws.metastores.current.return_value = METASTORE_ASSIGNMENT
    verify_metastore_obj = VerifyHasMetastore(ws)

    assert verify_metastore_obj.verify_metastore() is True

    assert verify_metastore_obj.metastore_id == "21fwef-b2345-sdas-2343-sddsvv332"
    assert verify_metastore_obj.default_catalog_name == "hive_metastore"
    assert verify_metastore_obj.workspace_id == 1234567890


def test_validate_no_metastore_exists():
    ws = create_autospec(WorkspaceClient)
    ws.metastores.current.return_value = None

    verify_metastore_obj = VerifyHasMetastore(ws)

    with pytest.raises(MetastoreNotFoundError, match="Metastore not found in the workspace"):
        verify_metastore_obj.verify_metastore()


def test_permission_denied_error():
    ws = create_autospec(WorkspaceClient)
    ws.metastores.current.side_effect = PermissionDenied()
    ws.metastores.current.return_value = None
    ws.return_value = None

    verify_metastore_obj = VerifyHasMetastore(ws)

    assert not verify_metastore_obj.verify_metastore()


def test_fail_get_permissions_missing_object(caplog):
    ws = create_autospec(WorkspaceClient)
    ws.permissions.get.side_effect = ResourceDoesNotExist("RESOURCE_DOES_NOT_EXIST")
    ws.groups.get.side_effect = NotFound("GROUP_MISSING")
    secret_scopes = SecretScopesSupport(ws)
    manager = VerificationManager(ws, secret_scopes)

    state = MigrationState(
        [
            MigratedGroup(
                id_in_workspace="wid1",
                name_in_workspace="test",
                name_in_account="test",
                temporary_name="db-temp-test",
                members=None,
                entitlements=None,
                external_id="eid1",
                roles=None,
            )
        ]
    )
    manager.verify_applied_permissions("ot1", "oid1", state, "backup")
    manager.verify_roles_and_entitlements(state, "backup")
    assert caplog.messages == ['removed on backend: ot1.oid1', 'removed on backend: wid1']


def test_verify():
    ws = create_autospec(WorkspaceClient)
    secret_scopes = SecretScopesSupport(ws)
    manager = VerificationManager(ws, secret_scopes)

    state = MigrationState(
        [
            MigratedGroup(
                id_in_workspace="wid1",
                name_in_workspace="test",
                name_in_account="test",
                temporary_name="db-temp-test",
                members=None,
                entitlements=None,
                external_id="eid1",
                roles=None,
            )
        ]
    )
    manager.verify_applied_scope_acls = MagicMock()
    manager.verify_applied_permissions = MagicMock()
    manager.verify_roles_and_entitlements = MagicMock()

    manager.verify(state, "backup", [("ot1", "oid1"), ("secrets", "oid2")])
    manager.verify_applied_scope_acls.assert_called_once_with("oid2", state, "backup")
    manager.verify_applied_permissions.assert_called_once_with("ot1", "oid1", state, "backup")
    manager.verify_roles_and_entitlements.assert_called_once_with(state, "backup")


def test_verify_applied_scope_acls():
    ws = create_autospec(WorkspaceClient)
    secret_scopes = SecretScopesSupport(ws)

    def mock_secret_scope_permission(scope_name, group_name):
        permissions_map = {
            ("oid1", "test1"): "READ",
            ("oid1", "test2"): "READ",
            ("oid1", "test3"): "WRITE",
        }
        return permissions_map.get((scope_name, group_name))

    secret_scopes.secret_scope_permission = MagicMock(side_effect=mock_secret_scope_permission)
    manager = VerificationManager(ws, secret_scopes)

    state = MigrationState(
        [
            MigratedGroup(
                id_in_workspace="wid1",
                name_in_workspace="test1",
                name_in_account="test1",
                temporary_name="test2",
                members=None,
                entitlements=None,
                external_id="eid1",
                roles=None,
            ),
            MigratedGroup(
                id_in_workspace="wid2",
                name_in_workspace="test2",
                name_in_account="test2",
                temporary_name="test3",
                members=None,
                entitlements=None,
                external_id="eid2",
                roles=None,
            ),
        ]
    )

    with pytest.raises(AssertionError, match="Scope ACLs were not applied correctly"):
        manager.verify_applied_scope_acls("oid1", state, "backup")

    secret_scopes.secret_scope_permission.assert_any_call("oid1", "test1")
    secret_scopes.secret_scope_permission.assert_any_call("oid1", "test2")
    secret_scopes.secret_scope_permission.assert_any_call("oid1", "test3")


def test_verify_applied_permissions_fails():
    ws = create_autospec(WorkspaceClient)
    secret_scopes = SecretScopesSupport(ws)
    manager = VerificationManager(ws, secret_scopes)

    mock_permission_data = ObjectPermissions(
        access_control_list=[
            AccessControlResponse(
                all_permissions=[
                    Permission(inherited=True, permission_level=[PermissionLevel.CAN_EDIT]),
                    Permission(inherited=False, permission_level=[PermissionLevel.CAN_VIEW]),
                ],
                group_name="test1",
            ),
            AccessControlResponse(
                all_permissions=[
                    Permission(inherited=True, permission_level=[PermissionLevel.CAN_EDIT]),
                    Permission(inherited=False, permission_level=[PermissionLevel.CAN_READ]),
                ],
                group_name="test2",
            ),
        ]
    )

    state = MigrationState(
        [
            MigratedGroup(
                id_in_workspace="wid1",
                name_in_workspace="test1",
                name_in_account="test1",
                temporary_name="test2",
                members=None,
                entitlements=None,
                external_id="eid1",
                roles=None,
            )
        ]
    )

    ws.permissions.get.side_effect = lambda x, _: mock_permission_data

    with pytest.raises(AssertionError, match="Target permissions were not applied correctly for ot1/oid1"):
        manager.verify_applied_permissions("ot1", "oid1", state, "backup")


def test_fail_verify_roles_and_entitlements_missing_second_object(caplog):
    ws = create_autospec(WorkspaceClient)

    def mock_get(db_id):
        if db_id == "wid1":
            return Group(["role1", "role2"])
        raise NotFound("GROUP_MISSING")

    ws.groups.get.side_effect = mock_get

    secret_scopes = SecretScopesSupport(ws)
    manager = VerificationManager(ws, secret_scopes)
    state = MigrationState(
        [
            MigratedGroup(
                id_in_workspace="wid1",
                name_in_workspace="test",
                name_in_account="test",
                temporary_name="db-temp-test",
                members=None,
                entitlements=None,
                external_id="eid1",
                roles=None,
            )
        ]
    )
    manager.verify_roles_and_entitlements(state, "backup")
    assert caplog.messages == ['removed on backend: eid1']


def test_verify_roles_and_entitlements():
    ws = create_autospec(WorkspaceClient)
    ws.groups.get.return_value = Group(
        roles=[ComplexValue(value="role1"), ComplexValue(value="role2")],
        entitlements=[ComplexValue(value="entitlement1"), ComplexValue(value="entitlement2")],
    )
    secret_scopes = SecretScopesSupport(ws)
    manager = VerificationManager(ws, secret_scopes)

    state = MigrationState(
        [
            MigratedGroup(
                id_in_workspace="wid1",
                name_in_workspace="test",
                name_in_account="test",
                temporary_name="db-temp-test",
                members=None,
                entitlements=None,
                external_id="eid1",
                roles=None,
            )
        ]
    )

    manager.verify_roles_and_entitlements(state, "backup")

    ws.groups.get.assert_any_call("wid1")
    ws.groups.get.assert_any_call("eid1")
    assert ws.groups.get.call_count == 2

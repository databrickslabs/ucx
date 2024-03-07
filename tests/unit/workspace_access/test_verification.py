from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, PermissionDenied, ResourceDoesNotExist
from databricks.sdk.service.catalog import MetastoreAssignment

from databricks.labs.ucx.workspace_access.groups import MigratedGroup, MigrationState
from databricks.labs.ucx.workspace_access.secrets import SecretScopesSupport
from databricks.labs.ucx.workspace_access.verification import (
    MetastoreNotFoundError,
    VerificationManager,
    VerifyHasMetastore,
)


def test_validate_metastore_exists(mocker):
    ws = mocker.patch("databricks.sdk.WorkspaceClient.__init__")
    ws.metastores = mocker.patch("databricks.sdk.WorkspaceClient.metastores")
    ws.metastores.current = lambda: MetastoreAssignment(
        metastore_id="21fwef-b2345-sdas-2343-sddsvv332", workspace_id=1234567890, default_catalog_name="hive_metastore"
    )
    verify_metastore_obj = VerifyHasMetastore(ws)

    assert verify_metastore_obj.verify_metastore() is True

    assert verify_metastore_obj.metastore_id == "21fwef-b2345-sdas-2343-sddsvv332"
    assert verify_metastore_obj.default_catalog_name == "hive_metastore"
    assert verify_metastore_obj.workspace_id == 1234567890


def test_validate_no_metastore_exists(mocker):
    ws = mocker.patch("databricks.sdk.WorkspaceClient.__init__")
    ws.metastores = mocker.patch("databricks.sdk.WorkspaceClient.metastores")
    ws.metastores.current = mocker.patch(
        "databricks.sdk.service.catalog.MetastoreAssignment.__init__", return_value=None
    )
    ws.metastores.current.return_value = None
    ws.return_value = None

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


def test_verify(mocker):
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
    verify_applied_scope_acls = mocker.patch(
        "databricks.labs.ucx.workspace_access.verification.VerificationManager.verify_applied_scope_acls"
    )
    verify_applied_permissions = mocker.patch(
        "databricks.labs.ucx.workspace_access.verification.VerificationManager.verify_applied_permissions"
    )
    verify_roles_and_entitlements = mocker.patch(
        "databricks.labs.ucx.workspace_access.verification.VerificationManager.verify_roles_and_entitlements"
    )
    manager.verify(state, "backup", [("ot1", "oid1"), ("secrets", "oid2")])
    verify_applied_scope_acls.assert_called_once()
    verify_applied_permissions.assert_called_once()
    verify_roles_and_entitlements.assert_called_once()


def test_verify_applied_scope_acls_fails(mocker):
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

    def mock_secret_scope_permission(scope_name, group_name):
        permissions_map = {
            ("oid1", "db-temp-test"): "WRITE",
            ("oid1", "test"): "READ",
        }
        return permissions_map.get((scope_name, group_name))

    mock = mocker.patch(
        'databricks.labs.ucx.workspace_access.secrets.SecretScopesSupport.secret_scope_permission',
        side_effect=mock_secret_scope_permission,
    )

    with pytest.raises(AssertionError, match="Scope ACLs were not applied correctly"):
        manager.verify_applied_scope_acls("oid1", state, "backup")

    mock.assert_any_call("oid1", "db-temp-test")
    mock.assert_any_call("oid1", "test")


def test_verify_applied_scope_acls(mocker, caplog):
    ws = create_autospec(WorkspaceClient)
    secret_scopes = SecretScopesSupport(ws)
    manager = VerificationManager(ws, secret_scopes)

    state = MigrationState(
        [
            MigratedGroup(
                id_in_workspace="wid2",
                name_in_workspace="test2",
                name_in_account="test2",
                temporary_name="db-temp-test2",
                members=None,
                entitlements=None,
                external_id="eid2",
                roles=None,
            )
        ]
    )

    def mock_secret_scope_permission(scope_name, group_name):
        permissions_map = {
            ("oid1", "db-temp-test2"): "MANAGE",
            ("oid1", "test2"): "MANAGE",
        }
        return permissions_map.get((scope_name, group_name))

    mock = mocker.patch(
        'databricks.labs.ucx.workspace_access.secrets.SecretScopesSupport.secret_scope_permission',
        side_effect=mock_secret_scope_permission,
    )

    manager.verify_applied_scope_acls("oid1", state, "backup")

    mock.assert_any_call("oid1", "db-temp-test2")
    mock.assert_any_call("oid1", "test2")

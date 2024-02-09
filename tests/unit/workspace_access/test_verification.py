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

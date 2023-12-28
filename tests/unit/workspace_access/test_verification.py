import unittest

from databricks.sdk.service.catalog import MetastoreAssignment

from databricks.labs.ucx.workspace_access.verification import (
    MetastoreNotFoundError,
    VerifyHasMetastore,
)


def test_validate_metastore_exists(mocker):
    ws = mocker.patch("databricks.sdk.WorkspaceClient.__init__")
    ws.metastores = mocker.patch("databricks.sdk.WorkspaceClient.metastores")
    ws.metastores.current = lambda: MetastoreAssignment(
        metastore_id="21fwef-b2345-sdas-2343-sddsvv332", workspace_id=1234567890, default_catalog_name="hive_metastore"
    )
    verify_metastore_obj = VerifyHasMetastore(ws)

    assert verify_metastore_obj.check_metastore_existence() is True
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
    ws.return_value = None

    verify_metastore_obj = VerifyHasMetastore(ws)
    unit_tester = unittest.TestCase

    assert verify_metastore_obj.check_metastore_existence() is False

    with unit_tester.assertRaises(unit_tester, MetastoreNotFoundError):
        verify_metastore_obj.verify_metastore()

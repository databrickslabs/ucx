from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

from databricks.labs.ucx.hive_metastore.verification import (
    MetastoreNotFoundError,
    VerifyHasCatalog,
    VerifyHasMetastore,
)
from tests.unit import mock_workspace_client


def test_verify_has_metastore() -> None:
    ws = mock_workspace_client()
    verify = VerifyHasMetastore(ws)
    assert verify.verify_metastore()


def test_verify_missing_metastore() -> None:
    ws = mock_workspace_client()
    ws.metastores.current.return_value = None
    verify = VerifyHasMetastore(ws)
    with pytest.raises(MetastoreNotFoundError):
        assert not verify.verify_metastore()


def test_verify_has_catalog_calls_catalog() -> None:
    ws = create_autospec(WorkspaceClient)
    has_catalog = VerifyHasCatalog(ws, "test")
    assert has_catalog.verify()
    ws.catalogs.get.assert_called_once_with("test")


def test_verify_has_no_catalog_when_getting_catalog_raises_exception() -> None:
    ws = create_autospec(WorkspaceClient)
    ws.catalogs.get.side_effect = NotFound
    has_catalog = VerifyHasCatalog(ws, "test")
    assert not has_catalog.verify()
    ws.catalogs.get.assert_called_once_with("test")

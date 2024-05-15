import pytest

from databricks.labs.ucx.hive_metastore.verification import (
    MetastoreNotFoundError,
    VerifyHasMetastore,
)
from tests.unit import mock_workspace_client


def test_verify_has_metastore():
    ws = mock_workspace_client()
    verify = VerifyHasMetastore(ws)
    assert verify.verify_metastore()


def test_verify_missing_metastore():
    ws = mock_workspace_client()
    ws.metastores.current.return_value = None
    verify = VerifyHasMetastore(ws)
    with pytest.raises(MetastoreNotFoundError):
        assert not verify.verify_metastore()

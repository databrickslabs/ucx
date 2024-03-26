import pytest

from databricks.labs.ucx.hive_metastore.verification import VerifyHasMetastore, MetastoreNotFoundError
from tests.unit import workspace_client_mock

def test_verify_has_metastore():
    ws = workspace_client_mock()
    verify = VerifyHasMetastore(ws)
    assert verify.verify_metastore()


def test_verify_missing_metastore():
    ws = workspace_client_mock()
    ws.metastores.current.return_value = None
    verify = VerifyHasMetastore(ws)
    with pytest.raises(MetastoreNotFoundError):
        assert not verify.verify_metastore()
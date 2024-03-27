from unittest.mock import create_autospec

import pytest
from databricks.labs.blueprint.installation import MockInstallation
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.hive_metastore.grants import PrincipalACL


@pytest.fixture
def ws():
    return create_autospec(WorkspaceClient)


@pytest.fixture
def installation():
    return MockInstallation(
        {
            "config.yml": {'warehouse_id': 'abc', 'connect': {'host': 'a', 'token': 'b'}, 'inventory_database': 'ucx'},
            "azure_storage_account_info.csv": [
                {
                    'prefix': 'prefix1',
                    'client_id': 'app_secret1',
                    'principal': 'principal_1',
                    'privilege': 'WRITE_FILES',
                    'type': 'Application',
                    'directory_id': 'directory_id_ss1',
                },
            ],
        }
    )


def test_for_cli(ws, installation):
    assert isinstance(PrincipalACL.for_cli(ws, installation), PrincipalACL)


def test_interactive_cluster(ws, installation):
    assert isinstance(PrincipalACL.for_cli(ws, installation), PrincipalACL)

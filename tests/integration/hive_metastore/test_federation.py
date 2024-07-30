from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.account.workspaces import WorkspaceInfo
from databricks.labs.ucx.hive_metastore import ExternalLocations
from databricks.labs.ucx.hive_metastore.federation import HiveMetastoreFederation


@pytest.fixture
def ws():
    return WorkspaceClient(profile='aws-sandbox')


@pytest.skip("hackathon")
def test_federation(ws, sql_backend):
    schema = 'ucx'
    external_locations = ExternalLocations(ws, sql_backend, schema)
    workspace_info = create_autospec(WorkspaceInfo)
    workspace_info.current.return_value = 'some_thing'
    federation = HiveMetastoreFederation(ws, external_locations, workspace_info)
    federation.run()
    workspace_info.current.assert_called_once()

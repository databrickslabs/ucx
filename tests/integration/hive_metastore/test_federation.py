from unittest.mock import create_autospec

import pytest
from databricks.labs.blueprint.tui import MockPrompts
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.account.workspaces import WorkspaceInfo
from databricks.labs.ucx.hive_metastore import ExternalLocations, TablesCrawler, MountsCrawler
from databricks.labs.ucx.hive_metastore.federation import HiveMetastoreFederation


@pytest.fixture
def ws():
    return WorkspaceClient(profile='aws-sandbox')


@pytest.mark.skip("needs to be enabled")
def test_federation(ws, ctx, sql_backend):
    schema = 'ucx'
    installation = ctx.installation
    tables_crawler = TablesCrawler(sql_backend, schema)
    mounts_crawler = MountsCrawler(sql_backend, ws, schema)
    external_locations = ExternalLocations(ws, sql_backend, schema, tables_crawler, mounts_crawler)
    workspace_info = create_autospec(WorkspaceInfo)
    workspace_info.current.return_value = 'some_thing'
    federation = HiveMetastoreFederation(installation, ws, external_locations, workspace_info)
    prompts = MockPrompts({})
    federation.create_from_cli(prompts)
    workspace_info.current.assert_called_once()

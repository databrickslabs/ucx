import logging

import pytest
from databricks.labs.blueprint.installation import Installation

from databricks.labs.ucx.azure.access import AzureResourcePermissions
from databricks.labs.ucx.azure.resources import AzureResources
from databricks.labs.ucx.hive_metastore.locations import (
    ExternalLocation,
    ExternalLocations,
)


@pytest.mark.skip
def test_azure_storage_accounts(ws, sql_backend, inventory_schema, make_random):
    logger = logging.getLogger(__name__)
    logger.setLevel("DEBUG")
    tables = [
        ExternalLocation("abfss://things@labsazurethings.dfs.core.windows.net/folder1", 1),
    ]
    sql_backend.save_table(f"{inventory_schema}.external_locations", tables, ExternalLocation)
    location = ExternalLocations(ws, sql_backend, inventory_schema)
    installation = Installation(ws, make_random)
    az_res_perm = AzureResourcePermissions(installation, ws, AzureResources(ws), location)
    accounts = list(az_res_perm._get_storage_accounts())
    assert len(accounts) == 1
    assert accounts[0] == "labsazurethings"


@pytest.mark.skip
def test_save_spn_permissions_local(ws, sql_backend, inventory_schema, make_random):
    tables = [
        ExternalLocation("abfss://things@labsazurethings.dfs.core.windows.net/folder1", 1),
    ]
    sql_backend.save_table(f"{inventory_schema}.external_locations", tables, ExternalLocation)
    location = ExternalLocations(ws, sql_backend, inventory_schema)
    installation = Installation(ws, make_random(4))
    az_res_perm = AzureResourcePermissions(installation, ws, AzureResources(ws, include_subscriptions=""), location)
    path = az_res_perm.save_spn_permissions()
    assert ws.workspace.get_status(path)

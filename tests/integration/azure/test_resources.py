import datetime as dt
import logging
import os
from functools import cached_property

import pytest

from databricks.labs.ucx.azure.resources import AccessConnector, AccessConnectorClient, AzureAPIClient
from databricks.labs.ucx.contexts.cli_command import WorkspaceContext


def test_access_connector_client_create_delete(az_cli_ctx, env_or_skip):
    # add TEST_SUBSCRIPTION_ID and TEST_RESOURCE_GROUP env variables to run this test
    logging.getLogger('databricks').setLevel(logging.DEBUG)
    all_connectors = list(az_cli_ctx.azure_resources.access_connectors.list(az_cli_ctx.azure_subscription_id))
    assert isinstance(all_connectors, list)

    resource_group = env_or_skip("TEST_RESOURCE_GROUP")
    # access_connector_id = (
    #     f"/subscriptions/{subscription_id}/resourceGroups/{resource_group}/providers"
    #     "/Microsoft.Databricks/accessConnectors/test"
    # )
    #
    # tomorrow = dt.datetime.now() + dt.timedelta(days=1)
    # access_connector = AccessConnector(
    #     id=access_connector_id,
    #     location="westeurope",
    #     tags={"RemoveAfter": str(tomorrow.date())},
    # )
    #
    # assert access_connector not in access_connector_client.list(subscription_id)
    # access_connector_client.create(access_connector)
    # access_connector_client.create_or_update(access_connector)
    # assert access_connector in access_connector_client.list(subscription_id)
    # access_connector_client.delete(access_connector)
    # assert access_connector not in access_connector_client.list(subscription_id)

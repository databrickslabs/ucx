import datetime as dt

from databricks.labs.ucx.azure.resources import AzureResource


def test_access_connector_client_create_get_list_delete(az_cli_ctx, env_or_skip, make_random):
    subscription_id = az_cli_ctx.azure_subscription_id
    resource_group_name = env_or_skip("TEST_RESOURCE_GROUP")
    access_connector_name = f"test-{make_random()}"
    location = "westeurope"
    tomorrow = dt.datetime.now() + dt.timedelta(days=1)
    tags = {"RemoveAfter": str(tomorrow), "NoAutoRemove": "False"}

    access_connector_id = AzureResource(
        f"/subscriptions/{subscription_id}/resourceGroups/{resource_group_name}/providers"
        f"/Microsoft.Databricks/accessConnectors/{access_connector_name}"
    )

    access_connectors = az_cli_ctx.azure_resources.access_connectors.list(subscription_id)
    access_connector_ids = [access_connector.id for access_connector in access_connectors]
    assert access_connector_id not in access_connector_ids

    az_cli_ctx.azure_resources.access_connectors.create_or_update(
        subscription_id,
        resource_group_name,
        access_connector_name,
        location,
        tags,
    )
    new_access_connector = az_cli_ctx.azure_resources.access_connectors.get(
        subscription_id,
        resource_group_name,
        access_connector_name,
    )
    assert new_access_connector.id == str(access_connector_id)
    assert new_access_connector.location == location
    assert new_access_connector.tags == tags

    az_cli_ctx.azure_resources.access_connectors.delete(
        subscription_id,
        resource_group_name,
        access_connector_name,
    )

    access_connectors = az_cli_ctx.azure_resources.access_connectors.list(subscription_id)
    access_connector_ids = [access_connector.id for access_connector in access_connectors]
    assert access_connector_id not in access_connector_ids


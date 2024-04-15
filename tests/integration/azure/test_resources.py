import datetime as dt

from databricks.labs.ucx.azure.resources import AzureResource


def test_azure_resource_storage_accounts_list_non_zero(az_cli_ctx, env_or_skip, make_random):
    """Expect at least one storage account."""
    storage_accounts = az_cli_ctx.azure_resources.storage_accounts()
    assert len(list(storage_accounts)) > 0


def test_azure_resource_access_connector_list_create_get_delete(az_cli_ctx, env_or_skip, make_random):
    """Test listing, creating, getting and deleting access connectors."""
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

    access_connectors = az_cli_ctx.azure_resources.list_access_connectors(subscription_id)
    access_connector_ids = [access_connector.id for access_connector in access_connectors]
    assert access_connector_id not in access_connector_ids

    az_cli_ctx.azure_resources.create_or_update_access_connector(
        subscription_id,
        resource_group_name,
        access_connector_name,
        location,
        tags,
    )
    new_access_connector = az_cli_ctx.azure_resources.get_access_connector(
        subscription_id,
        resource_group_name,
        access_connector_name,
    )
    assert new_access_connector.id == str(access_connector_id)
    assert new_access_connector.location == location
    assert new_access_connector.tags == tags

    az_cli_ctx.azure_resources.delete_access_connector(
        subscription_id,
        resource_group_name,
        access_connector_name,
    )

    access_connectors = az_cli_ctx.azure_resources.list_access_connectors(subscription_id)
    access_connector_ids = [access_connector.id for access_connector in access_connectors]
    assert access_connector_id not in access_connector_ids

import datetime as dt


from databricks.labs.ucx.azure.resources import AccessConnector


def test_access_connector_client_create_delete(az_cli_ctx, env_or_skip, make_random):
    subscription_id = az_cli_ctx.azure_subscription_id
    resource_group_name = env_or_skip("TEST_RESOURCE_GROUP")
    access_connector_name = f"test-{make_random()}"
    access_connector_id = (
        f"/subscriptions/{subscription_id}/resourceGroups/{resource_group_name}/providers"
        f"/Microsoft.Databricks/accessConnectors/{access_connector_name}"
    )

    tomorrow = dt.datetime.now() + dt.timedelta(days=1)
    access_connector = AccessConnector(
        id=access_connector_id,
        name=access_connector_name,
        location="westeurope",
        tags={"RemoveAfter": str(tomorrow.date())},
    )
    assert access_connector not in list(az_cli_ctx.azure_resources.access_connectors.list(subscription_id))
    az_cli_ctx.azure_resources.access_connectors.create_or_update(
        subscription_id,
        resource_group_name,
        access_connector_name,
        "westeurope",
        {"RemoveAfter": str(tomorrow.date())}
    )
    assert access_connector in list(az_cli_ctx.azure_resources.access_connectors.list(subscription_id))
    az_cli_ctx.azure_resources.access_connectors.delete(
        subscription_id,
        resource_group_name,
        access_connector_name,
    )
    assert access_connector not in list(az_cli_ctx.azure_resources.access_connectors.list(subscription_id))

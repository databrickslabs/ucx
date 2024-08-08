import datetime as dt
import uuid

import pytest

from databricks.labs.ucx.azure.resources import AzureResource


def test_azure_resource_storage_accounts_list_non_zero(az_cli_ctx):
    """Expect at least one storage account."""
    storage_accounts = az_cli_ctx.azure_resources.storage_accounts()
    assert len(list(storage_accounts)) > 0


def test_azure_resource_gets_applies_and_deletes_storage_permissions(az_cli_ctx, env_or_skip, make_random):
    storage_account_name = env_or_skip("TEST_STORAGE_ACCOUNT_NAME")
    storage_accounts = []
    for storage_account in az_cli_ctx.azure_resources.storage_accounts():
        if storage_account.name == storage_account_name:
            storage_accounts.append(storage_account)
    if len(storage_accounts) == 0:
        pytest.skip("Test storage account not found")

    storage_account = storage_accounts[0]
    access_connector_name = f"test-{make_random()}"
    tomorrow = dt.datetime.now() + dt.timedelta(days=1)
    tags = {"RemoveAfter": str(tomorrow), "NoAutoRemove": "False"}
    # TODO: Move this to a fixture that also deletes the access connector
    access_connector = az_cli_ctx.azure_resources.create_or_update_access_connector(
        storage_account.id.subscription_id,
        storage_account.id.resource_group,
        access_connector_name,
        storage_account.location,
        tags,
    )

    role_guid = str(uuid.uuid4())
    storage_permission = az_cli_ctx.azure_resources.get_storage_permission(storage_account, role_guid)
    assert storage_permission is None

    az_cli_ctx.azure_resources.apply_storage_permission(
        access_connector.principal_id,
        storage_account,
        "STORAGE_BLOB_DATA_READER",
        role_guid,
    )
    storage_permission = az_cli_ctx.azure_resources.get_storage_permission(
        storage_account,
        role_guid,
        timeout=dt.timedelta(minutes=2),
    )
    assert storage_permission is not None
    assert storage_permission.principal.object_id == access_connector.principal_id
    assert storage_permission.role_name == "Storage Blob Data Reader"

    az_cli_ctx.azure_resources.delete_storage_permission(access_connector.principal_id, storage_account)
    storage_permission = az_cli_ctx.azure_resources.get_storage_permission(storage_account, role_guid)
    assert storage_permission is None


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

    access_connectors = az_cli_ctx.azure_resources.access_connectors()
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
    assert new_access_connector.id == access_connector_id
    assert new_access_connector.location == location
    assert new_access_connector.tags == tags

    az_cli_ctx.azure_resources.delete_access_connector(
        subscription_id,
        resource_group_name,
        access_connector_name,
    )

    access_connectors = az_cli_ctx.azure_resources.access_connectors()
    access_connector_ids = [access_connector.id for access_connector in access_connectors]
    assert access_connector_id not in access_connector_ids

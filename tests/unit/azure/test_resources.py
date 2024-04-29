from unittest.mock import create_autospec

import pytest
from databricks.sdk.errors import NotFound, PermissionDenied, ResourceConflict

from databricks.labs.ucx.azure.resources import (
    AzureAPIClient,
    AzureResource,
    AzureResources,
    Principal,
    StorageAccount,
)

from . import azure_api_client, get_az_api_mapping


def test_subscriptions_no_subscription():
    api_client = azure_api_client()
    azure_resource = AzureResources(api_client, api_client, include_subscriptions="001")
    subscriptions = list(azure_resource.subscriptions())

    assert len(subscriptions) == 0


def test_subscriptions_valid_subscription():
    api_client = azure_api_client()
    azure_resource = AzureResources(api_client, api_client, include_subscriptions="002")
    subscriptions = list(azure_resource.subscriptions())
    assert len(subscriptions) == 1
    for subscription in subscriptions:
        assert subscription.name == "sub2"


def test_storage_accounts():
    api_client = azure_api_client()
    azure_resource = AzureResources(api_client, api_client, include_subscriptions="002")
    storage_accounts = list(azure_resource.storage_accounts())
    assert len(storage_accounts) == 2
    for storage_account in storage_accounts:
        assert storage_account.id.resource_group == "rg1"
        assert storage_account.name in {"sto2", "sto3"}
        assert storage_account.id.subscription_id == "002"


def test_containers():
    api_client = azure_api_client()
    azure_resource = AzureResources(api_client, api_client, include_subscriptions="002")
    azure_storage = AzureResource("subscriptions/002/resourceGroups/rg1/storageAccounts/sto2")
    containers = list(azure_resource.containers(azure_storage))
    assert len(containers) == 3
    for container in containers:
        assert container.resource_group == "rg1"
        assert container.storage_account == "sto2"
        assert container.container in {"container1", "container2", "container3"}
        assert container.subscription_id == "002"


def test_role_assignments_storage():
    api_client = azure_api_client()
    azure_resource = AzureResources(api_client, api_client, include_subscriptions="002")
    resource_id = "subscriptions/002/resourceGroups/rg1/storageAccounts/sto2"
    role_assignments = list(azure_resource.role_assignments(resource_id))
    assert len(role_assignments) == 1
    for role_assignment in role_assignments:
        assert role_assignment.role_name == "Contributor"
        assert role_assignment.principal == Principal(
            "appIduser2", "disNameuser2", "Iduser2", "Application", "0000-0000"
        )
        assert str(role_assignment.scope) == resource_id
        assert role_assignment.resource == AzureResource(resource_id)


def test_role_assignments_container():
    api_client = azure_api_client()
    azure_resource = AzureResources(api_client, api_client, include_subscriptions="002")
    resource_id = "subscriptions/002/resourceGroups/rg1/storageAccounts/sto2/containers/container1"
    role_assignments = list(azure_resource.role_assignments(resource_id))
    assert len(role_assignments) == 1
    for role_assignment in role_assignments:
        assert role_assignment.role_name == "Contributor"
        assert role_assignment.principal == Principal(
            "appIduser2", "disNameuser2", "Iduser2", "Application", "0000-0000"
        )
        assert str(role_assignment.scope) == resource_id
        assert role_assignment.resource == AzureResource(resource_id)


def test_create_service_principal():
    api_client = azure_api_client()
    azure_resource = AzureResources(api_client, api_client)
    global_spn = azure_resource.create_service_principal("disNameuser1")
    assert global_spn.client.client_id == "appIduser1"
    assert global_spn.client.object_id == "Iduser1"
    assert global_spn.client.display_name == "disNameuser1"
    assert global_spn.client.directory_id == "dir1"
    assert global_spn.secret == "mypwd"


def test_create_service_principal_no_access():
    api_client = azure_api_client()
    api_client.post.side_effect = PermissionDenied()
    azure_resource = AzureResources(api_client, api_client)
    with pytest.raises(PermissionDenied):
        azure_resource.create_service_principal("disNameuser1")


def test_apply_storage_permission():
    api_client = azure_api_client()
    azure_resource = AzureResources(api_client, api_client)
    azure_storage = StorageAccount(
        id=AzureResource("/subscriptions/002/resourceGroups/rg1/storageAccounts/sto2"),
        name="sto2",
        location="eastus",
    )
    azure_resource.apply_storage_permission("test", azure_storage, "STORAGE_BLOB_DATA_READER", "12345")
    path = "/subscriptions/002/resourceGroups/rg1/storageAccounts/sto2/providers/Microsoft.Authorization/roleAssignments/12345"
    body = {
        'properties': {
            'principalId': 'test',
            'principalType': 'ServicePrincipal',
            'roleDefinitionId': '/subscriptions/002/providers/Microsoft.Authorization/roleDefinitions/2a2b9908-6ea1-4ae2-8e65-a410df84e7d1',
        }
    }

    api_client.put.assert_called_with(path, "2022-04-01", body)


def test_apply_storage_permission_no_access():
    api_client = azure_api_client()
    api_client.put.side_effect = PermissionDenied()
    azure_storage = StorageAccount(
        id=AzureResource("/subscriptions/002/resourceGroups/rg1/storageAccounts/sto2"),
        name="sto2",
        location="eastus",
    )
    azure_resource = AzureResources(api_client, api_client)
    with pytest.raises(PermissionDenied):
        azure_resource.apply_storage_permission("test", azure_storage, "STORAGE_BLOB_DATA_READER", "12345")


def test_delete_service_principal_no_access():
    api_client = azure_api_client()
    api_client.delete.side_effect = PermissionDenied()
    azure_resource = AzureResources(api_client, api_client)
    with pytest.raises(PermissionDenied):
        azure_resource.delete_service_principal("test")


def test_delete_service_principal():
    api_client = azure_api_client()
    azure_resource = AzureResources(api_client, api_client)
    azure_resource.delete_service_principal("test")
    api_client.delete.assert_called_with("/v1.0/applications(appId='test')")


def test_apply_storage_permission_assignment_present():
    api_client = azure_api_client()
    api_client.put.side_effect = ResourceConflict()
    azure_storage = StorageAccount(
        id=AzureResource("/subscriptions/002/resourceGroups/rg1/storageAccounts/sto2"),
        name="sto2",
        location="eastus",
    )
    azure_resource = AzureResources(api_client, api_client)
    azure_resource.apply_storage_permission("test", azure_storage, "STORAGE_BLOB_DATA_READER", "12345")
    path = "/subscriptions/002/resourceGroups/rg1/storageAccounts/sto2/providers/Microsoft.Authorization/roleAssignments/12345"
    body = {
        'properties': {
            'principalId': 'test',
            'principalType': 'ServicePrincipal',
            'roleDefinitionId': '/subscriptions/002/providers/Microsoft.Authorization/roleDefinitions/2a2b9908-6ea1-4ae2-8e65-a410df84e7d1',
        }
    }
    api_client.put.assert_called_with(path, "2022-04-01", body)


def test_azure_client_api_put_graph():
    api_client = AzureAPIClient("foo", "bar")
    api_client.api_client.do = get_az_api_mapping
    result = api_client.post("/v1.0/servicePrincipals", {"foo": "bar"})
    assert result.get("appId") == "appIduser1"


def test_azure_client_api_put_mgmt():
    api_client = AzureAPIClient("foo", "bar")
    api_client.api_client.do = get_az_api_mapping
    result = api_client.put("id001", "2022-04-01", {"foo": "bar"})
    assert result.get("id") == "role1"


def test_azure_client_api_get_azure():
    api_client = AzureAPIClient("foo", "bar")
    api_client.api_client.do = get_az_api_mapping
    subscriptions = api_client.get("/subscriptions")
    assert len(subscriptions) == 1


def test_azure_client_api_get_graph():
    api_client = AzureAPIClient("foo", "bar")
    api_client.api_client.do = get_az_api_mapping
    principal = api_client.get("/v1.0/directoryObjects/user1", "2022-02-01")
    assert principal.get("appId") == "appIduser1"


def test_azure_client_api_delete_spn():
    api_client = AzureAPIClient("foo", "bar")
    api_client.api_client.do = get_az_api_mapping
    api_client.delete("/applications/1234")


def test_managed_identity_client_id():
    api_client = azure_api_client()
    azure_resource = AzureResources(api_client, api_client)

    assert (
        azure_resource.managed_identity_client_id(
            "/subscriptions/123/resourcegroups/abc/providers/Microsoft.Databricks/accessConnectors/credential_system_assigned_mi"
        )
        == "application_id_system_assigned_mi-123"
    )
    assert (
        azure_resource.managed_identity_client_id(
            "/subscriptions/123/resourcegroups/abc/providers/Microsoft.ManagedIdentity/accessConnectors/credential_user_assigned_mi",
            "/subscriptions/123/resourcegroups/abc/providers/Microsoft.ManagedIdentity/userAssignedIdentities/credential_user_assigned_mi",
        )
        == "application_id_user_assigned_mi-123"
    )


def test_access_connector_not_found(caplog):
    api_client = create_autospec(AzureAPIClient)
    azure_resource = AzureResources(api_client, api_client)
    api_client.get.side_effect = NotFound()
    assert azure_resource.managed_identity_client_id("test") is None
    assert "no longer exists" in caplog.text


def test_non_app_id_access_connector():
    api_client = create_autospec(AzureAPIClient)
    azure_resource = AzureResources(api_client, api_client)

    api_client.get.return_value = {
        "identity": {
            "userAssignedIdentities": {
                "test_mi_id": {"principalId": "test_principal_id", "clientId": "test_client_id"}
            },
            "type": "UserAssigned",
        }
    }
    # test managed_identity_client_id is called without providing user_assigned_identity_id when userAssignedIdentity is encountered
    assert azure_resource.managed_identity_client_id("no_user_assigned_identity_id_provided") is None
    # test no application_id of the user assigned managed identity is found
    assert azure_resource.managed_identity_client_id("no_such_access_connector", "no_such_identity") is None

    # test if identity type is not UserAssigned nor SystemAssigned
    api_client.get.return_value = {"identity": {"principalId": "test_principal_id", "type": "Other"}}
    assert azure_resource.managed_identity_client_id("other_type") is None


def azure_api_side_effect(*args, **kwargs):
    if "/v1.0/directoryObjects/" in args[0]:
        raise NotFound()
    return get_az_api_mapping(*args, **kwargs)


def test_managed_identity_not_found():
    api_client = azure_api_client()
    api_client.get.side_effect = azure_api_side_effect
    azure_resource = AzureResources(api_client, api_client)

    assert (
        azure_resource.managed_identity_client_id(
            "/subscriptions/123/resourcegroups/abc/providers/Microsoft.Databricks/accessConnectors/credential_system_assigned_mi"
        )
        is None
    )


def test_azure_resources_list_access_connectors() -> None:
    """Non-zero access connectors are mocked"""
    api_client = azure_api_client()
    azure_resource = AzureResources(api_client, api_client, include_subscriptions=["002"])
    access_connectors = azure_resource.access_connectors()
    assert len(list(access_connectors)) > 0


def test_azure_resources_get_access_connector() -> None:
    """Should return the properties of the mocked response."""
    api_client = azure_api_client()
    azure_resource = AzureResources(api_client, api_client)
    access_connector = azure_resource.get_access_connector("002", "rg-test", "test-access-connector")
    assert access_connector is not None
    assert access_connector.name == "test-access-connector"
    assert access_connector.tags["application"] == "databricks"
    assert access_connector.tags["Owner"] == "cor.zuurmond@databricks.com"


def test_azure_resources_get_access_connector_missing_name() -> None:
    """Should return none."""
    api_client = azure_api_client()
    azure_resource = AzureResources(api_client, api_client)
    access_connector = azure_resource.get_access_connector("003", "rg-test", "test-access-connector")
    assert access_connector is None


def test_azure_resources_get_access_connector_missing_location() -> None:
    """Should return none."""
    api_client = azure_api_client()
    azure_resource = AzureResources(api_client, api_client)
    access_connector = azure_resource.get_access_connector("003", "rg-test", "test-access-connector-noloc")
    assert access_connector is None


def test_azure_resources_get_access_connector_missing_ps() -> None:
    """Should return none."""
    api_client = azure_api_client()
    azure_resource = AzureResources(api_client, api_client)
    access_connector = azure_resource.get_access_connector("003", "rg-test", "test-access-connector-nops")
    assert access_connector is None


def test_azure_resources_get_access_connector_inv_identity() -> None:
    """Should return none."""
    api_client = azure_api_client()
    azure_resource = AzureResources(api_client, api_client)
    access_connector = azure_resource.get_access_connector("003", "rg-test", "test-access-connector-invidentity")
    assert access_connector is None


def test_azure_resources_create_or_update_access_connector() -> None:
    """Should return the properties of the mocked response."""
    api_client = azure_api_client()
    azure_resource = AzureResources(api_client, api_client)
    access_connector = azure_resource.create_or_update_access_connector(
        "002", "rg-test", "test-access-connector", "central-us", {"application": "databricks"}
    )
    assert access_connector is not None
    assert access_connector.name == "test-access-connector"
    assert access_connector.tags["application"] == "databricks"
    assert access_connector.tags["Owner"] == "cor.zuurmond@databricks.com"


def test_azure_resources_delete_access_connector() -> None:
    api_client = azure_api_client()
    azure_resource = AzureResources(api_client, api_client)
    azure_resource.delete_access_connector("002", "rg-test", "test-access-connector")
    assert api_client.delete.called

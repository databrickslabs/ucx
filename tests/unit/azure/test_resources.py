import pytest
from databricks.sdk.errors import PermissionDenied, ResourceConflict

from databricks.labs.ucx.azure.resources import (
    AzureAPIClient,
    AzureResource,
    AzureResources,
    Principal,
)

from . import azure_api_client, get_az_api_mapping


def test_subscriptions_no_subscription(mocker):
    api_client = azure_api_client()
    azure_resource = AzureResources(api_client, api_client, include_subscriptions="001")
    subscriptions = list(azure_resource.subscriptions())

    assert len(subscriptions) == 0


def test_subscriptions_valid_subscription(mocker):
    api_client = azure_api_client()
    azure_resource = AzureResources(api_client, api_client, include_subscriptions="002")
    subscriptions = list(azure_resource.subscriptions())
    assert len(subscriptions) == 1
    for subscription in subscriptions:
        assert subscription.name == "sub2"


def test_storage_accounts(mocker):
    api_client = azure_api_client()
    azure_resource = AzureResources(api_client, api_client, include_subscriptions="002")
    storage_accounts = list(azure_resource.storage_accounts())
    assert len(storage_accounts) == 2
    for storage_account in storage_accounts:
        assert storage_account.resource_group == "rg1"
        assert storage_account.storage_account in {"sto2", "sto3"}
        assert storage_account.container is None
        assert storage_account.subscription_id == "002"


def test_containers(mocker):
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


def test_role_assignments_storage(mocker):
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


def test_role_assignments_container(mocker):
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


def test_create_service_principal(mocker):
    api_client = azure_api_client()
    azure_resource = AzureResources(api_client, api_client)
    global_spn = azure_resource.create_service_principal()
    assert global_spn.client_id == "appIduser1"
    assert global_spn.object_id == "Iduser1"
    assert global_spn.display_name == "disNameuser1"
    assert global_spn.directory_id == "dir1"
    assert global_spn.secret == "mypwd"


def test_create_service_principal_no_access(mocker):
    api_client = azure_api_client()
    api_client.post.side_effect = PermissionDenied()
    azure_resource = AzureResources(api_client, api_client)
    with pytest.raises(PermissionDenied):
        azure_resource.create_service_principal()


def test_apply_storage_permission(mocker):
    api_client = azure_api_client()
    azure_resource = AzureResources(api_client, api_client)
    azure_storage = AzureResource("subscriptions/002/resourceGroups/rg1/storageAccounts/sto2")
    azure_resource.apply_storage_permission("test", azure_storage, "STORAGE_BLOB_DATA_READER", "12345")
    path = "subscriptions/002/resourceGroups/rg1/storageAccounts/sto2/providers/Microsoft.Authorization/roleAssignments/12345"
    body = {
        'properties': {
            'principalId': 'test',
            'principalType': 'ServicePrincipal',
            'roleDefinitionId': '/subscriptions/002/providers/Microsoft.Authorization/roleDefinitions/2a2b9908-6ea1-4ae2-8e65-a410df84e7d1',
        }
    }

    api_client.put.assert_called_with(path, "2022-04-01", body)


def test_apply_storage_permission_no_access(mocker):
    api_client = azure_api_client()
    api_client.put.side_effect = PermissionDenied()
    azure_storage = AzureResource("subscriptions/002/resourceGroups/rg1/storageAccounts/sto2")
    azure_resource = AzureResources(api_client, api_client)
    with pytest.raises(PermissionDenied):
        azure_resource.apply_storage_permission("test", azure_storage, "STORAGE_BLOB_DATA_READER", "12345")


def test_apply_storage_permission_assignment_present(mocker):
    api_client = azure_api_client()
    api_client.put.side_effect = ResourceConflict()
    azure_storage = AzureResource("subscriptions/002/resourceGroups/rg1/storageAccounts/sto2")
    azure_resource = AzureResources(api_client, api_client)
    azure_resource.apply_storage_permission("test", azure_storage, "STORAGE_BLOB_DATA_READER", "12345")
    path = "subscriptions/002/resourceGroups/rg1/storageAccounts/sto2/providers/Microsoft.Authorization/roleAssignments/12345"
    body = {
        'properties': {
            'principalId': 'test',
            'principalType': 'ServicePrincipal',
            'roleDefinitionId': '/subscriptions/002/providers/Microsoft.Authorization/roleDefinitions/2a2b9908-6ea1-4ae2-8e65-a410df84e7d1',
        }
    }
    api_client.put.assert_called_with(path, "2022-04-01", body)


def test_azure_client_api_put_graph(mocker):
    api_client = AzureAPIClient("foo", "bar")
    api_client.api_client.do = get_az_api_mapping
    result = api_client.post("/v1.0/servicePrincipals", {"foo": "bar"})
    assert result.get("appId") == "appIduser1"


def test_azure_client_api_put_mgmt(mocker):
    api_client = AzureAPIClient("foo", "bar")
    api_client.api_client.do = get_az_api_mapping
    result = api_client.put("id001", "2022-04-01", {"foo": "bar"})
    assert result.get("id") == "role1"


def test_azure_client_api_get_azure(mocker):
    api_client = AzureAPIClient("foo", "bar")
    api_client.api_client.do = get_az_api_mapping
    subscriptions = api_client.get("/subscriptions")
    assert len(subscriptions) == 1


def test_azure_client_api_get_graph(mocker):
    api_client = AzureAPIClient("foo", "bar")
    api_client.api_client.do = get_az_api_mapping
    principal = api_client.get("/v1.0/directoryObjects/user1", "2022-02-01")
    assert principal.get("appId") == "appIduser1"


def test_azure_client_api_delete_spn(mocker):
    api_client = AzureAPIClient("foo", "bar")
    api_client.api_client.do = get_az_api_mapping
    api_client.delete("/applications/1234")

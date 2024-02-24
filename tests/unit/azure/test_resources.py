import base64
import json
from unittest import mock
from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import PermissionDenied
from databricks.sdk.oauth import Token

from databricks.labs.ucx.azure.resources import (
    AzureAPIClient,
    AzureResource,
    AzureResources,
    Principal,
)

from . import get_az_api_mapping


def azure_api_client():
    token = json.dumps({"aud": "foo", "tid": "bar"}).encode("utf-8")
    str_token = base64.b64encode(token).decode("utf-8").replace("=", "")
    tok = Token(access_token=f"header.{str_token}.sig")
    w = create_autospec(WorkspaceClient)
    api_client = create_autospec(AzureAPIClient(w))
    type(api_client).token = mock.PropertyMock(return_value=tok)
    api_client.get.side_effect = get_az_api_mapping
    api_client.put.side_effect = get_az_api_mapping
    return api_client


def test_subscriptions_no_subscription(mocker, az_token):
    api_client = azure_api_client()
    azure_resource = AzureResources(include_subscriptions="001", api_client=api_client)
    subscriptions = list(azure_resource.subscriptions())
    assert len(subscriptions) == 0


def test_subscriptions_valid_subscription(mocker, az_token):
    api_client = azure_api_client()
    azure_resource = AzureResources(include_subscriptions="002", api_client=api_client)
    subscriptions = list(azure_resource.subscriptions())
    assert len(subscriptions) == 1
    for subscription in subscriptions:
        assert subscription.name == "sub2"


def test_storage_accounts(mocker, az_token):
    api_client = azure_api_client()
    azure_resource = AzureResources(include_subscriptions="002", api_client=api_client)
    storage_accounts = list(azure_resource.storage_accounts())
    assert len(storage_accounts) == 2
    for storage_account in storage_accounts:
        assert storage_account.resource_group == "rg1"
        assert storage_account.storage_account in {"sto2", "sto3"}
        assert storage_account.container is None
        assert storage_account.subscription_id == "002"


def test_containers(mocker, az_token):
    api_client = azure_api_client()
    azure_resource = AzureResources(include_subscriptions="002", api_client=api_client)
    azure_storage = AzureResource("subscriptions/002/resourceGroups/rg1/storageAccounts/sto2")
    containers = list(azure_resource.containers(azure_storage))
    assert len(containers) == 3
    for container in containers:
        assert container.resource_group == "rg1"
        assert container.storage_account == "sto2"
        assert container.container in {"container1", "container2", "container3"}
        assert container.subscription_id == "002"


def test_role_assignments_storage(mocker, az_token):
    api_client = azure_api_client()
    azure_resource = AzureResources(include_subscriptions="002", api_client=api_client)
    resource_id = "subscriptions/002/resourceGroups/rg1/storageAccounts/sto2"
    role_assignments = list(azure_resource.role_assignments(resource_id))
    assert len(role_assignments) == 1
    for role_assignment in role_assignments:
        assert role_assignment.role_name == "Contributor"
        assert role_assignment.principal == Principal("appIduser2", "disNameuser2", "Iduser2", "0000-0000")
        assert str(role_assignment.scope) == resource_id
        assert role_assignment.resource == AzureResource(resource_id)


def test_role_assignments_container(mocker, az_token):
    api_client = azure_api_client()
    azure_resource = AzureResources(include_subscriptions="002", api_client=api_client)
    resource_id = "subscriptions/002/resourceGroups/rg1/storageAccounts/sto2/containers/container1"
    role_assignments = list(azure_resource.role_assignments(resource_id))
    assert len(role_assignments) == 1
    for role_assignment in role_assignments:
        assert role_assignment.role_name == "Contributor"
        assert role_assignment.principal == Principal("appIduser2", "disNameuser2", "Iduser2", "0000-0000")
        assert str(role_assignment.scope) == resource_id
        assert role_assignment.resource == AzureResource(resource_id)


def test_create_service_principal(mocker, az_token):
    api_client = azure_api_client()
    azure_resource = AzureResources(api_client=api_client)
    global_spn = azure_resource.create_service_principal()
    assert global_spn.client_id == "appIduser1"
    assert global_spn.object_id == "Iduser1"
    assert global_spn.display_name == "disNameuser1"
    assert global_spn.directory_id == "dir1"
    assert global_spn.secret == "mypwd"


def test_create_service_principal_no_access(mocker, az_token):
    api_client = azure_api_client()
    api_client.put.side_effect = PermissionDenied()
    azure_resource = AzureResources(api_client=api_client)
    with pytest.raises(PermissionDenied):
        azure_resource.create_service_principal()


def test_apply_storage_permission(mocker, az_token):
    api_client = azure_api_client()
    azure_resource = AzureResources(api_client=api_client)
    azure_resource.apply_storage_permission("test", "resourceid1")
    path = "/resourceid1/providers/Microsoft.Authorization/roleAssignments/2a2b9908-6ea1-4ae2-8e65-a410df84e7d1?api-version=2022-04-01"
    body = {
        'properties': {
            'principalId': 'test',
            'principalType': 'ServicePrincipal',
            'roleDefinitionId': '/resourceid1/providers/Microsoft.Authorization/roleDefinitions/2a2b9908-6ea1-4ae2-8e65-a410df84e7d1',
        }
    }

    api_client.put.assert_called_with(path, "azure_graph", body)


def test_apply_storage_permission_no_access(az_token):
    api_client = azure_api_client()
    api_client.put.side_effect = PermissionDenied()
    azure_resource = AzureResources(api_client=api_client)
    with pytest.raises(PermissionDenied):
        azure_resource.apply_storage_permission("test", "resourceid1")


def test_azure_client_api_put_graph(mocker, az_token):
    mocker.patch("databricks.sdk.core.ApiClient.do", side_effect=get_az_api_mapping)
    w = create_autospec(WorkspaceClient)
    api_client = AzureAPIClient(w)
    result = api_client.put("/v1.0/servicePrincipals", "azure_graph", {"foo": "bar"})
    assert result.get("appId") == "appIduser1"


def test_azure_client_api_get_azure(mocker, az_token):
    mocker.patch("databricks.sdk.core.ApiClient.do", side_effect=get_az_api_mapping)
    w = create_autospec(WorkspaceClient)
    api_client = AzureAPIClient(w)
    subscriptions = api_client.get("/subscriptions", "azure_mgmt", {"foo": "bar"})
    assert len(subscriptions) == 1


def test_azure_client_api_get_graph(mocker, az_token):
    mocker.patch("databricks.sdk.core.ApiClient.do", side_effect=get_az_api_mapping)
    w = create_autospec(WorkspaceClient)
    api_client = AzureAPIClient(w)
    principal = api_client.get("/v1.0/directoryObjects/user1", "azure_graph")
    assert principal.get("appId") == "appIduser1"

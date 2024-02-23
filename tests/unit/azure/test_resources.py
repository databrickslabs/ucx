from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.azure.resources import AzureResource, AzureResources, Principal
from databricks.sdk.errors import NotFound, PermissionDenied
from . import get_az_api_mapping
from databricks.sdk.core import ApiClient

def test_subscriptions_no_subscription(mocker, az_token):
    w = create_autospec(WorkspaceClient)
    mocker.patch("databricks.sdk.core.ApiClient.do", side_effect=get_az_api_mapping)
    azure_resource = AzureResources(w, include_subscriptions="001")
    subscriptions = list(azure_resource.subscriptions())
    assert len(subscriptions) == 0


def test_subscriptions_valid_subscription(mocker, az_token):
    w = create_autospec(WorkspaceClient)
    mocker.patch("databricks.sdk.core.ApiClient.do", side_effect=get_az_api_mapping)
    azure_resource = AzureResources(w, include_subscriptions="002")
    subscriptions = list(azure_resource.subscriptions())
    assert len(subscriptions) == 1
    for subscription in subscriptions:
        assert subscription.name == "sub2"


def test_storage_accounts(mocker, az_token):
    w = create_autospec(WorkspaceClient)
    mocker.patch("databricks.sdk.core.ApiClient.do", side_effect=get_az_api_mapping)
    azure_resource = AzureResources(w, include_subscriptions="002")
    storage_accounts = list(azure_resource.storage_accounts())
    assert len(storage_accounts) == 2
    for storage_account in storage_accounts:
        assert storage_account.resource_group == "rg1"
        assert storage_account.storage_account in {"sto2", "sto3"}
        assert storage_account.container is None
        assert storage_account.subscription_id == "002"


def test_containers(mocker, az_token):
    w = create_autospec(WorkspaceClient)
    mocker.patch("databricks.sdk.core.ApiClient.do", side_effect=get_az_api_mapping)
    azure_resource = AzureResources(w, include_subscriptions="002")
    azure_storage = AzureResource("subscriptions/002/resourceGroups/rg1/storageAccounts/sto2")
    containers = list(azure_resource.containers(azure_storage))
    assert len(containers) == 3
    for container in containers:
        assert container.resource_group == "rg1"
        assert container.storage_account == "sto2"
        assert container.container in {"container1", "container2", "container3"}
        assert container.subscription_id == "002"


def test_role_assignments_storage(mocker, az_token):
    w = create_autospec(WorkspaceClient)
    mocker.patch("databricks.sdk.core.ApiClient.do", side_effect=get_az_api_mapping)
    azure_resource = AzureResources(w, include_subscriptions="002")
    resource_id = "subscriptions/002/resourceGroups/rg1/storageAccounts/sto2"
    role_assignments = list(azure_resource.role_assignments(resource_id))
    assert len(role_assignments) == 1
    for role_assignment in role_assignments:
        assert role_assignment.role_name == "Contributor"
        assert role_assignment.principal == Principal("appIduser2", "disNameuser2", "Iduser2")
        assert str(role_assignment.scope) == resource_id
        assert role_assignment.resource == AzureResource(resource_id)


def test_role_assignments_container(mocker, az_token):
    w = create_autospec(WorkspaceClient)
    mocker.patch("databricks.sdk.core.ApiClient.do", side_effect=get_az_api_mapping)
    azure_resource = AzureResources(w, include_subscriptions="002")
    resource_id = "subscriptions/002/resourceGroups/rg1/storageAccounts/sto2/containers/container1"
    role_assignments = list(azure_resource.role_assignments(resource_id))
    assert len(role_assignments) == 1
    for role_assignment in role_assignments:
        assert role_assignment.role_name == "Contributor"
        assert role_assignment.principal == Principal("appIduser2", "disNameuser2", "Iduser2")
        assert str(role_assignment.scope) == resource_id
        assert role_assignment.resource == AzureResource(resource_id)


def test_create_service_principal(mocker, az_token):
    w = create_autospec(WorkspaceClient)
    api_client = create_autospec(ApiClient)
    api_client.do.side_effect = get_az_api_mapping
    azure_resource = AzureResources(w)
    mocker.patch("databricks.sdk.core.ApiClient.do", side_effect=get_az_api_mapping)
    global_spn = azure_resource.create_service_principal()
    assert global_spn.client_id == "appIduser1"
    assert global_spn.object_id == "Iduser1"
    assert global_spn.display_name == "disNameuser1"
    assert global_spn.secret == "mypwd"


def test_create_service_principal_no_access(mocker, az_token):
    w = create_autospec(WorkspaceClient)
    api_client = create_autospec(ApiClient)
    api_client.do.side_effect = PermissionDenied()
    azure_resource = AzureResources(w)
    mocker.patch("databricks.sdk.core.ApiClient.do", side_effect=PermissionDenied())
    with pytest.raises(PermissionDenied):
        global_spn = azure_resource.create_service_principal()

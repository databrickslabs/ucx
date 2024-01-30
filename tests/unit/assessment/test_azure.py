import base64
import json
from unittest.mock import Mock, create_autospec

import pytest
from databricks.labs.blueprint.installation import MockInstallation
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.oauth import Token
from databricks.sdk.service.compute import (
    AutoScale,
    ClusterDetails,
    ClusterSource,
    ClusterSpec,
)
from databricks.sdk.service.jobs import (
    BaseJob,
    JobCluster,
    JobSettings,
    NotebookTask,
    Task,
)
from databricks.sdk.service.sql import EndpointConfPair
from databricks.sdk.service.workspace import GetSecretResponse

from databricks.labs.ucx.assessment.azure import (
    AzureResource,
    AzureResourcePermissions,
    AzureResources,
    AzureServicePrincipalCrawler,
    Principal,
)
from databricks.labs.ucx.assessment.pipelines import PipelineInfo
from databricks.labs.ucx.hive_metastore import ExternalLocations

from ..framework.mocks import MockBackend


@pytest.fixture
def az_token(mocker):
    token = json.dumps({"aud": "foo", "tid": "bar"}).encode("utf-8")
    str_token = base64.b64encode(token).decode("utf-8").replace("=", "")
    tok = Token(access_token=f"header.{str_token}.sig")
    mocker.patch("databricks.sdk.oauth.Refreshable.token", return_value=tok)


def get_az_api_mapping(*args, **kwargs):
    mapping = {
        "/v1.0/directoryObjects/user1": {"appId": "appIduser1", "displayName": "disNameuser1", "id": "Iduser1"},
        "/v1.0/directoryObjects/user2": {"appId": "appIduser2", "displayName": "disNameuser2", "id": "Iduser2"},
        "/v1.0/directoryObjects/user3": {"appId": "appIduser3", "displayName": "disNameuser3", "id": "Iduser3"},
        "/subscriptions": {
            "value": [
                {"displayName": "sub1", "subscriptionId": "001", "tenantId": "bar1"},
                {"displayName": "sub2", "subscriptionId": "002", "tenantId": "bar"},
                {"displayName": "sub3", "subscriptionId": "003", "tenantId": "bar3"},
            ]
        },
        "/subscriptions/002/providers/Microsoft.Storage/storageAccounts": {
            "value": [
                {
                    "name": "sto1",
                },
                {"name": "sto2", "id": "subscriptions/002/resourceGroups/rg1/storageAccounts/sto2"},
                {"name": "sto3", "id": "subscriptions/002/resourceGroups/rg1/storageAccounts/sto3"},
            ]
        },
        "subscriptions/002/resourceGroups/rg1/storageAccounts/sto2/blobServices/default/containers": {
            "value": [
                {
                    "name": "container1",
                    "id": "subscriptions/002/resourceGroups/rg1/storageAccounts/sto2/containers/container1",
                },
                {
                    "name": "container2",
                    "id": "subscriptions/002/resourceGroups/rg1/storageAccounts/sto2/containers/container2",
                },
                {
                    "name": "container3",
                    "id": "subscriptions/002/resourceGroups/rg1/storageAccounts/sto2/containers/container3",
                },
                {
                    "name": "container4",
                },
            ]
        },
        "subscriptions/002/resourceGroups/rg1/storageAccounts/sto1/providers/Microsoft.Authorization/roleAssignments": {
            "value": [
                {
                    "properties": {
                        "principalId": "user1",
                        "principalType": "User",
                        "roleDefinitionId": "id001",
                        "scope": "subscriptions/002/resourceGroups/rg1/storageAccounts/sto1",
                    },
                    "id": "rol1",
                },
            ]
        },
        "subscriptions/002/resourceGroups/rg1/storageAccounts/sto2/providers/Microsoft.Authorization/roleAssignments": {
            "value": [
                {
                    "properties": {
                        "principalId": "user2",
                        "principalType": "ServicePrincipal",
                        "roleDefinitionId": "id001",
                        "scope": "subscriptions/002/resourceGroups/rg1/storageAccounts/sto2",
                    },
                    "id": "rol1",
                },
            ]
        },
        "id001": {
            "id": "role1",
            "properties": {
                "roleName": "Contributor",
            },
        },
        "subscriptions/002/resourceGroups/rg1/storageAccounts/sto3/providers/Microsoft.Authorization/roleAssignments": {
            "value": [
                {
                    "properties": {
                        "principalId": "user3",
                        "principalType": "ServicePrincipal",
                        "roleDefinitionId": "id002",
                        "scope": "subscriptions/002/resourceGroups/rg1/storageAccounts/sto3",
                    },
                    "id": "rol1",
                },
                {
                    "properties": {
                        "principalId": "user3",
                        "principalType": "ServicePrincipal",
                        "roleDefinitionId": "id002",
                        "scope": "subscriptions/002/resourceGroups/rg1/storageAccounts/sto3",
                    },
                    "id": "rol2",
                },
            ]
        },
        "id002": {
            "id": "role2",
            "properties": {
                "roleName": "Storage Blob Data Owner",
            },
        },
        "subscriptions/002/resourceGroups/rg1/storageAccounts/sto2/containers/container1/providers/"
        "Microsoft.Authorization/roleAssignments": {
            "value": [
                {
                    "properties": {
                        "principalId": "user2",
                        "principalType": "ServicePrincipal",
                        "roleDefinitionId": "id001",
                        "scope": "subscriptions/002/resourceGroups/rg1/storageAccounts/sto2/containers/container1",
                    },
                    "id": "rol1",
                },
            ]
        },
        "subscriptions/002/resourceGroups/rg1/storageAccounts/sto2/containers/container2/providers/"
        "Microsoft.Authorization/roleAssignments": {
            "value": [
                {
                    "properties": {
                        "principalId": "user1",
                        "principalType": "User",
                        "roleDefinitionId": "id001",
                        "scope": "this",
                    },
                    "id": "rol1",
                },
            ]
        },
        "subscriptions/002/resourceGroups/rg1/storageAccounts/sto2/containers/container3/providers/"
        "Microsoft.Authorization/roleAssignments": {
            "value": [
                {
                    "properties": {
                        "principalId": "user3",
                        "principalType": "ServicePrincipal",
                        "roleDefinitionId": "id002",
                        "scope": "subscriptions/002/resourceGroups/rg1/storageAccounts/sto2/containers/container3",
                    },
                    "id": "rol1",
                },
                {
                    "properties": {
                        "principalId": "user3",
                        "principalType": "ServicePrincipal",
                        "roleDefinitionId": "id002",
                        "scope": "subscriptions/002/resourceGroups/rg1/storageAccounts/sto2",
                    },
                    "id": "rol2",
                },
            ]
        },
    }
    if args[1] in mapping:
        return mapping[args[1]]
    else:
        return {}


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
        assert storage_account.storage_account in ["sto2", "sto3"]
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
        assert container.container in ["container1", "container2", "container3"]
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


def test_save_spn_permissions_no_external_table(caplog):
    w = create_autospec(WorkspaceClient)
    rows = {"SELECT \\* FROM ucx.external_locations": []}
    backend = MockBackend(rows=rows)
    location = ExternalLocations(w, backend, "ucx")
    installation = MockInstallation()
    azure_resource_permission = AzureResourcePermissions(installation, w, AzureResources(w, include_subscriptions="002"), location)
    azure_resource_permission.save_spn_permissions()
    msg = "There are no external table present with azure storage account. Please check if assessment job is run"
    assert [rec.message for rec in caplog.records if msg in rec.message]


def test_save_spn_permissions_no_azure_storage_account():
    w = create_autospec(WorkspaceClient)
    rows = {"SELECT \\* FROM ucx.external_locations": [["s3://bucket1/folder1", "0"]]}
    backend = MockBackend(rows=rows)
    location = ExternalLocations(w, backend, "ucx")
    installation = MockInstallation()
    azure_resource_permission = AzureResourcePermissions(installation, w, AzureResources(w, include_subscriptions="002"), location)
    storage_accounts = azure_resource_permission._get_storage_accounts()
    assert len(storage_accounts) == 0


def test_save_spn_permissions_valid_azure_storage_account():
    w = create_autospec(WorkspaceClient)
    rows = {
        "SELECT \\* FROM ucx.external_locations": [
            ["s3://bucket1/folder1", "1"],
            ["abfss://continer1@storage1.dfs.core.windows.net/folder1", "1"],
        ]
    }
    backend = MockBackend(rows=rows)
    location = ExternalLocations(w, backend, "ucx")
    installation = MockInstallation()
    azure_resource_permission = AzureResourcePermissions(installation, w, AzureResources(w, include_subscriptions="002"), location)
    storage_accounts = azure_resource_permission._get_storage_accounts()
    assert storage_accounts[0] == "storage1"


def test_map_storage_with_spn_no_blob_permission(mocker, az_token):
    w = create_autospec(WorkspaceClient)
    location = ExternalLocations(w, MockBackend(), "ucx")
    resource_id = "subscriptions/002/resourceGroups/rg1/storageAccounts/sto3"
    installation = MockInstallation()
    azure_resource_permission = AzureResourcePermissions(installation, w, AzureResources(w, include_subscriptions="002"), location)
    mocker.patch("databricks.sdk.core.ApiClient.do", side_effect=get_az_api_mapping)
    storage_permission_mappings = azure_resource_permission._map_storage(AzureResource(resource_id))
    assert len(storage_permission_mappings) == 0


def test_get_role_assignments_with_spn_and_blob_permission(mocker, az_token):
    w = create_autospec(WorkspaceClient)
    location = ExternalLocations(w, MockBackend(), "ucx")
    resource_id = "subscriptions/002/resourceGroups/rg1/storageAccounts/sto2"
    installation = MockInstallation()
    azure_resource_permission = AzureResourcePermissions(installation, w, AzureResources(w, include_subscriptions="002"), location)
    mocker.patch("databricks.sdk.core.ApiClient.do", side_effect=get_az_api_mapping)
    storage_permission_mappings = azure_resource_permission._map_storage(AzureResource(resource_id))
    assert len(storage_permission_mappings) == 2
    for storage_permission_mapping in storage_permission_mappings:
        assert storage_permission_mapping.prefix == "abfss://container3@sto2.dfs.core.windows.net/"
        assert storage_permission_mapping.principal == "disNameuser3"
        assert storage_permission_mapping.privilege == "WRITE_FILES"
        assert storage_permission_mapping.client_id == "appIduser3"


def test_save_spn_permissions_no_valid_storage_accounts(caplog, mocker, az_token):
    w = create_autospec(WorkspaceClient)
    rows = {"SELECT \\* FROM ucx.external_locations": [["abfss://continer1@sto3.dfs.core.windows.net/folder1", 1]]}
    mocker.patch("databricks.sdk.core.ApiClient.do", side_effect=get_az_api_mapping)
    backend = MockBackend(rows=rows)
    location = ExternalLocations(w, backend, "ucx")
    installation = MockInstallation()
    azure_resource_permission = AzureResourcePermissions(installation, w, AzureResources(w, include_subscriptions="002"), location)
    azure_resource_permission.save_spn_permissions()
    assert [rec.message for rec in caplog.records if "No storage account found in current tenant" in rec.message]


def test_save_spn_permissions_valid_storage_accounts(caplog, mocker, az_token):
    w = create_autospec(WorkspaceClient)
    rows = {"SELECT \\* FROM ucx.external_locations": [["abfss://continer1@sto2.dfs.core.windows.net/folder1", 1]]}
    mocker.patch("databricks.sdk.core.ApiClient.do", side_effect=get_az_api_mapping)
    backend = MockBackend(rows=rows)
    location = ExternalLocations(w, backend, "ucx")
    installation = MockInstallation()
    azure_resource_permission = AzureResourcePermissions(installation, w, AzureResources(w, include_subscriptions="002"), location)
    azure_resource_permission.save_spn_permissions()
    installation.assert_file_written('azure_storage_account_info.csv', [{'client_id': 'appIduser3',
  'prefix': 'abfss://container3@sto2.dfs.core.windows.net/',
  'principal': 'disNameuser3',
  'privilege': 'WRITE_FILES'},
 {'client_id': 'appIduser3',
  'prefix': 'abfss://container3@sto2.dfs.core.windows.net/',
  'principal': 'disNameuser3',
  'privilege': 'WRITE_FILES'}])


def test_azure_spn_info_without_secret(mocker):
    sample_clusters = [
        ClusterDetails(
            autoscale=AutoScale(min_workers=1, max_workers=6),
            cluster_source=ClusterSource.UI,
            spark_conf={
                "spark.hadoop.fs.azure.account.oauth2.client.id.abcde.dfs.core.windows.net": "test123456789",
                "spark.hadoop.fs.azure.account.oauth2.client.endpoint.abcde.dfs.core.windows.net": "https://login"
                ".microsoftonline"
                ".com/dedededede/token",
                "spark.hadoop.fs.azure.account.oauth2.client.secret.abcde.dfs.core.windows.net": "{{secrets/abcff"
                "/sp_secret}}",
            },
            spark_context_id=5134472582179565315,
            spark_env_vars=None,
            spark_version="9.3.x-cpu-ml-scala2.12",
            cluster_id="0810-225833-atlanta69",
            cluster_name="Tech Summit FY24 Cluster-1",
        )
    ]
    sample_spns = [{"application_id": "test123456789", "secret_scope": "", "secret_key": ""}]
    ws = mocker.Mock()
    ws.clusters.list.return_value = sample_clusters
    ws.cluster_policies.get().policy_family_definition_overrides = None
    AzureServicePrincipalCrawler(ws, MockBackend(), "ucx")._list_all_cluster_with_spn_in_spark_conf()
    crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx")._assess_service_principals(sample_spns)
    result_set = list(crawler)

    assert len(result_set) == 1
    assert result_set[0].application_id == "test123456789"


def test_azure_service_principal_info_crawl(mocker):
    sample_clusters = [
        ClusterDetails(
            autoscale=AutoScale(min_workers=1, max_workers=6),
            cluster_source=ClusterSource.UI,
            spark_context_id=5134472582179565315,
            spark_env_vars=None,
            spark_version="9.3.x-cpu-ml-scala2.12",
            cluster_id="0810-225833-atlanta69",
            cluster_name="Tech Summit FY24 Cluster-1",
        ),
        ClusterDetails(
            cluster_name="Tech Summit FY24 Cluster-2",
            autoscale=AutoScale(min_workers=1, max_workers=6),
            spark_conf={
                "spark.hadoop.fs.azure.account."
                "oauth2.client.id.abcde.dfs.core.windows.net": "{{secrets/abcff/sp_app_client_id}}",
                "spark.hadoop.fs.azure.account."
                "oauth2.client.endpoint.abcde.dfs.core.windows.net": "https://login.microsoftonline.com/dedededede"
                "/token",
                "spark.hadoop.fs.azure.account."
                "oauth2.client.secret.abcde.dfs.core.windows.net": "{{secrets/abcff/sp_secret}}",
            },
            spark_context_id=5134472582179565315,
            spark_env_vars=None,
            spark_version="13.3.x-cpu-ml-scala2.12",
            cluster_id="0915-190044-3dqy6751",
            cluster_source=ClusterSource.UI,
        ),
    ]
    sample_pipelines = [
        PipelineInfo(
            creator_name="abcde.defgh@databricks.com",
            pipeline_name="New DLT Pipeline",
            pipeline_id="0112eae7-9d11-4b40-a2b8-6c83cb3c7497",
            success=1,
            failures="",
        )
    ]
    sample_jobs = [
        BaseJob(
            created_time=1694536604319,
            creator_user_name="anonymous@databricks.com",
            job_id=536591785949415,
            settings=JobSettings(
                compute=None,
                continuous=None,
                tasks=[
                    Task(
                        task_key="Ingest",
                        existing_cluster_id="0807-225846-motto493",
                        notebook_task=NotebookTask(
                            notebook_path="/Users/foo.bar@databricks.com/Customers/Example/Test/Load"
                        ),
                        timeout_seconds=0,
                    )
                ],
                timeout_seconds=0,
            ),
        ),
        BaseJob(
            created_time=1694536604319,
            creator_user_name="anonymous@databricks.com",
            job_id=536591785949416,
            settings=JobSettings(
                compute=None,
                continuous=None,
                job_clusters=[
                    JobCluster(
                        job_cluster_key="rrrrrrrr",
                        new_cluster=ClusterSpec(
                            autoscale=None,
                            node_type_id="Standard_DS3_v2",
                            num_workers=2,
                            spark_conf={
                                "spark.hadoop.fs.azure.account.oauth2.client.id.abcde.dfs"
                                ".core.windows.net": "1234567890",
                                "spark.databricks.delta.formatCheck.enabled": "false",
                            },
                        ),
                    ),
                ],
                tasks=[
                    Task(
                        task_key="Ingest",
                        notebook_task=NotebookTask(
                            notebook_path="/Users/foo.bar@databricks.com/Customers/Example/Test/Load"
                        ),
                        timeout_seconds=0,
                    )
                ],
                timeout_seconds=0,
            ),
        ),
        BaseJob(
            created_time=1694536604319,
            creator_user_name="anonymous@databricks.com",
            job_id=536591785949416,
            settings=JobSettings(
                compute=None,
                continuous=None,
                job_clusters=[
                    JobCluster(
                        job_cluster_key="rrrrrr",
                        new_cluster=ClusterSpec(
                            autoscale=None,
                            node_type_id="Standard_DS3_v2",
                            num_workers=2,
                            spark_conf={
                                "spark.databricks.delta.formatCheck.enabled": "false",
                            },
                        ),
                    ),
                ],
                tasks=[
                    Task(
                        task_key="Ingest",
                        notebook_task=NotebookTask(
                            notebook_path="/Users/foo.bar@databricks.com/Customers/Example/Test/Load"
                        ),
                        timeout_seconds=0,
                    )
                ],
                timeout_seconds=0,
            ),
        ),
        BaseJob(
            created_time=1694536604319,
            creator_user_name="anonymous@databricks.com",
            job_id=536591785949416,
            settings=JobSettings(
                compute=None,
                continuous=None,
                job_clusters=[
                    JobCluster(job_cluster_key="rrrrrr"),
                ],
                tasks=[
                    Task(
                        task_key="Ingest",
                        existing_cluster_id="0807-225846-motto493",
                        notebook_task=NotebookTask(
                            notebook_path="/Users/foo.bar@databricks.com/Customers/Example/Test/Load"
                        ),
                        timeout_seconds=0,
                    )
                ],
                timeout_seconds=0,
            ),
        ),
    ]
    ws = mocker.Mock()
    ws.clusters.list.return_value = sample_clusters
    ws.pipelines.list_pipelines.return_value = sample_pipelines
    config_dict = {
        "spark.hadoop.fs.azure.account.oauth2.client.id.abcde.dfs.core.windows.net": "wewewerty",
        "spark.hadoop.fs.azure.account.auth.type.abcde.dfs.core.windows.net": "SAS",
        "spark.hadoop.fs.azure.sas.token.provider.type.abcde.dfs."
        "core.windows.net": "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider",
        "spark.hadoop.fs.azure.sas.fixed.token.abcde.dfs.core.windows.net": "{{secrets/abcde_access/sasFixedToken}}",
    }
    ws.warehouses.get_workspace_warehouse_config().data_access_config = [
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.auth.type.storage_acct1.dfs.core.windows.net", value="OAuth"
        ),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth.provider.type.storage_acct1.dfs.core.windows.net",
            value="org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        ),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth2.client.id.storage_acct2.dfs.core.windows.net",
            value="dummy_application_id",
        ),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth2.client.secret.storage_acct1.dfs.core.windows.net",
            value="dfddsaaaaddwwdds",
        ),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth2.client.endpoint.storage_acct2.dfs.core.windows.net",
            value="https://login.microsoftonline.com/dummy_tenant_id/oauth2/token",
        ),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.auth.type.storage_acct2.dfs.core.windows.net", value="OAuth"
        ),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth.provider.type.storage_acct2.dfs.core.windows.net",
            value="org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        ),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth2.client.id.storage_acct1.dfs.core.windows.net",
            value="dummy_application_id_2",
        ),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth2.client.secret.storage_acct2.dfs.core.windows.net",
            value="dfddsaaaaddwwdds",
        ),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth2.client.endpoint.storage_acct1.dfs.core.windows.net",
            value="https://login.microsoftonline.com/dummy_tenant_id_2/oauth2/token",
        ),
    ]
    ws.pipelines.get().spec.configuration = config_dict
    ws.secrets.get_secret.return_value = GetSecretResponse(key="username", value=_SECRET_VALUE)
    ws.cluster_policies.get().policy_family_definition_overrides = None
    ws.jobs.list.return_value = sample_jobs
    spn_crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx")._crawl()

    assert len(spn_crawler) == 5


def test_azure_service_principal_info_spark_conf_crawl(mocker):
    sample_clusters = [
        ClusterDetails(
            autoscale=AutoScale(min_workers=1, max_workers=6),
            cluster_source=ClusterSource.UI,
            spark_context_id=5134472582179565315,
            spark_env_vars=None,
            spark_version="9.3.x-cpu-ml-scala2.12",
            cluster_id="0810-225833-atlanta69",
            cluster_name="Tech Summit FY24 Cluster-1",
        )
    ]
    sample_pipelines = [
        PipelineInfo(
            creator_name="abcde.defgh@databricks.com",
            pipeline_name="New DLT Pipeline",
            pipeline_id="0112eae7-9d11-4b40-a2b8-6c83cb3c7497",
            success=1,
            failures="",
        )
    ]
    sample_jobs = [
        BaseJob(
            created_time=1694536604319,
            creator_user_name="anonymous@databricks.com",
            job_id=536591785949415,
            settings=JobSettings(
                compute=None,
                continuous=None,
                tasks=[
                    Task(
                        task_key="Ingest",
                        existing_cluster_id="0807-225846-motto493",
                        notebook_task=NotebookTask(
                            notebook_path="/Users/foo.bar@databricks.com/Customers/Example/Test/Load"
                        ),
                        timeout_seconds=0,
                    )
                ],
                timeout_seconds=0,
            ),
        ),
        BaseJob(
            created_time=1694536604319,
            creator_user_name="anonymous@databricks.com",
            job_id=536591785949416,
            settings=JobSettings(
                compute=None,
                continuous=None,
                job_clusters=[
                    JobCluster(
                        job_cluster_key="rrrrrrr",
                        new_cluster=ClusterSpec(
                            autoscale=None,
                            node_type_id="Standard_DS3_v2",
                            num_workers=2,
                            spark_conf={
                                "spark.hadoop.fs.azure.account.oauth2.client.id.abcde.dfs"
                                ".core.windows.net": "1234567890",
                                "spark.databricks.delta.formatCheck.enabled": "false",
                            },
                        ),
                    ),
                ],
                tasks=[
                    Task(
                        task_key="Ingest",
                        notebook_task=NotebookTask(
                            notebook_path="/Users/foo.bar@databricks.com/Customers/Example/Test/Load"
                        ),
                        timeout_seconds=0,
                    )
                ],
                timeout_seconds=0,
            ),
        ),
        BaseJob(
            created_time=1694536604319,
            creator_user_name="anonymous@databricks.com",
            job_id=536591785949416,
            settings=JobSettings(
                compute=None,
                continuous=None,
                job_clusters=[
                    JobCluster(
                        job_cluster_key="rrrrrrr",
                        new_cluster=ClusterSpec(
                            autoscale=None,
                            node_type_id="Standard_DS3_v2",
                            num_workers=2,
                            spark_conf={
                                "spark.databricks.delta.formatCheck.enabled": "false",
                            },
                        ),
                    ),
                ],
                tasks=[
                    Task(
                        task_key="Ingest",
                        notebook_task=NotebookTask(
                            notebook_path="/Users/foo.bar@databricks.com/Customers/Example/Test/Load"
                        ),
                        timeout_seconds=0,
                    )
                ],
                timeout_seconds=0,
            ),
        ),
        BaseJob(
            created_time=1694536604319,
            creator_user_name="anonymous@databricks.com",
            job_id=536591785949416,
            settings=JobSettings(
                compute=None,
                continuous=None,
                job_clusters=[
                    JobCluster(job_cluster_key="rrrrrrr"),
                ],
                tasks=[
                    Task(
                        task_key="Ingest",
                        existing_cluster_id="0807-225846-motto493",
                        notebook_task=NotebookTask(
                            notebook_path="/Users/foo.bar@databricks.com/Customers/Example/Test/Load"
                        ),
                        timeout_seconds=0,
                    )
                ],
                timeout_seconds=0,
            ),
        ),
    ]
    ws = mocker.Mock()
    ws.clusters.list.return_value = sample_clusters
    ws.pipelines.list_pipelines.return_value = sample_pipelines
    config_dict = {}
    ws.pipelines.get().spec.configuration = config_dict
    ws.cluster_policies.get().policy_family_definition_overrides = None
    ws.jobs.list.return_value = sample_jobs
    ws.warehouses.get_workspace_warehouse_config().data_access_config = [
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.auth.type.storage_acct1.dfs.core.windows.net", value="OAuth"
        ),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth.provider.type.storage_acct1.dfs.core.windows.net",
            value="org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        ),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth2.client.id.storage_acct2.dfs.core.windows.net",
            value="dummy_application_id",
        ),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth2.client.secret.storage_acct1.dfs.core.windows.net",
            value="dfddsaaaaddwwdds",
        ),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth2.client.endpoint.storage_acct2.dfs.core.windows.net",
            value="https://login.microsoftonline.com/dummy_tenant_id/oauth2/token",
        ),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.auth.type.storage_acct2.dfs.core.windows.net", value="OAuth"
        ),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth.provider.type.storage_acct2.dfs.core.windows.net",
            value="org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        ),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth2.client.id.storage_acct1.dfs.core.windows.net",
            value="dummy_application_id_2",
        ),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth2.client.secret.storage_acct2.dfs.core.windows.net",
            value="dfddsaaaaddwwdds",
        ),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth2.client.endpoint.storage_acct1.dfs.core.windows.net",
            value="https://login.microsoftonline.com/dummy_tenant_id_2/oauth2/token",
        ),
    ]
    spn_crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx")._crawl()

    assert len(spn_crawler) == 3


def test_azure_service_principal_info_no_spark_conf_crawl(mocker):
    sample_clusters = [
        ClusterDetails(
            autoscale=AutoScale(min_workers=1, max_workers=6),
            cluster_source=ClusterSource.UI,
            spark_context_id=5134472582179565315,
            spark_env_vars=None,
            spark_version="9.3.x-cpu-ml-scala2.12",
            cluster_id="0810-225833-atlanta69",
            cluster_name="Tech Summit FY24 Cluster-1",
            spark_conf={
                "spark.hadoop.fs.azure.account.oauth2.client.id.abcde.dfs.core.windows.net": "",
            },
        )
    ]
    sample_pipelines = [
        PipelineInfo(
            creator_name="abcde.defgh@databricks.com",
            pipeline_name="New DLT Pipeline",
            pipeline_id="0112eae7-9d11-4b40-a2b8-6c83cb3c7497",
            success=1,
            failures="",
        )
    ]
    sample_jobs = [
        BaseJob(
            created_time=1694536604319,
            creator_user_name="anonymous@databricks.com",
            job_id=536591785949415,
            settings=JobSettings(
                compute=None,
                continuous=None,
                job_clusters=[
                    JobCluster(
                        job_cluster_key="rrrrrrr",
                        new_cluster=ClusterSpec(autoscale=None, node_type_id="Standard_DS3_v2", num_workers=2),
                    ),
                ],
                tasks=[
                    Task(
                        task_key="Ingest",
                        existing_cluster_id="0810-225833-atlanta69",
                        new_cluster=ClusterSpec(autoscale=None, node_type_id="Standard_DS3_v2", num_workers=2),
                        notebook_task=NotebookTask(
                            notebook_path="/Users/foo.bar@databricks.com/Customers/Example/Test/Load"
                        ),
                        timeout_seconds=0,
                    )
                ],
                timeout_seconds=0,
            ),
        ),
        BaseJob(
            created_time=1694536604319,
            creator_user_name="anonymous@databricks.com",
            job_id=536591785949415,
            settings=JobSettings(
                compute=None,
                continuous=None,
                job_clusters=[
                    JobCluster(
                        job_cluster_key="rrrrrrr",
                        new_cluster=ClusterSpec(autoscale=None, node_type_id="Standard_DS3_v2", num_workers=2),
                    ),
                ],
                tasks=[
                    Task(
                        task_key="Ingest",
                        new_cluster=ClusterSpec(autoscale=None, node_type_id="Standard_DS3_v2", num_workers=2),
                        notebook_task=NotebookTask(
                            notebook_path="/Users/foo.bar@databricks.com/Customers/Example/Test/Load"
                        ),
                        timeout_seconds=0,
                    )
                ],
                timeout_seconds=0,
            ),
        ),
    ]
    ws = mocker.Mock()
    ws.clusters.list.return_value = sample_clusters
    ws.pipelines.list_pipelines.return_value = sample_pipelines
    config_dict = {}
    ws.pipelines.get().spec.configuration = config_dict
    ws.jobs.list.return_value = sample_jobs
    ws.cluster_policies.get().policy_family_definition_overrides = json.dumps(
        {
            "spark_conf.fs.azure1.account.auth.type": {"type": "fixed", "value": "OAuth", "hidden": "true"},
            "spark_conf.fs.azure1.account.oauth.provider.type": {
                "type": "fixed",
                "value": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                "hidden": "true",
            },
            "spark_conf.fs.azure1.account.oauth2.client.id": {
                "type": "fixed",
                "value": "",
                "hidden": "true",
            },
            "spark_conf.fs.azure1.account.oauth2.client.secret": {
                "type": "fixed",
                "value": "gfgfgfgfggfggfgfdds",
                "hidden": "true",
            },
            "spark_conf.fs.azure1.account.oauth2.client.endpoint": {
                "type": "fixed",
                "value": "https://login.microsoftonline.com/1234ededed/oauth2/token",
                "hidden": "true",
            },
        }
    )
    ws.warehouses.get_workspace_warehouse_config().data_access_config = []
    spn_crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx")._crawl()

    assert len(spn_crawler) == 0


def test_azure_service_principal_info_policy_family_conf_crawl(mocker):
    sample_clusters = [
        ClusterDetails(
            autoscale=AutoScale(min_workers=1, max_workers=6),
            cluster_source=ClusterSource.UI,
            spark_context_id=5134472582179565315,
            spark_env_vars=None,
            spark_version="9.3.x-cpu-ml-scala2.12",
            cluster_id="0810-225833-atlanta69",
            cluster_name="Tech Summit FY24 Cluster-1",
            spark_conf={
                "spark.hadoop.fs.azure.account.oauth2.client.id.abcde.dfs.core.windows.net": "",
            },
        )
    ]
    sample_pipelines = [
        PipelineInfo(
            creator_name="abcde.defgh@databricks.com",
            pipeline_name="New DLT Pipeline",
            pipeline_id="0112eae7-9d11-4b40-a2b8-6c83cb3c7497",
            success=1,
            failures="",
        )
    ]
    sample_jobs = [
        BaseJob(
            created_time=1694536604319,
            creator_user_name="anonymous@databricks.com",
            job_id=536591785949415,
            settings=JobSettings(
                compute=None,
                continuous=None,
                job_clusters=[
                    JobCluster(
                        job_cluster_key="rrrrrrrr",
                        new_cluster=ClusterSpec(autoscale=None, node_type_id="Standard_DS3_v2", num_workers=2),
                    ),
                ],
                tasks=[
                    Task(
                        task_key="Ingest",
                        existing_cluster_id="0810-225833-atlanta69",
                        new_cluster=ClusterSpec(autoscale=None, node_type_id="Standard_DS3_v2", num_workers=2),
                        notebook_task=NotebookTask(
                            notebook_path="/Users/foo.bar@databricks.com/Customers/Example/Test/Load"
                        ),
                        timeout_seconds=0,
                    )
                ],
                timeout_seconds=0,
            ),
        ),
        BaseJob(
            created_time=1694536604319,
            creator_user_name="anonymous@databricks.com",
            job_id=536591785949415,
            settings=JobSettings(
                compute=None,
                continuous=None,
                job_clusters=[
                    JobCluster(
                        job_cluster_key="rrrrrrrrr",
                        new_cluster=ClusterSpec(autoscale=None, node_type_id="Standard_DS3_v2", num_workers=2),
                    ),
                ],
                tasks=[
                    Task(
                        task_key="Ingest",
                        new_cluster=ClusterSpec(autoscale=None, node_type_id="Standard_DS3_v2", num_workers=2),
                        notebook_task=NotebookTask(
                            notebook_path="/Users/foo.bar@databricks.com/Customers/Example/Test/Load"
                        ),
                        timeout_seconds=0,
                    )
                ],
                timeout_seconds=0,
            ),
        ),
    ]
    ws = mocker.Mock()
    ws.clusters.list.return_value = sample_clusters
    ws.pipelines.list_pipelines.return_value = sample_pipelines
    config_dict = {}
    ws.pipelines.get().spec.configuration = config_dict
    ws.jobs.list.return_value = sample_jobs
    ws.cluster_policies.get().policy_family_definition_overrides = json.dumps(
        {
            "spark_conf.fs.azure1.account.auth.type": {"type": "fixed", "value": "OAuth", "hidden": "true"},
            "spark_conf.fs.azure1.account.oauth.provider.type": {
                "type": "fixed",
                "value": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                "hidden": "true",
            },
            "spark_conf.fs.azure.account.oauth2.client.id": {
                "type": "fixed",
                "value": "",
                "hidden": "true",
            },
            "spark_conf.fs.azure1.account.oauth2.client.secret": {
                "type": "fixed",
                "value": "gfgfgfgfggfggfgfdds",
                "hidden": "true",
            },
            "spark_conf.fs.azure1.account.oauth2.client.endpoint": {
                "type": "fixed",
                "value": "https://login.microsoftonline.com/1234ededed/oauth2/token",
                "hidden": "true",
            },
        }
    )
    ws.warehouses.get_workspace_warehouse_config().data_access_config = []
    spn_crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx")._crawl()

    assert len(spn_crawler) == 0


def test_azure_service_principal_info_null_applid_crawl(mocker):
    sample_clusters = [
        ClusterDetails(
            autoscale=AutoScale(min_workers=1, max_workers=6),
            cluster_source=ClusterSource.UI,
            spark_context_id=5134472582179565315,
            spark_env_vars=None,
            spark_version="9.3.x-cpu-ml-scala2.12",
            cluster_id="0810-225833-atlanta69",
            cluster_name="Tech Summit FY24 Cluster-1",
            policy_id="bdqwbdqiwd1111",
        )
    ]
    sample_pipelines = [
        PipelineInfo(
            creator_name="abcde.defgh@databricks.com",
            pipeline_name="New DLT Pipeline",
            pipeline_id="0112eae7-9d11-4b40-a2b8-6c83cb3c7497",
            success=1,
            failures="",
        )
    ]
    sample_jobs = [
        BaseJob(
            created_time=1694536604319,
            creator_user_name="anonymous@databricks.com",
            job_id=536591785949415,
            settings=JobSettings(
                compute=None,
                continuous=None,
                job_clusters=[
                    JobCluster(
                        job_cluster_key="rrrrrrrrr",
                        new_cluster=ClusterSpec(autoscale=None, node_type_id="Standard_DS3_v2", num_workers=2),
                    ),
                ],
                tasks=[
                    Task(
                        task_key="Ingest",
                        existing_cluster_id="0810-225833-atlanta69",
                        new_cluster=ClusterSpec(autoscale=None, node_type_id="Standard_DS3_v2", num_workers=2),
                        notebook_task=NotebookTask(
                            notebook_path="/Users/foo.bar@databricks.com/Customers/Example/Test/Load"
                        ),
                        timeout_seconds=0,
                    )
                ],
                timeout_seconds=0,
            ),
        ),
        BaseJob(
            created_time=1694536604319,
            creator_user_name="anonymous@databricks.com",
            job_id=536591785949415,
            settings=JobSettings(
                compute=None,
                continuous=None,
                job_clusters=[
                    JobCluster(
                        job_cluster_key="rrrrrrrrr",
                        new_cluster=ClusterSpec(autoscale=None, node_type_id="Standard_DS3_v2", num_workers=2),
                    ),
                ],
                tasks=[
                    Task(
                        task_key="Ingest",
                        new_cluster=ClusterSpec(autoscale=None, node_type_id="Standard_DS3_v2", num_workers=2),
                        notebook_task=NotebookTask(
                            notebook_path="/Users/foo.bar@databricks.com/Customers/Example/Test/Load"
                        ),
                        timeout_seconds=0,
                    )
                ],
                timeout_seconds=0,
            ),
        ),
    ]
    ws = mocker.Mock()
    ws.warehouses.get_workspace_warehouse_config().data_access_config = []
    ws.clusters.list.return_value = sample_clusters
    ws.pipelines.list_pipelines.return_value = sample_pipelines
    config_dict = {}
    ws.pipelines.get().spec.configuration = config_dict
    ws.jobs.list.return_value = sample_jobs
    ws.cluster_policies.get().definition = json.dumps(
        {
            "spark_conf.fs.azure.account.auth.type": {"type": "fixed", "value": "OAuth", "hidden": "true"},
            "spark_conf.fs.azure.account.oauth.provider.type": {
                "type": "fixed",
                "value": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                "hidden": "true",
            },
            "spark_conf.fs.azure.account.oauth2.client.id": {
                "type": "fixed",
                "value": "",
                "hidden": "true",
            },
            "spark_conf.fs.azure.account.oauth2.client.secret": {
                "type": "fixed",
                "value": "gfgfgfgfggfggfgfdds",
                "hidden": "true",
            },
            "spark_conf.fs.azure.account.oauth2.client.endpoint": {
                "type": "fixed",
                "value": "https://login.microsoftonline.com/1234ededed/oauth2/token",
                "hidden": "true",
            },
        }
    )
    ws.cluster_policies.get().policy_family_definition_overrides = None
    spn_crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx")._crawl()
    assert len(spn_crawler) == 0


def test_azure_spn_info_with_secret(mocker):
    sample_clusters = [
        ClusterDetails(
            cluster_name="Tech Summit FY24 Cluster",
            autoscale=AutoScale(min_workers=1, max_workers=6),
            spark_conf={
                "spark.hadoop.fs.azure.account.oauth2.client.id.abcde.dfs.core.windows.net": "{{secrets/abcff"
                "/sp_app_client_id}}",
                "spark.hadoop.fs.azure.account.oauth2.client.endpoint.abcde.dfs.core.windows.net": "https://login"
                ".microsoftonline"
                ".com/dedededede"
                "/token",
                "spark.hadoop.fs.azure.account.oauth2.client.secret.abcde.dfs.core.windows.net": "{{secrets/abcff"
                "/sp_secret}}",
            },
            spark_context_id=5134472582179565315,
            spark_env_vars=None,
            spark_version="13.3.x-cpu-ml-scala2.12",
            cluster_id="0915-190044-3dqy6751",
        )
    ]
    sample_spns = [{"application_id": "test123456780", "secret_scope": "abcff", "secret_key": "sp_app_client_id"}]
    ws = mocker.Mock()
    ws.clusters.list.return_value = sample_clusters
    crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx")._assess_service_principals(sample_spns)
    result_set = list(crawler)

    assert len(result_set) == 1
    assert result_set[0].application_id == "test123456780"


def test_spn_with_spark_config_snapshot_try_fetch(mocker):
    sample_spns = [
        {
            "application_id": "test123456780",
            "secret_scope": "abcff",
            "secret_key": "sp_app_client_id",
            "tenant_id": "dummy",
            "storage_account": "SA_Dummy",
        }
    ]
    mock_ws = Mock()
    crawler = AzureServicePrincipalCrawler(mock_ws, MockBackend(), "ucx")
    crawler._fetch = Mock(return_value=sample_spns)
    crawler._crawl = Mock(return_value=sample_spns)

    result_set = crawler.snapshot()

    assert len(result_set) == 1


def test_spn_with_spark_config_snapshot(mocker):
    sample_spns = [{"application_id": "test123456780", "secret_scope": "abcff", "secret_key": "sp_app_client_id"}]
    mock_ws = Mock()
    crawler = AzureServicePrincipalCrawler(mock_ws, MockBackend(), "ucx")
    crawler._try_fetch = Mock(return_value=sample_spns)
    crawler._crawl = Mock(return_value=sample_spns)

    result_set = crawler.snapshot()

    assert len(result_set) == 1
    assert result_set[0] == {
        "application_id": "test123456780",
        "secret_scope": "abcff",
        "secret_key": "sp_app_client_id",
    }


def test_list_all_cluster_with_spn_in_spark_conf_with_secret(mocker):
    sample_clusters = [
        ClusterDetails(
            cluster_name="Tech Summit FY24 Cluster",
            autoscale=AutoScale(min_workers=1, max_workers=6),
            spark_conf={
                "spark.hadoop.fs.azure.account."
                "oauth2.client.id.abcde.dfs.core.windows.net": "{{secrets/abcff/sp_app_client_id}}",
                "spark.hadoop.fs.azure.account."
                "oauth2.client.endpoint.abcde.dfs.core.windows.net": "https://login.microsoftonline.com/dedededede"
                "/token",
                "spark.hadoop.fs.azure.account."
                "oauth2.client.secret.abcde.dfs.core.windows.net": "{{secrets/abcff/sp_secret}}",
            },
            spark_context_id=5134472582179565315,
            spark_env_vars=None,
            spark_version="13.3.x-cpu-ml-scala2.12",
            cluster_id="0915-190044-3dqy6751",
        )
    ]

    ws = mocker.Mock()
    ws.clusters.list.return_value = sample_clusters
    ws.secrets.get_secret.return_value = GetSecretResponse(key="username", value=_SECRET_VALUE)
    ws.cluster_policies.get().policy_family_definition_overrides = None
    crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx")._list_all_cluster_with_spn_in_spark_conf()
    result_set = list(crawler)

    assert len(result_set) == 1


def test_list_all_wh_config_with_spn_no_secret(mocker):
    ws = mocker.Mock()
    ws.warehouses.get_workspace_warehouse_config().data_access_config = [
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.auth.type.storage_acct1.dfs.core.windows.net", value="OAuth"
        ),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth.provider.type.storage_acct1.dfs.core.windows.net",
            value="org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        ),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth2.client.id.storage_acct2.dfs.core.windows.net",
            value="dummy_application_id",
        ),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth2.client.secret.storage_acct1.dfs.core.windows.net",
            value="dfddsaaaaddwwdds",
        ),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth2.client.endpoint.storage_acct2.dfs.core.windows.net",
            value="https://login.microsoftonline.com/dummy_tenant_id/oauth2/token",
        ),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.auth.type.storage_acct2.dfs.core.windows.net", value="OAuth"
        ),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth.provider.type.storage_acct2.dfs.core.windows.net",
            value="org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        ),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth2.client.id.storage_acct1.dfs.core.windows.net",
            value="dummy_application_id_2",
        ),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth2.client.secret.storage_acct2.dfs.core.windows.net",
            value="dfddsaaaaddwwdds",
        ),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth2.client.endpoint.storage_acct1.dfs.core.windows.net",
            value="https://login.microsoftonline.com/dummy_tenant_id_2/oauth2/token",
        ),
    ]
    result_set = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx")._list_all_spn_in_sql_warehouses_spark_conf()

    assert len(result_set) == 2
    assert result_set[0].get("application_id") == "dummy_application_id"
    assert result_set[0].get("tenant_id") == "dummy_tenant_id"
    assert result_set[0].get("storage_account") == "storage_acct2"


def test_list_all_wh_config_with_spn_and_secret(mocker):
    ws = mocker.Mock()
    ws.warehouses.get_workspace_warehouse_config().data_access_config = [
        EndpointConfPair(key="spark.hadoop.fs.azure.account.auth.type.abcde.dfs.core.windows.net", value="OAuth"),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth.provider.type.xyz.dfs.core.windows.net",
            value="org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        ),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth2.client.id.abcde.dfs.core.windows.net",
            value="dummy_application_id",
        ),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth2.client.secret.xyz.dfs.core.windows.net",
            value="ddddddddddddddddddd",
        ),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth2.client.endpoint.abcde.dfs.core.windows.net",
            value="https://login.microsoftonline.com/dummy_tenant_id/oauth2/token",
        ),
        EndpointConfPair(key="spark.hadoop.fs.azure.account.auth.type.xyz.dfs.core.windows.net", value="OAuth"),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth.provider.type.abcde.dfs.core.windows.net",
            value="org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        ),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth2.client.id.xyz.dfs.core.windows.net",
            value="{{secrets/dummy_scope/sp_app_client_id}}",
        ),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth2.client.secret.abcde.dfs.core.windows.net",
            value="ddddddddddddddddddd",
        ),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth2.client.endpoint.xyz.dfs.core.windows.net",
            value="https://login.microsoftonline.com/dummy_tenant_id2/oauth2/token",
        ),
    ]
    mocker.Mock().secrets.get_secret()
    ws.secrets.get_secret.return_value = GetSecretResponse(key="username", value=_SECRET_VALUE)
    result_set = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx")._list_all_spn_in_sql_warehouses_spark_conf()

    assert len(result_set) == 2
    assert result_set[0].get("tenant_id") == "dummy_tenant_id"
    assert result_set[0].get("storage_account") == "abcde"


def test_list_all_clusters_spn_in_spark_conf_with_tenant(mocker):
    sample_clusters = [
        ClusterDetails(
            cluster_name="Tech Summit FY24 Cluster",
            autoscale=AutoScale(min_workers=1, max_workers=6),
            spark_conf={
                "spark.hadoop.fs.azure.account."
                "oauth2.client.id.abcde.dfs.core.windows.net": "{{secrets/abcff/sp_app_client_id}}",
                "spark.hadoop.fs.azure.account."
                "oauth2.client.endpoint.abcde.dfs.core.windows.net": "https://login.microsoftonline.com/dummy-tenant"
                "-id/oauth2/token",
                "spark.hadoop.fs.azure.account."
                "oauth2.client.secret.abcde.dfs.core.windows.net": "{{secrets/abcff/sp_secret}}",
            },
            spark_context_id=5134472582179565315,
            spark_env_vars=None,
            spark_version="13.3.x-cpu-ml-scala2.12",
            cluster_id="0915-190044-3dqy6751",
        )
    ]

    ws = mocker.Mock()
    ws.clusters.list.return_value = sample_clusters
    ws.cluster_policies.get().policy_family_definition_overrides = None
    ws.secrets.get_secret.return_value = GetSecretResponse(key="username", value=_SECRET_VALUE)
    result_set = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx")._list_all_cluster_with_spn_in_spark_conf()

    assert len(result_set) == 1
    assert result_set[0].get("tenant_id") == "dummy-tenant-id"


def test_azure_service_principal_info_policy_conf(mocker):
    sample_clusters = [
        ClusterDetails(
            autoscale=AutoScale(min_workers=1, max_workers=6),
            cluster_source=ClusterSource.UI,
            spark_context_id=5134472582179565315,
            spark_env_vars=None,
            spark_version="9.3.x-cpu-ml-scala2.12",
            cluster_id="0810-225833-atlanta69",
            cluster_name="Tech Summit FY24 Cluster-1",
            policy_id="1234567890",
        )
    ]
    sample_pipelines = [
        PipelineInfo(
            creator_name="abcde.defgh@databricks.com",
            pipeline_name="New DLT Pipeline",
            pipeline_id="0112eae7-9d11-4b40-a2b8-6c83cb3c7497",
            success=1,
            failures="",
        )
    ]
    sample_jobs = [
        BaseJob(
            created_time=1694536604319,
            creator_user_name="anonymous@databricks.com",
            job_id=536591785949415,
            settings=JobSettings(
                compute=None,
                continuous=None,
                job_clusters=[
                    JobCluster(
                        job_cluster_key="rrrrrrrrr",
                        new_cluster=ClusterSpec(
                            autoscale=None,
                            node_type_id="Standard_DS3_v2",
                            num_workers=2,
                            policy_id="1111111",
                            spark_conf={
                                "spark.hadoop.fs.azure.account.oauth2.client.id.abcde.dfs"
                                ".core.windows.net": "1234567890",
                                "spark.databricks.delta.formatCheck.enabled": "false",
                                "fs.azure.account.oauth2.client.endpoint.dummy.dfs.core.windows.net": "https://login.microsoftonline.com/dummy-123tenant-123/oauth2/token",
                            },
                        ),
                    ),
                ],
                tasks=[
                    Task(
                        task_key="Ingest",
                        notebook_task=NotebookTask(
                            notebook_path="/Users/foo.bar@databricks.com/Customers/Example/Test/Load"
                        ),
                        timeout_seconds=0,
                    )
                ],
                timeout_seconds=0,
            ),
        )
    ]
    ws = mocker.Mock()
    ws.clusters.list.return_value = sample_clusters
    ws.pipelines.list_pipelines.return_value = sample_pipelines
    config_dict = {}
    ws.pipelines.get().spec.configuration = config_dict
    ws.jobs.list.return_value = sample_jobs
    ws.cluster_policies.get().definition = json.dumps(
        {
            "spark_conf.fs.azure.account.auth.type": {"type": "fixed", "value": "OAuth", "hidden": "true"},
            "spark_conf.fs.azure.account.oauth.provider.type": {
                "type": "fixed",
                "value": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                "hidden": "true",
            },
            "spark_conf.fs.azure.account.oauth2.client.id": {
                "type": "fixed",
                "value": "dummyclientidfromprofile",
                "hidden": "true",
            },
            "spark_conf.fs.azure.account.oauth2.client.secret": {
                "type": "fixed",
                "value": "gfgfgfgfggfggfgfdds",
                "hidden": "true",
            },
            "spark_conf.fs.azure.account.oauth2.client.endpoint": {
                "type": "fixed",
                "value": "https://login.microsoftonline.com/1234ededed/oauth2/token",
                "hidden": "true",
            },
        }
    )
    ws.cluster_policies.get().policy_family_definition_overrides = None
    ws.warehouses.get_workspace_warehouse_config().data_access_config = [
        EndpointConfPair(key="spark.hadoop.fs.azure.account.auth.type.abcde.dfs.core.windows.net", value="OAuth"),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth.provider.type.xyz.dfs.core.windows.net",
            value="org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        ),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth2.client.id.abcde.dfs.core.windows.net",
            value="dummy_application_id",
        ),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth2.client.secret.xyz.dfs.core.windows.net",
            value="ddddddddddddddddddd",
        ),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth2.client.endpoint.abcde.dfs.core.windows.net",
            value="https://login.microsoftonline.com/dummy_tenant_id/oauth2/token",
        ),
        EndpointConfPair(key="spark.hadoop.fs.azure.account.auth.type.xyz.dfs.core.windows.net", value="OAuth"),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth.provider.type.abcde.dfs.core.windows.net",
            value="org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        ),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth2.client.id.xyz.dfs.core.windows.net",
            value="{{secrets/dummy_scope/sp_app_client_id}}",
        ),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth2.client.secret.abcde.dfs.core.windows.net",
            value="ddddddddddddddddddd",
        ),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth2.client.endpoint.xyz.dfs.core.windows.net",
            value="https://login.microsoftonline.com/dummy_tenant_id2/oauth2/token",
        ),
    ]
    ws.secrets.get_secret.return_value = GetSecretResponse(key="username", value=_SECRET_VALUE)
    spn_crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx")._crawl()

    assert len(spn_crawler) == 4


def test_azure_service_principal_info_dedupe(mocker):
    sample_clusters = [
        ClusterDetails(
            autoscale=AutoScale(min_workers=1, max_workers=6),
            cluster_source=ClusterSource.UI,
            spark_context_id=5134472582179565315,
            spark_env_vars=None,
            spark_version="9.3.x-cpu-ml-scala2.12",
            cluster_id="0810-225833-atlanta69",
            cluster_name="Tech Summit FY24 Cluster-1",
            policy_id="1234567890",
        )
    ]
    sample_pipelines = [
        PipelineInfo(
            creator_name="abcde.defgh@databricks.com",
            pipeline_name="New DLT Pipeline",
            pipeline_id="0112eae7-9d11-4b40-a2b8-6c83cb3c7497",
            success=1,
            failures="",
        )
    ]
    sample_jobs = [
        BaseJob(
            created_time=1694536604319,
            creator_user_name="anonymous@databricks.com",
            job_id=536591785949415,
            settings=JobSettings(
                compute=None,
                continuous=None,
                job_clusters=[
                    JobCluster(
                        job_cluster_key="rrrrrrrrr",
                        new_cluster=ClusterSpec(
                            autoscale=None,
                            node_type_id="Standard_DS3_v2",
                            num_workers=2,
                            policy_id="1111111",
                            spark_conf={
                                "spark.hadoop.fs.azure.account.auth.type.abcde.dfs.core.windows.net": "OAuth",
                                "spark.hadoop.fs.azure.account.oauth.provider.type.abcde.dfs.core.windows.net": ""
                                "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                                "spark.hadoop.fs.azure.account.oauth2.client.id.abcde.dfs.core.windows.net": ""
                                "dummy_application_id",
                                "spark.hadoop.fs.azure.account.oauth2.client.secret.abcde.dfs.core.windows.net": ""
                                "ddddddddddddddddddd",
                                "spark.hadoop.fs.azure.account.oauth2.client.endpoint.abcde.dfs.core.windows.net": ""
                                "https://login.microsoftonline.com/dummy_tenant_id/oauth2/token",
                            },
                        ),
                    ),
                ],
                tasks=[
                    Task(
                        task_key="Ingest",
                        notebook_task=NotebookTask(
                            notebook_path="/Users/foo.bar@databricks.com/Customers/Example/Test/Load"
                        ),
                        timeout_seconds=0,
                    )
                ],
                timeout_seconds=0,
            ),
        )
    ]
    ws = mocker.Mock()
    ws.clusters.list.return_value = sample_clusters
    ws.pipelines.list_pipelines.return_value = sample_pipelines
    config_dict = {}
    ws.pipelines.get().spec.configuration = config_dict
    ws.jobs.list.return_value = sample_jobs
    ws.secrets.get_secret.return_value = GetSecretResponse(key="username", value=_SECRET_VALUE)
    ws.cluster_policies.get().definition = json.dumps(
        {
            "spark_conf.fs.azure.account.auth.type": {"type": "fixed", "value": "OAuth", "hidden": "true"},
            "spark_conf.fs.azure.account.oauth.provider.type": {
                "type": "fixed",
                "value": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                "hidden": "true",
            },
        }
    )
    ws.cluster_policies.get().policy_family_definition_overrides = None
    ws.warehouses.get_workspace_warehouse_config().data_access_config = [
        EndpointConfPair(key="spark.hadoop.fs.azure.account.auth.type.abcde.dfs.core.windows.net", value="OAuth"),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth.provider.type.xyz.dfs.core.windows.net",
            value="org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        ),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth2.client.id.abcde.dfs.core.windows.net",
            value="dummy_application_id",
        ),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth2.client.secret.xyz.dfs.core.windows.net",
            value="ddddddddddddddddddd",
        ),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth2.client.endpoint.abcde.dfs.core.windows.net",
            value="https://login.microsoftonline.com/dummy_tenant_id/oauth2/token",
        ),
        EndpointConfPair(key="spark.hadoop.fs.azure.account.auth.type.xyz.dfs.core.windows.net", value="OAuth"),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth.provider.type.abcde.dfs.core.windows.net",
            value="org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
        ),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth2.client.id.xyz.dfs.core.windows.net",
            value="{{secrets/dummy_scope/sp_app_client_id}}",
        ),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth2.client.secret.abcde.dfs.core.windows.net",
            value="ddddddddddddddddddd",
        ),
        EndpointConfPair(
            key="spark.hadoop.fs.azure.account.oauth2.client.endpoint.xyz.dfs.core.windows.net",
            value="https://login.microsoftonline.com/dummy_tenant_id2/oauth2/token",
        ),
    ]
    spn_crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx")._crawl()

    assert len(spn_crawler) == 2


def test_list_all_pipeline_with_conf_spn_in_spark_conf(mocker):
    sample_pipelines = [
        PipelineInfo(
            creator_name="abcde.defgh@databricks.com",
            pipeline_name="New DLT Pipeline",
            pipeline_id="0112eae7-9d11-4b40-a2b8-6c83cb3c7497",
            success=1,
            failures="",
        )
    ]
    ws = mocker.Mock()
    ws.pipelines.list_pipelines.return_value = sample_pipelines
    config_dict = {
        "spark.hadoop.fs.azure.account.oauth2.client.id.newstorageacct.dfs.core.windows.net": ""
        "pipeline_dummy_application_id",
        "spark.hadoop.fs.azure.account.oauth2.client.endpoint.newstorageacct.dfs.core.windows.net": ""
        "https://login.microsoftonline.com/directory_12345/oauth2/token",
        "spark.hadoop.fs.azure.sas.fixed.token.abcde.dfs.core.windows.net": "{{secrets/abcde_access/sasFixedToken}}",
    }
    ws.pipelines.get().spec.configuration = config_dict

    result_set = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx")._list_all_pipeline_with_spn_in_spark_conf()

    assert len(result_set) == 1
    assert result_set[0].get("storage_account") == "newstorageacct"
    assert result_set[0].get("tenant_id") == "directory_12345"
    assert result_set[0].get("application_id") == "pipeline_dummy_application_id"


def test_list_all_pipeline_wo_conf_spn_in_spark_conf(mocker):
    sample_pipelines = [
        PipelineInfo(
            creator_name="abcde.defgh@databricks.com",
            pipeline_name="New DLT Pipeline",
            pipeline_id="0112eae7-9d11-4b40-a2b8-6c83cb3c7497",
            success=1,
            failures="",
        )
    ]
    ws = mocker.Mock()
    ws.pipelines.list_pipelines.return_value = sample_pipelines
    config_dict = {}
    ws.pipelines.get().spec.configuration = config_dict
    result_set = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx")._list_all_pipeline_with_spn_in_spark_conf()

    assert len(result_set) == 0


def test_list_all_pipeline_with_conf_spn_tenat(mocker):
    sample_pipelines = [
        PipelineInfo(
            creator_name="abcde.defgh@databricks.com",
            pipeline_name="New DLT Pipeline",
            pipeline_id="0112eae7-9d11-4b40-a2b8-6c83cb3c7497",
            success=1,
            failures="",
        )
    ]
    ws = mocker.Mock()
    ws.pipelines.list_pipelines.return_value = sample_pipelines
    config_dict = {
        "spark.hadoop.fs.azure.account.oauth2.client.id.newstorageacct.dfs.core.windows.net": ""
        "pipeline_dummy_application_id",
        "spark.hadoop.fs.azure1.account.oauth2.client.endpoint.newstorageacct.dfs.core.windows.net": ""
        "https://login.microsoftonline.com/directory_12345/oauth2/token",
        "spark.hadoop.fs.azure.sas.fixed.token.abcde.dfs.core.windows.net": "{{secrets/abcde_access/sasFixedToken}}",
    }
    ws.pipelines.get().spec.configuration = config_dict

    result_set = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx")._list_all_pipeline_with_spn_in_spark_conf()

    assert len(result_set) == 1
    assert result_set[0].get("storage_account") == "newstorageacct"
    assert result_set[0].get("application_id") == "pipeline_dummy_application_id"


def test_list_all_pipeline_with_conf_spn_secret(mocker):
    sample_pipelines = [
        PipelineInfo(
            creator_name="abcde.defgh@databricks.com",
            pipeline_name="New DLT Pipeline",
            pipeline_id="0112eae7-9d11-4b40-a2b8-6c83cb3c7497",
            success=1,
            failures="",
        )
    ]
    ws = mocker.Mock()
    ws.pipelines.list_pipelines.return_value = sample_pipelines
    config_dict = {
        "spark.hadoop.fs.azure.account.oauth2.client.id.newstorageacct.dfs.core.windows"
        ".net": "{{secrets/abcde_access/sasFixedToken}}",
        "spark.hadoop.fs.azure1.account.oauth2.client."
        "endpoint.newstorageacct.dfs.core.windows.net": "https://"
        "login.microsoftonline.com/directory_12345/oauth2/token",
        "spark.hadoop.fs.azure.sas.fixed.token.abcde.dfs.core.windows.net": "{{secrets/abcde_access/sasFixedToken}}",
    }
    ws.pipelines.get().spec.configuration = config_dict
    ws.secrets.get_secret.return_value = GetSecretResponse(key="username", value=_SECRET_VALUE)
    result_set = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx")._list_all_pipeline_with_spn_in_spark_conf()

    assert len(result_set) == 1
    assert result_set[0].get("storage_account") == "newstorageacct"


def test_azure_service_principal_info_policy_family(mocker):
    sample_clusters = [
        ClusterDetails(
            autoscale=AutoScale(min_workers=1, max_workers=6),
            cluster_source=ClusterSource.UI,
            spark_context_id=5134472582179565315,
            spark_env_vars=None,
            spark_version="9.3.x-cpu-ml-scala2.12",
            cluster_id="0810-225833-atlanta69",
            cluster_name="Tech Summit FY24 Cluster-1",
            spark_conf={"spark.hadoop.fs.azure.account.oauth2.client.id.abcde.dfs.core.windows.net": ""},
            policy_id="D96308F1BF0003A9",
        )
    ]
    sample_pipelines = [
        PipelineInfo(
            creator_name="abcde.defgh@databricks.com",
            pipeline_name="New DLT Pipeline",
            pipeline_id="0112eae7-9d11-4b40-a2b8-6c83cb3c7497",
            success=1,
            failures="",
        )
    ]
    sample_jobs = [
        BaseJob(
            created_time=1694536604319,
            creator_user_name="anonymous@databricks.com",
            job_id=536591785949415,
            settings=JobSettings(
                compute=None,
                continuous=None,
                job_clusters=[
                    JobCluster(
                        job_cluster_key="rrrrrrrrr",
                        new_cluster=ClusterSpec(autoscale=None, node_type_id="Standard_DS3_v2", num_workers=2),
                    ),
                ],
                tasks=[
                    Task(
                        task_key="Ingest",
                        existing_cluster_id="0810-225833-atlanta69",
                        new_cluster=ClusterSpec(autoscale=None, node_type_id="Standard_DS3_v2", num_workers=2),
                        notebook_task=NotebookTask(
                            notebook_path="/Users/foo.bar@databricks.com/Customers/Example/Test/Load"
                        ),
                        timeout_seconds=0,
                    )
                ],
                timeout_seconds=0,
            ),
        ),
        BaseJob(
            created_time=1694536604319,
            creator_user_name="anonymous@databricks.com",
            job_id=536591785949415,
            settings=JobSettings(
                compute=None,
                continuous=None,
                job_clusters=[
                    JobCluster(
                        job_cluster_key="rrrrrrrr",
                        new_cluster=ClusterSpec(autoscale=None, node_type_id="Standard_DS3_v2", num_workers=2),
                    ),
                ],
                tasks=[
                    Task(
                        task_key="Ingest",
                        new_cluster=ClusterSpec(autoscale=None, node_type_id="Standard_DS3_v2", num_workers=2),
                        notebook_task=NotebookTask(
                            notebook_path="/Users/foo.bar@databricks.com/Customers/Example/Test/Load"
                        ),
                        timeout_seconds=0,
                    )
                ],
                timeout_seconds=0,
            ),
        ),
    ]
    ws = mocker.Mock()
    ws.clusters.list.return_value = sample_clusters
    ws.pipelines.list_pipelines.return_value = sample_pipelines
    config_dict = {}
    ws.pipelines.get().spec.configuration = config_dict
    ws.jobs.list.return_value = sample_jobs
    ws.cluster_policies.get().definition = json.dumps({})
    ws.cluster_policies.get().policy_family_definition_overrides = json.dumps(
        {
            "spark_conf.fs.azure.account.auth.type": {"type": "fixed", "value": "OAuth", "hidden": "true"},
            "spark_conf.fs.azure.account.oauth.provider.type": {
                "type": "fixed",
                "value": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                "hidden": "true",
            },
            "spark_conf.fs.azure.account.oauth2.client.id": {
                "type": "fixed",
                "value": "dummy_appl_id",
                "hidden": "true",
            },
            "spark_conf.fs.azure.account.oauth2.client.secret": {
                "type": "fixed",
                "value": "gfgfgfgfggfggfgfdds",
                "hidden": "true",
            },
            "spark_conf.fs.azure.account.oauth2.client.endpoint": {
                "type": "fixed",
                "value": "https://login.microsoftonline.com/dummy_tenant_id/oauth2/token",
                "hidden": "true",
            },
        }
    )
    ws.warehouses.get_workspace_warehouse_config().data_access_config = []
    spn_crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx")._crawl()

    assert len(spn_crawler) == 1
    assert spn_crawler[0].application_id == "dummy_appl_id"
    assert spn_crawler[0].tenant_id == "dummy_tenant_id"


def test_list_all_pipeline_with_conf_spn_secret_unavlbl(mocker):
    sample_pipelines = [
        PipelineInfo(
            creator_name="abcde.defgh@databricks.com",
            pipeline_name="New DLT Pipeline",
            pipeline_id="0112eae7-9d11-4b40-a2b8-6c83cb3c7497",
            success=1,
            failures="",
        )
    ]
    ws = mocker.Mock()
    ws.pipelines.list_pipelines.return_value = sample_pipelines
    config_dict = {
        "spark.hadoop.fs.azure.account.oauth2.client.id.newstorageacct.dfs.core.windows"
        ".net": "{{secrets/reallyreallyasecret/sasFixedToken}}",
        "spark.hadoop.fs.azure.account.oauth2.client."
        "endpoint.newstorageacct.dfs.core.windows.net": "https://"
        "login.microsoftonline.com/directory_12345/oauth2/token",
        "spark.hadoop.fs.azure.sas.fixed.token.abcde.dfs.core.windows.net": "{{secrets/abcde_access/sasFixedToken}}",
    }
    ws.pipelines.get().spec.configuration = config_dict
    ws.secrets.get_secret = mock_get_secret
    crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx")
    result_set = crawler._list_all_pipeline_with_spn_in_spark_conf()

    assert len(result_set) == 0


def test_list_all_pipeline_with_conf_spn_secret_avlb(mocker):
    sample_pipelines = [
        PipelineInfo(
            creator_name="abcde.defgh@databricks.com",
            pipeline_name="New DLT Pipeline",
            pipeline_id="0112eae7-9d11-4b40-a2b8-6c83cb3c7497",
            success=1,
            failures="",
        )
    ]
    ws = mocker.Mock()
    ws.pipelines.list_pipelines.return_value = sample_pipelines
    config_dict = {
        "spark.hadoop.fs.azure.account.oauth2.client.id.newstorageacct.dfs.core.windows"
        ".net": "{{secrets/reallyreallyasecret/sasFixedToken}}",
        "spark.hadoop.fs.azure.account.oauth2.client."
        "endpoint.newstorageacct.dfs.core.windows.net": "https://"
        "login.microsoftonline.com/directory_12345/oauth2/token",
        "spark.hadoop.fs.azure.sas.fixed.token.abcde.dfs.core.windows.net": "{{secrets/abcde_access/sasFixedToken}}",
    }
    ws.pipelines.get().spec.configuration = config_dict
    ws.secrets.get_secret.return_value = GetSecretResponse(key="username", value=_SECRET_VALUE)
    result_set = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx")._list_all_pipeline_with_spn_in_spark_conf()

    assert len(result_set) > 0
    assert result_set[0].get("application_id") == "Hello, World!"
    assert result_set[0].get("tenant_id") == "directory_12345"
    assert result_set[0].get("storage_account") == "newstorageacct"


def test_azure_spn_info_with_secret_unavailable(mocker):
    ws = mocker.Mock()
    spark_conf = {
        "spark.hadoop.fs.azure.account."
        "oauth2.client.id.abcde.dfs.core.windows.net": "{{secrets/abcff/sp_app_client_id}}",
        "spark.hadoop.fs.azure.account."
        "oauth2.client.endpoint.abcde.dfs.core.windows.net": "https://login.microsoftonline.com/dedededede"
        "/token",
        "spark.hadoop.fs.azure.account."
        "oauth2.client.secret.abcde.dfs.core.windows.net": "{{secrets/abcff/sp_secret}}",
    }
    ws.secrets.get_secret = mock_get_secret
    crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx")._get_azure_spn_list(spark_conf)

    assert crawler == []


def mock_get_secret(secret_scope, secret_key):
    msg = f"Secret Scope {secret_scope} does not exist!"
    raise NotFound(msg)


_SECRET_VALUE = b"SGVsbG8sIFdvcmxkIQ=="
_SECRET_PATTERN = r"{{(secrets.*?)}}"

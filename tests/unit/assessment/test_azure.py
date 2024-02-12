import base64
import json
from unittest.mock import Mock, create_autospec

import pytest
from databricks.labs.blueprint.installation import MockInstallation
from databricks.sdk import WorkspaceClient
from databricks.sdk.oauth import Token

from databricks.labs.ucx.assessment.azure import (
    AzureResource,
    AzureResourcePermissions,
    AzureResources,
    AzureServicePrincipalCrawler,
    Principal,
    generate_service_principals,
)
from databricks.labs.ucx.hive_metastore import ExternalLocations

from ..framework.mocks import MockBackend
from . import get_az_api_mapping, workspace_client_mock


@pytest.fixture
def az_token(mocker):
    token = json.dumps({"aud": "foo", "tid": "bar"}).encode("utf-8")
    str_token = base64.b64encode(token).decode("utf-8").replace("=", "")
    tok = Token(access_token=f"header.{str_token}.sig")
    mocker.patch("databricks.sdk.oauth.Refreshable.token", return_value=tok)


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
    azure_resource_permission = AzureResourcePermissions(
        installation, w, AzureResources(w, include_subscriptions="002"), location
    )
    azure_resource_permission.save_spn_permissions()
    msg = "There are no external table present with azure storage account. Please check if assessment job is run"
    assert [rec.message for rec in caplog.records if msg in rec.message]


def test_save_spn_permissions_no_azure_storage_account():
    w = create_autospec(WorkspaceClient)
    rows = {"SELECT \\* FROM ucx.external_locations": [["s3://bucket1/folder1", "0"]]}
    backend = MockBackend(rows=rows)
    location = ExternalLocations(w, backend, "ucx")
    installation = MockInstallation()
    azure_resource_permission = AzureResourcePermissions(
        installation, w, AzureResources(w, include_subscriptions="002"), location
    )
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
    azure_resource_permission = AzureResourcePermissions(
        installation, w, AzureResources(w, include_subscriptions="002"), location
    )
    storage_accounts = azure_resource_permission._get_storage_accounts()
    assert storage_accounts[0] == "storage1"


def test_map_storage_with_spn_no_blob_permission(mocker, az_token):
    w = create_autospec(WorkspaceClient)
    location = ExternalLocations(w, MockBackend(), "ucx")
    resource_id = "subscriptions/002/resourceGroups/rg1/storageAccounts/sto3"
    installation = MockInstallation()
    azure_resource_permission = AzureResourcePermissions(
        installation, w, AzureResources(w, include_subscriptions="002"), location
    )
    mocker.patch("databricks.sdk.core.ApiClient.do", side_effect=get_az_api_mapping)
    storage_permission_mappings = azure_resource_permission._map_storage(AzureResource(resource_id))
    assert len(storage_permission_mappings) == 0


def test_get_role_assignments_with_spn_and_blob_permission(mocker, az_token):
    w = create_autospec(WorkspaceClient)
    location = ExternalLocations(w, MockBackend(), "ucx")
    resource_id = "subscriptions/002/resourceGroups/rg1/storageAccounts/sto2"
    installation = MockInstallation()
    azure_resource_permission = AzureResourcePermissions(
        installation, w, AzureResources(w, include_subscriptions="002"), location
    )
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
    azure_resource_permission = AzureResourcePermissions(
        installation, w, AzureResources(w, include_subscriptions="002"), location
    )
    azure_resource_permission.save_spn_permissions()
    assert [rec.message for rec in caplog.records if "No storage account found in current tenant" in rec.message]


def test_save_spn_permissions_valid_storage_accounts(caplog, mocker, az_token):
    w = create_autospec(WorkspaceClient)
    rows = {"SELECT \\* FROM ucx.external_locations": [["abfss://continer1@sto2.dfs.core.windows.net/folder1", 1]]}
    mocker.patch("databricks.sdk.core.ApiClient.do", side_effect=get_az_api_mapping)
    backend = MockBackend(rows=rows)
    location = ExternalLocations(w, backend, "ucx")
    installation = MockInstallation()
    azure_resource_permission = AzureResourcePermissions(
        installation, w, AzureResources(w, include_subscriptions="002"), location
    )
    azure_resource_permission.save_spn_permissions()
    installation.assert_file_written(
        'azure_storage_account_info.csv',
        [
            {
                'client_id': 'appIduser3',
                'prefix': 'abfss://container3@sto2.dfs.core.windows.net/',
                'principal': 'disNameuser3',
                'privilege': 'WRITE_FILES',
            },
            {
                'client_id': 'appIduser3',
                'prefix': 'abfss://container3@sto2.dfs.core.windows.net/',
                'principal': 'disNameuser3',
                'privilege': 'WRITE_FILES',
            },
        ],
    )


def test_azure_spn_info_without_secret():
    ws = workspace_client_mock(clusters="single-cluster-spn.json")
    sample_spns = [{"application_id": "test123456789", "secret_scope": "", "secret_key": ""}]
    AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()
    crawler = generate_service_principals(sample_spns)
    result_set = list(crawler)

    assert len(result_set) == 1
    assert result_set[0].application_id == "test123456789"


def test_azure_service_principal_infosnapshot():
    ws = workspace_client_mock(
        clusters="assortment-spn.json",
        pipelines="single-pipeline-with-spn.json",
        jobs="assortment-spn.json",
        warehouse_config="spn-config.json",
        secret_exists=True,
    )
    spn_crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(spn_crawler) == 5


def test_azure_service_principal_info_spark_confsnapshot():
    ws = workspace_client_mock(
        clusters="no-spark-conf.json",
        pipelines="single-pipeline.json",
        jobs="assortment-spn.json",
        warehouse_config="spn-config.json",
    )

    spn_crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(spn_crawler) == 3


def test_azure_service_principal_info_no_spark_confsnapshot():
    ws = workspace_client_mock(
        clusters="no-spark-conf.json",
        pipelines="single-pipeline.json",
        jobs="single-job.json",
        warehouse_config="single-config.json",
    )

    spn_crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(spn_crawler) == 0


def test_azure_service_principal_info_policy_family_conf_crawl(mocker):
    ws = workspace_client_mock()
    spn_crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(spn_crawler) == 0


def test_azure_service_principal_info_null_applidsnapshot():
    ws = workspace_client_mock(
        clusters="single-cluster-spn-with-policy.json", pipelines="single-pipeline.json", jobs="single-job.json"
    )
    spn_crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()
    assert len(spn_crawler) == 0


def test_azure_spn_info_with_secret():
    sample_spns = [{"application_id": "test123456780", "secret_scope": "abcff", "secret_key": "sp_app_client_id"}]
    crawler = generate_service_principals(sample_spns)
    result_set = list(crawler)

    assert len(result_set) == 1
    assert result_set[0].application_id == "test123456780"


def test_spn_with_spark_config_snapshot_try_fetch():
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


def test_spn_with_spark_config_snapshot():
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


def test_list_all_cluster_with_spn_in_spark_conf_with_secret():
    ws = workspace_client_mock(clusters="single-cluster-spn.json")
    result_set = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(result_set) == 1


def test_list_all_wh_config_with_spn_no_secret():
    ws = workspace_client_mock(warehouse_config="spn-config.json")
    result_set = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(result_set) == 2


def test_list_all_wh_config_with_spn_and_secret():
    ws = workspace_client_mock(warehouse_config="spn-secret-config.json", secret_exists=True)
    result_set = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(result_set) == 2


def test_list_all_clusters_spn_in_spark_conf_with_tenant():
    ws = workspace_client_mock(clusters="single-cluster-spn.json", secret_exists=True)
    result_set = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(result_set) == 1
    assert result_set[0].tenant_id == "dummy_tenant_id"


def test_azure_service_principal_info_policy_conf():
    ws = workspace_client_mock(
        clusters="single-cluster-spn.json",
        jobs="single-spn-with-policy.json",
        pipelines="single-pipeline-with-spn.json",
        warehouse_config="spn-config.json",
        secret_exists=True,
    )
    spn_crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(spn_crawler) == 4


def test_azure_service_principal_info_dedupe():
    ws = workspace_client_mock(
        clusters="single-cluster-dupe-spn.json",
        jobs="single-spn-with-policy.json",
        pipelines="single-pipeline-with-spn.json",
        warehouse_config="dupe-spn-config.json",
        secret_exists=True,
    )
    spn_crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(spn_crawler) == 2


def test_list_all_pipeline_with_conf_spn_in_spark_conf():
    ws = workspace_client_mock(pipelines="single-pipeline-with-spn.json")
    result_set = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(result_set) == 1
    assert result_set[0].storage_account == "newstorageacct"
    assert result_set[0].tenant_id == "directory_12345"
    assert result_set[0].application_id == "pipeline_dummy_application_id"


def test_list_all_pipeline_wo_conf_spn_in_spark_conf():
    ws = workspace_client_mock(pipelines="single-pipeline.json")
    result_set = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(result_set) == 0


def test_list_all_pipeline_with_conf_spn_tenant():
    ws = workspace_client_mock(pipelines="single-pipeline-with-spn.json")
    result_set = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(result_set) == 1
    assert result_set[0].storage_account == "newstorageacct"
    assert result_set[0].application_id == "pipeline_dummy_application_id"


def test_list_all_pipeline_with_conf_spn_secret():
    ws = workspace_client_mock(pipelines="single-pipeline-with-spn.json", secret_exists=True)
    result_set = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(result_set) == 1
    assert result_set[0].storage_account == "newstorageacct"


def test_azure_service_principal_info_policy_family():
    ws = workspace_client_mock(clusters="single-cluster-spn-with-policy-overrides.json")
    spn_crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(spn_crawler) == 1
    assert spn_crawler[0].application_id == "dummy_appl_id"
    assert spn_crawler[0].tenant_id == "dummy_tenant_id"


def test_list_all_pipeline_with_conf_spn_secret_unavlbl():
    ws = workspace_client_mock(pipelines="single-pipeline.json", secret_exists=False)
    crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx")
    result_set = crawler.snapshot()

    assert len(result_set) == 0


def test_list_all_pipeline_with_conf_spn_secret_avlb():
    ws = workspace_client_mock(pipelines="single-pipeline-with-spn.json", secret_exists=True)
    result_set = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(result_set) > 0
    assert result_set[0].application_id == "pipeline_dummy_application_id"
    assert result_set[0].tenant_id == "directory_12345"
    assert result_set[0].storage_account == "newstorageacct"


def test_azure_spn_info_with_secret_unavailable():
    ws = workspace_client_mock(secret_exists=False)
    spark_conf = {
        "spark.hadoop.fs.azure.account."
        "oauth2.client.id.abcde.dfs.core.windows.net": "{{secrets/abcff/sp_app_client_id}}",
        "spark.hadoop.fs.azure.account."
        "oauth2.client.endpoint.abcde.dfs.core.windows.net": "https://login.microsoftonline.com/dedededede"
        "/token",
        "spark.hadoop.fs.azure.account."
        "oauth2.client.secret.abcde.dfs.core.windows.net": "{{secrets/abcff/sp_secret}}",
    }
    crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx")._get_azure_spn_from_config(spark_conf)

    assert crawler == []

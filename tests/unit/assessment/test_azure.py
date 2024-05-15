from unittest.mock import create_autospec

from databricks.labs.lsql.backends import MockBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import (
    ClusterDetails,
    ClusterSource,
    DataSecurityMode,
)

from databricks.labs.ucx.assessment.azure import (
    AzureServicePrincipalCrawler,
    AzureServicePrincipalInfo,
)

from .. import mock_workspace_client


def test_azure_service_principal_info_crawl():
    ws = mock_workspace_client(
        cluster_ids=['azure-spn-secret', 'simplest-autoscale'],
        pipeline_ids=['spec-with-spn'],
        job_ids=['some-spn'],
        warehouse_config="spn-config",
        secret_exists=True,
    )
    spn_crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(spn_crawler) == 5


def test_azure_service_principal_info_spark_conf_crawl():
    ws = mock_workspace_client(
        cluster_ids=['simplest-autoscale'],
        pipeline_ids=['empty-spec'],
        job_ids=['some-spn'],
        warehouse_config="spn-config",
    )

    spn_crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(spn_crawler) == 3


def test_azure_service_principal_info_no_spark_conf_crawl():
    ws = mock_workspace_client(
        cluster_ids=['simplest-autoscale'],
        pipeline_ids=['empty-spec'],
        job_ids=['single-job'],
        warehouse_config="single-config",
    )

    spn_crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(spn_crawler) == 0


def test_azure_service_principal_info_policy_family_conf_crawl():
    ws = mock_workspace_client(cluster_ids=['simplest-autoscale'], pipeline_ids=['empty-spec'])
    spn_crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(spn_crawler) == 0


def test_azure_service_principal_info_null_applid_crawl():
    ws = mock_workspace_client(
        cluster_ids=['policy-single-user-with-empty-appid-spn'],
        pipeline_ids=['empty-spec'],
        job_ids=['single-job'],
    )
    spn_crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()
    assert len(spn_crawler) == 0


def test_list_all_cluster_with_spn_in_spark_conf_with_secret():
    ws = mock_workspace_client(cluster_ids=['azure-spn-secret'], pipeline_ids=['empty-spec'])
    result_set = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(result_set) == 1


def test_list_all_wh_config_with_spn_no_secret():
    ws = mock_workspace_client(
        cluster_ids=['simplest-autoscale'], pipeline_ids=['empty-spec'], warehouse_config="spn-config"
    )
    result_set = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(result_set) == 2
    assert any(_ for _ in result_set if _.application_id == "dummy_application_id")
    assert any(_ for _ in result_set if _.tenant_id == "dummy_tenant_id")
    assert any(_ for _ in result_set if _.storage_account == "storage_acct2")


def test_list_all_wh_config_with_spn_and_secret():
    ws = mock_workspace_client(
        cluster_ids=['simplest-autoscale'],
        pipeline_ids=['empty-spec'],
        warehouse_config="spn-secret-config",
        secret_exists=True,
    )
    result_set = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(result_set) == 2
    assert any(_ for _ in result_set if _.tenant_id == "dummy_tenant_id")
    assert any(_ for _ in result_set if _.storage_account == "abcde")


def test_list_all_clusters_spn_in_spark_conf_with_tenant():
    ws = mock_workspace_client(cluster_ids=['azure-spn-secret'], pipeline_ids=['empty-spec'], secret_exists=True)
    result_set = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(result_set) == 1
    assert result_set[0].tenant_id == "dedededede"


def test_azure_service_principal_info_policy_conf():
    ws = mock_workspace_client(
        cluster_ids=['policy-single-user-with-spn', 'policy-azure-oauth'],
        pipeline_ids=['spec-with-spn'],
        job_ids=['policy-single-job-with-spn'],
        warehouse_config="spn-config",
        secret_exists=True,
    )
    spn_crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(spn_crawler) == 4


def test_azure_service_principal_info_dedupe():
    ws = mock_workspace_client(
        cluster_ids=['policy-single-user-with-spn'],
        pipeline_ids=['spec-with-spn'],
        job_ids=['policy-single-job-with-spn'],
        warehouse_config="dupe-spn-config",
        secret_exists=True,
    )
    spn_crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(spn_crawler) == 2


def test_list_all_pipeline_with_conf_spn_in_spark_conf():
    ws = mock_workspace_client(cluster_ids=['simplest-autoscale'], pipeline_ids=['spec-with-spn'])
    result_set = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(result_set) == 1
    assert result_set[0].storage_account == "newstorageacct"
    assert result_set[0].tenant_id == "directory_12345"
    assert result_set[0].application_id == "pipeline_dummy_application_id"


def test_list_all_pipeline_wo_conf_spn_in_spark_conf():
    ws = mock_workspace_client(cluster_ids=['simplest-autoscale'], pipeline_ids=['empty-spec'])
    result_set = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(result_set) == 0


def test_list_all_pipeline_with_conf_spn_tenant():
    ws = mock_workspace_client(cluster_ids=['simplest-autoscale'], pipeline_ids=['spec-with-spn'])
    result_set = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(result_set) == 1
    assert result_set[0].storage_account == "newstorageacct"
    assert result_set[0].application_id == "pipeline_dummy_application_id"


def test_list_all_pipeline_with_conf_spn_secret():
    ws = mock_workspace_client(cluster_ids=['simplest-autoscale'], pipeline_ids=['spec-with-spn'], secret_exists=True)
    result_set = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(result_set) == 1
    assert result_set[0].storage_account == "newstorageacct"


def test_azure_service_principal_info_policy_family():
    ws = mock_workspace_client(cluster_ids=['policy-spn-in-policy-overrides'], pipeline_ids=['empty-spec'])
    spn_crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(spn_crawler) == 1
    assert spn_crawler[0].application_id == "dummy_appl_id"
    assert spn_crawler[0].tenant_id == "dummy_tenant_id"


def test_list_all_pipeline_with_conf_spn_secret_unavlbl():
    ws = mock_workspace_client(cluster_ids=['simplest-autoscale'], pipeline_ids=['empty-spec'], secret_exists=False)
    crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx")
    result_set = crawler.snapshot()

    assert len(result_set) == 0


def test_list_all_pipeline_with_conf_spn_secret_avlb():
    ws = mock_workspace_client(cluster_ids=['simplest-autoscale'], pipeline_ids=['spec-with-spn'], secret_exists=True)
    result_set = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(result_set) > 0
    assert result_set[0].application_id == "pipeline_dummy_application_id"
    assert result_set[0].tenant_id == "directory_12345"
    assert result_set[0].storage_account == "newstorageacct"


def test_azure_spn_info_with_secret_unavailable():
    ws = mock_workspace_client(cluster_ids=['azure-spn-secret'], secret_exists=False)
    result = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()
    assert len(result) == 0


def test_jobs_assessment_with_spn_cluster_policy_not_found():
    ws = mock_workspace_client(job_ids=['policy-not-found'])
    crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()
    assert len(crawler) == 1


def test_get_cluster_to_storage_mapping_no_cluster_return_empty():
    ws = create_autospec(WorkspaceClient)
    ws.clusters.list.return_value = []
    crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx")
    assert not crawler.get_cluster_to_storage_mapping()


def test_get_cluster_to_storage_mapping_no_interactive_cluster_return_empty():
    ws = mock_workspace_client(cluster_ids=['azure-spn-secret'])
    ws.clusters.list.return_value = [
        ClusterDetails(cluster_source=ClusterSource.JOB),
        ClusterDetails(cluster_source=ClusterSource.UI, data_security_mode=DataSecurityMode.SINGLE_USER),
    ]
    crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx")
    assert not crawler.get_cluster_to_storage_mapping()


def test_get_cluster_to_storage_mapping_interactive_cluster_no_spn_return_empty():
    ws = mock_workspace_client(cluster_ids=['azure-spn-secret-interactive-multiple-spn'])

    crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx")
    cluster_spn_info = crawler.get_cluster_to_storage_mapping()
    spn_info = {
        AzureServicePrincipalInfo(
            application_id='Hello, World!',
            secret_scope='abcff',
            secret_key='sp_secret',
            tenant_id='dedededede',
            storage_account='abcde',
        ),
        AzureServicePrincipalInfo(
            application_id='Hello, World!',
            secret_scope='fgh',
            secret_key='sp_secret2',
            tenant_id='dedededede',
            storage_account='fgh',
        ),
    }

    assert cluster_spn_info[0].cluster_id == "azure-spn-secret-interactive"
    assert len(cluster_spn_info[0].spn_info) == 2
    assert cluster_spn_info[0].spn_info == spn_info

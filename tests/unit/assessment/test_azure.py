from unittest.mock import Mock

from databricks.labs.ucx.assessment.azure import (
    AzureServicePrincipalCrawler,
    generate_service_principals,
)

from ..framework.mocks import MockBackend
from . import workspace_client_mock


def test_azure_spn_info_without_secret():
    ws = workspace_client_mock(clusters="single-cluster-spn.json")
    sample_spns = [{"application_id": "test123456789", "secret_scope": "", "secret_key": ""}]
    AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()
    crawler = generate_service_principals(sample_spns)
    result_set = list(crawler)

    assert len(result_set) == 1
    assert result_set[0].application_id == "test123456789"


def test_azure_service_principal_info_crawl():
    ws = workspace_client_mock(
        clusters="assortment-spn.json",
        pipelines="single-pipeline-with-spn.json",
        jobs="assortment-spn.json",
        warehouse_config="spn-config.json",
        secret_exists=True,
    )
    spn_crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(spn_crawler) == 5


def test_azure_service_principal_info_spark_conf_crawl():
    ws = workspace_client_mock(
        clusters="no-spark-conf.json",
        pipelines="single-pipeline.json",
        jobs="assortment-spn.json",
        warehouse_config="spn-config.json",
    )

    spn_crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(spn_crawler) == 3


def test_azure_service_principal_info_no_spark_conf_crawl():
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


def test_azure_service_principal_info_null_applid_crawl():
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
    assert any(_ for _ in result_set if _.application_id == "dummy_application_id")
    assert any(_ for _ in result_set if _.tenant_id == "dummy_tenant_id")
    assert any(_ for _ in result_set if _.storage_account == "storage_acct2")


def test_list_all_wh_config_with_spn_and_secret():
    ws = workspace_client_mock(warehouse_config="spn-secret-config.json", secret_exists=True)
    result_set = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(result_set) == 2
    assert any(_ for _ in result_set if _.tenant_id == "dummy_tenant_id")
    assert any(_ for _ in result_set if _.storage_account == "abcde")


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

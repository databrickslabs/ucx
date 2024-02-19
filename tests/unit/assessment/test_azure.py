from databricks.labs.ucx.assessment.azure import AzureServicePrincipalCrawler

from ..framework.mocks import MockBackend
from . import workspace_client_mock


def test_azure_service_principal_info_crawl():
    ws = workspace_client_mock(
        cluster_ids=['azure-spn-secret', 'simplest-autoscale'],
        pipeline_ids=['spec-with-spn'],
        job_ids=['some-spn'],
        warehouse_config="spn-config.json",
        secret_exists=True,
    )
    spn_crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(spn_crawler) == 5


def test_azure_service_principal_info_spark_conf_crawl():
    ws = workspace_client_mock(
        cluster_ids=['simplest-autoscale'],
        pipeline_ids=['empty-spec'],
        job_ids=['some-spn'],
        warehouse_config="spn-config.json",
    )

    spn_crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(spn_crawler) == 3


def test_azure_service_principal_info_no_spark_conf_crawl():
    ws = workspace_client_mock(
        cluster_ids=['simplest-autoscale'],
        pipeline_ids=['empty-spec'],
        job_ids=['single-job'],
        warehouse_config="single-config.json",
    )

    spn_crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(spn_crawler) == 0


def test_azure_service_principal_info_policy_family_conf_crawl():
    ws = workspace_client_mock(cluster_ids=['simplest-autoscale'], pipeline_ids=['empty-spec'])
    spn_crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(spn_crawler) == 0


def test_azure_service_principal_info_null_applid_crawl():
    ws = workspace_client_mock(
        cluster_ids=['policy-single-user-with-empty-appid-spn'],
        pipeline_ids=['empty-spec'],
        job_ids=['single-job'],
    )
    spn_crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()
    assert len(spn_crawler) == 0


def test_list_all_cluster_with_spn_in_spark_conf_with_secret():
    ws = workspace_client_mock(cluster_ids=['azure-spn-secret'], pipeline_ids=['empty-spec'])
    result_set = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(result_set) == 1


def test_list_all_wh_config_with_spn_no_secret():
    ws = workspace_client_mock(
        cluster_ids=['simplest-autoscale'], pipeline_ids=['empty-spec'], warehouse_config="spn-config.json"
    )
    result_set = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(result_set) == 2
    assert any(_ for _ in result_set if _.application_id == "dummy_application_id")
    assert any(_ for _ in result_set if _.tenant_id == "dummy_tenant_id")
    assert any(_ for _ in result_set if _.storage_account == "storage_acct2")


def test_list_all_wh_config_with_spn_and_secret():
    ws = workspace_client_mock(
        cluster_ids=['simplest-autoscale'],
        pipeline_ids=['empty-spec'],
        warehouse_config="spn-secret-config.json",
        secret_exists=True,
    )
    result_set = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(result_set) == 2
    assert any(_ for _ in result_set if _.tenant_id == "dummy_tenant_id")
    assert any(_ for _ in result_set if _.storage_account == "abcde")


def test_list_all_clusters_spn_in_spark_conf_with_tenant():
    ws = workspace_client_mock(cluster_ids=['azure-spn-secret'], pipeline_ids=['empty-spec'], secret_exists=True)
    result_set = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(result_set) == 1
    assert result_set[0].tenant_id == "dedededede"


def test_azure_service_principal_info_policy_conf():
    ws = workspace_client_mock(
        cluster_ids=['policy-single-user-with-spn', 'policy-azure-oauth'],
        pipeline_ids=['spec-with-spn'],
        job_ids=['policy-single-job-with-spn'],
        warehouse_config="spn-config.json",
        secret_exists=True,
    )
    spn_crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(spn_crawler) == 4


def test_azure_service_principal_info_dedupe():
    ws = workspace_client_mock(
        cluster_ids=['policy-single-user-with-spn'],
        pipeline_ids=['spec-with-spn'],
        job_ids=['policy-single-job-with-spn'],
        warehouse_config="dupe-spn-config.json",
        secret_exists=True,
    )
    spn_crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(spn_crawler) == 2


def test_list_all_pipeline_with_conf_spn_in_spark_conf():
    ws = workspace_client_mock(cluster_ids=['simplest-autoscale'], pipeline_ids=['spec-with-spn'])
    result_set = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(result_set) == 1
    assert result_set[0].storage_account == "newstorageacct"
    assert result_set[0].tenant_id == "directory_12345"
    assert result_set[0].application_id == "pipeline_dummy_application_id"


def test_list_all_pipeline_wo_conf_spn_in_spark_conf():
    ws = workspace_client_mock(cluster_ids=['simplest-autoscale'], pipeline_ids=['empty-spec'])
    result_set = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(result_set) == 0


def test_list_all_pipeline_with_conf_spn_tenant():
    ws = workspace_client_mock(cluster_ids=['simplest-autoscale'], pipeline_ids=['spec-with-spn'])
    result_set = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(result_set) == 1
    assert result_set[0].storage_account == "newstorageacct"
    assert result_set[0].application_id == "pipeline_dummy_application_id"


def test_list_all_pipeline_with_conf_spn_secret():
    ws = workspace_client_mock(cluster_ids=['simplest-autoscale'], pipeline_ids=['spec-with-spn'], secret_exists=True)
    result_set = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(result_set) == 1
    assert result_set[0].storage_account == "newstorageacct"


def test_azure_service_principal_info_policy_family():
    ws = workspace_client_mock(cluster_ids=['policy-spn-in-policy-overrides'], pipeline_ids=['empty-spec'])
    spn_crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(spn_crawler) == 1
    assert spn_crawler[0].application_id == "dummy_appl_id"
    assert spn_crawler[0].tenant_id == "dummy_tenant_id"


def test_list_all_pipeline_with_conf_spn_secret_unavlbl():
    ws = workspace_client_mock(cluster_ids=['simplest-autoscale'], pipeline_ids=['empty-spec'], secret_exists=False)
    crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx")
    result_set = crawler.snapshot()

    assert len(result_set) == 0


def test_list_all_pipeline_with_conf_spn_secret_avlb():
    ws = workspace_client_mock(cluster_ids=['simplest-autoscale'], pipeline_ids=['spec-with-spn'], secret_exists=True)
    result_set = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()

    assert len(result_set) > 0
    assert result_set[0].application_id == "pipeline_dummy_application_id"
    assert result_set[0].tenant_id == "directory_12345"
    assert result_set[0].storage_account == "newstorageacct"


def test_azure_spn_info_with_secret_unavailable():
    ws = workspace_client_mock(cluster_ids=['simplest-autoscale'], pipeline_ids=['empty-spec'], secret_exists=False)
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

    assert crawler == set()


def test_jobs_assessment_with_spn_cluster_policy_not_found():
    ws = workspace_client_mock(job_ids=['policy-not-found'])
    crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()
    assert len(crawler) == 1

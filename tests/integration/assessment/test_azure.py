from datetime import timedelta

from databricks.sdk.errors import NotFound

from databricks.labs.ucx.assessment.azure import AzureServicePrincipalCrawler

from ..retries import retried
from .test_assessment import (
    _PIPELINE_CONF,
    _PIPELINE_CONF_WITH_SECRET,
    _SPARK_CONF,
    _TEST_STORAGE_ACCOUNT,
    _TEST_TENANT_ID,
)


@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_spn_crawler(ws, inventory_schema, make_job, make_pipeline, sql_backend):
    make_job(spark_conf=_SPARK_CONF)
    make_pipeline(configuration=_PIPELINE_CONF)
    spn_crawler = AzureServicePrincipalCrawler(ws, sql_backend, inventory_schema)
    results = spn_crawler.snapshot()

    assert any(_ for _ in results if _.tenant_id == _TEST_TENANT_ID)
    assert any(_ for _ in results if _.storage_account == _TEST_STORAGE_ACCOUNT)


@retried(on=[NotFound], timeout=timedelta(minutes=5))
def test_spn_crawler_no_config(ws, inventory_schema, make_job, make_pipeline, sql_backend, make_cluster):
    make_job()
    make_pipeline()
    make_cluster(single_node=True)
    spn_crawler = AzureServicePrincipalCrawler(ws, sql_backend, inventory_schema)
    spn_crawler.snapshot()


@retried(on=[NotFound], timeout=timedelta(minutes=5))
def test_spn_crawler_deleted_cluster_policy(
    ws,
    inventory_schema,
    sql_backend,
    make_job,
    make_cluster,
    make_cluster_policy,
    make_random,
    make_notebook,
):
    cluster_policy_id = make_cluster_policy().policy_id
    make_cluster(single_node=True, spark_conf=_SPARK_CONF, policy_id=cluster_policy_id)
    ws.cluster_policies.delete(policy_id=cluster_policy_id)
    spn_crawler = AzureServicePrincipalCrawler(ws, sql_backend, inventory_schema)
    results = spn_crawler.snapshot()

    assert any(_ for _ in results if _.tenant_id == _TEST_TENANT_ID)
    assert any(_ for _ in results if _.storage_account == _TEST_STORAGE_ACCOUNT)


@retried(on=[NotFound], timeout=timedelta(minutes=5))
def test_spn_crawler_with_pipeline_unavailable_secret(ws, inventory_schema, make_job, make_pipeline, sql_backend):
    make_job(spark_conf=_SPARK_CONF)
    make_pipeline(configuration=_PIPELINE_CONF_WITH_SECRET)
    spn_crawler = AzureServicePrincipalCrawler(ws, sql_backend, inventory_schema)
    results = spn_crawler.snapshot()

    assert any(_ for _ in results if _.tenant_id == _TEST_TENANT_ID)
    assert any(_ for _ in results if _.storage_account == _TEST_STORAGE_ACCOUNT)


@retried(on=[NotFound], timeout=timedelta(minutes=5))
def test_spn_crawler_with_available_secrets(
    ws, inventory_schema, make_job, make_pipeline, sql_backend, make_secret_scope
):
    secret_scope = make_secret_scope()
    client_id_secret_key = "spn_client_id"
    client_secret_secret_key = "spn_client_secret"
    tenant_id_secret_key = "spn_tenant_id"
    ws.secrets.put_secret(secret_scope, client_id_secret_key, string_value="New_Application_Id")
    ws.secrets.put_secret(secret_scope, client_secret_secret_key, string_value="secret")
    ws.secrets.put_secret(
        secret_scope,
        tenant_id_secret_key,
        string_value=f"https://login.microsoftonline.com/{_TEST_TENANT_ID}/oauth2/token",
    )
    conf_secret_available = {
        "fs.azure.account.oauth2.client.id.SA1.dfs.core.windows.net": f"{{{{secrets/{secret_scope}/{client_id_secret_key}}}}}",
        "fs.azure.account.oauth2.client.secret.SA1.dfs.core.windows.net": f"{{{{secrets/{secret_scope}/{client_secret_secret_key}}}}}",
        "fs.azure.account.oauth2.client.endpoint.SA1.dfs.core.windows.net": f"{{{{secrets/{secret_scope}/{tenant_id_secret_key}}}}}",
    }
    make_job()
    make_pipeline(configuration=conf_secret_available)
    spn_crawler = AzureServicePrincipalCrawler(ws, sql_backend, inventory_schema)
    results = spn_crawler.snapshot()

    assert any(_ for _ in results if _.secret_scope == secret_scope)
    assert any(_ for _ in results if _.secret_key == client_secret_secret_key)
    assert any(_ for _ in results if _.tenant_id == _TEST_TENANT_ID)
    assert any(_ for _ in results if _.application_id == "New_Application_Id")
    assert any(_ for _ in results if _.storage_account == "SA1")

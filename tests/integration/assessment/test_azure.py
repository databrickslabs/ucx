import logging
from datetime import timedelta

import pytest
from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service import compute, jobs

from databricks.labs.ucx.assessment.azure import (
    AzureResourcePermissions,
    AzureResources,
    AzureServicePrincipalCrawler,
    StoragePermissionMapping,
)
from databricks.labs.ucx.hive_metastore.locations import (
    ExternalLocation,
    ExternalLocations,
)

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
    spn_crawler = AzureServicePrincipalCrawler(ws=ws, sbe=sql_backend, schema=inventory_schema)
    spns = spn_crawler.snapshot()
    results = []
    for spn in spns:
        results.append(spn)

    assert any(_ for _ in results if _.tenant_id == _TEST_TENANT_ID)
    assert any(_ for _ in results if _.storage_account == _TEST_STORAGE_ACCOUNT)


@retried(on=[NotFound], timeout=timedelta(minutes=5))
def test_spn_crawler_no_config(ws, inventory_schema, make_job, make_pipeline, sql_backend, make_cluster):
    make_job()
    make_pipeline()
    make_cluster(single_node=True)
    spn_crawler = AzureServicePrincipalCrawler(ws=ws, sbe=sql_backend, schema=inventory_schema)
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
    make_job(
        name=f"sdk-{make_random(4)}",
        tasks=[
            jobs.Task(
                task_key=make_random(4),
                description=make_random(4),
                new_cluster=compute.ClusterSpec(
                    num_workers=1,
                    node_type_id=ws.clusters.select_node_type(local_disk=True),
                    spark_version=ws.clusters.select_spark_version(latest=True),
                    spark_conf=_SPARK_CONF,
                    policy_id=cluster_policy_id,
                ),
                notebook_task=jobs.NotebookTask(notebook_path=make_notebook()),
                timeout_seconds=0,
            )
        ],
    )
    make_cluster(single_node=True, spark_conf=_SPARK_CONF, policy_id=cluster_policy_id)
    ws.cluster_policies.delete(policy_id=cluster_policy_id)
    spn_crawler = AzureServicePrincipalCrawler(ws=ws, sbe=sql_backend, schema=inventory_schema)
    results = spn_crawler.snapshot()

    assert any(_ for _ in results if _.tenant_id == _TEST_TENANT_ID)
    assert any(_ for _ in results if _.storage_account == _TEST_STORAGE_ACCOUNT)


@retried(on=[NotFound], timeout=timedelta(minutes=5))
def test_spn_crawler_with_pipeline_unavailable_secret(ws, inventory_schema, make_job, make_pipeline, sql_backend):
    make_job(spark_conf=_SPARK_CONF)
    make_pipeline(configuration=_PIPELINE_CONF_WITH_SECRET)
    spn_crawler = AzureServicePrincipalCrawler(ws=ws, sbe=sql_backend, schema=inventory_schema)
    results = spn_crawler.snapshot()

    assert any(_ for _ in results if _.tenant_id == _TEST_TENANT_ID)
    assert any(_ for _ in results if _.storage_account == _TEST_STORAGE_ACCOUNT)


@retried(on=[NotFound], timeout=timedelta(minutes=5))
def test_spn_crawler_with_available_secrets(
    ws, inventory_schema, make_job, make_pipeline, sql_backend, make_secret_scope
):
    secret_scope = make_secret_scope()
    secret_key = "spn_client_id"
    ws.secrets.put_secret(scope=secret_scope, key=secret_key, string_value="New_Application_Id")
    _pipeline_conf_with_avlbl_secret = {}
    _pipeline_conf_with_avlbl_secret["fs.azure.account.oauth2.client.id.SA1.dfs.core.windows.net"] = (
        "{" + (f"{{secrets/{secret_scope}/{secret_key}}}") + "}"
    )
    _pipeline_conf_with_avlbl_secret[
        "fs.azure.account.oauth2.client.endpoint.SA1.dfs.core.windows.net"
    ] = "https://login.microsoftonline.com/dummy_tenant/oauth2/token"
    make_job()
    make_pipeline(configuration=_pipeline_conf_with_avlbl_secret)
    spn_crawler = AzureServicePrincipalCrawler(ws=ws, sbe=sql_backend, schema=inventory_schema)
    results = spn_crawler.snapshot()

    assert any(_ for _ in results if _.secret_scope == secret_scope)
    assert any(_ for _ in results if _.secret_key == secret_key)


@pytest.mark.skip
def test_azure_storage_accounts(ws, sql_backend, inventory_schema):
    logger = logging.getLogger(__name__)
    logger.setLevel("DEBUG")
    tables = [
        ExternalLocation("abfss://things@labsazurethings.dfs.core.windows.net/folder1", 1),
    ]
    sql_backend.save_table(f"{inventory_schema}.external_locations", tables, ExternalLocation)
    location = ExternalLocations(ws, sql_backend, inventory_schema)
    az_res_perm = AzureResourcePermissions(ws, location, sql_backend, inventory_schema)
    accounts = list(az_res_perm._get_current_tenant_storage_accounts())
    assert len(accounts) == 1
    for acct in accounts:
        assert acct.name == "labsazurethings"


@pytest.mark.skip
def test_save_spn_permissions(ws, sql_backend, inventory_schema):
    logger = logging.getLogger(__name__)
    logger.setLevel("DEBUG")
    tables = [
        ExternalLocation("abfss://things@labsazurethings.dfs.core.windows.net/folder1", 1),
    ]
    sql_backend.save_table(f"{inventory_schema}.external_locations", tables, ExternalLocation)
    location = ExternalLocations(ws, sql_backend, inventory_schema)
    az_res_perm = AzureResourcePermissions(ws, location, sql_backend, inventory_schema)
    az_res_perm.save_spn_permissions()
    sql_query = (
        f"SELECT storage_acct_name, spn_client_id, role_name from hive_metastore.{inventory_schema}"
        f".azure_storage_accounts"
    )
    results = sql_backend.fetch(sql_query)
    for r in results:
        m = StoragePermissionMapping(*r)
        assert m.storage_acct_name == "labsazurethings"


@pytest.mark.skip
def test_save_spn_permissions_local(ws, sql_backend, inventory_schema):
    tables = [
        ExternalLocation("abfss://contname@storagename.dfs.core.windows.net/folder1", 1),
    ]
    sql_backend.save_table(f"{inventory_schema}.external_locations", tables, ExternalLocation)
    location = ExternalLocations(ws, sql_backend, inventory_schema)
    az_res_perm = AzureResourcePermissions(ws, AzureResources(ws, include_subscriptions=""), location)
    path = az_res_perm.save_spn_permissions()
    assert ws.workspace.get_status(path)

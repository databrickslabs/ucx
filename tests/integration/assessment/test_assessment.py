import logging

from databricks.sdk.service import compute, jobs

from databricks.labs.ucx.assessment.crawlers import (
    AzureServicePrincipalCrawler,
    ClustersCrawler,
    JobsCrawler,
    PipelinesCrawler,
)
from databricks.labs.ucx.workspace_access.generic import WorkspaceListing

_TEST_STORAGE_ACCOUNT = "storage_acct_1"

_TEST_TENANT_ID = "directory_12345"

logger = logging.getLogger(__name__)

_PIPELINE_CONF = {
    f"spark.hadoop.fs.azure.account.oauth2.client.id.{_TEST_STORAGE_ACCOUNT}.dfs.core.windows.net": ""
    "pipeline_dummy_application_id",
    f"spark.hadoop.fs.azure.account.oauth2.client.endpoint.{_TEST_STORAGE_ACCOUNT}.dfs.core.windows.net": ""
    "https://login"
    f".microsoftonline.com/{_TEST_TENANT_ID}/oauth2/token",
}

_PIPELINE_CONF_WITH_SECRET = {
    "fs.azure.account.oauth2.client.id.abcde.dfs.core.windows.net": "{{secrets/reallyasecret123/sp_app_client_id}}",
    "fs.azure.account.oauth2.client.endpoint.abcde.dfs.core.windows.net": "https://login.microsoftonline.com"
    "/dummy_application/token",
}

_SPARK_CONF = {
    "spark.databricks.cluster.profile": "singleNode",
    "spark.master": "local[*]",
    f"fs.azure.account.auth.type.{_TEST_STORAGE_ACCOUNT}.dfs.core.windows.net": "OAuth",
    f"fs.azure.account.oauth.provider.type.{_TEST_STORAGE_ACCOUNT}.dfs.core.windows.net": "org.apache.hadoop.fs"
    ".azurebfs.oauth2.ClientCredsTokenProvider",
    f"fs.azure.account.oauth2.client.id.{_TEST_STORAGE_ACCOUNT}.dfs.core.windows.net": "dummy_application_id",
    f"fs.azure.account.oauth2.client.secret.{_TEST_STORAGE_ACCOUNT}.dfs.core.windows.net": "dummy",
    f"fs.azure.account.oauth2.client.endpoint.{_TEST_STORAGE_ACCOUNT}.dfs.core.windows.net": "https://login"
    f".microsoftonline.com/{_TEST_TENANT_ID}/oauth2/token",
}


def test_pipeline_crawler(ws, make_pipeline, inventory_schema, sql_backend):
    logger.info("setting up fixtures")
    created_pipeline = make_pipeline(configuration=_PIPELINE_CONF)

    pipeline_crawler = PipelinesCrawler(ws=ws, sbe=sql_backend, schema=inventory_schema)
    pipelines = pipeline_crawler.snapshot()
    results = []
    for pipeline in pipelines:
        if pipeline.success != 0:
            continue
        if pipeline.pipeline_id == created_pipeline.pipeline_id:
            results.append(pipeline)

    assert len(results) >= 1
    assert results[0].pipeline_id == created_pipeline.pipeline_id


def test_pipeline_with_secret_conf_crawler(ws, make_pipeline, inventory_schema, sql_backend):
    logger.info("setting up fixtures")
    created_pipeline = make_pipeline(configuration=_PIPELINE_CONF_WITH_SECRET)

    pipeline_crawler = PipelinesCrawler(ws=ws, sbe=sql_backend, schema=inventory_schema)
    pipelines = pipeline_crawler.snapshot()
    results = []
    for pipeline in pipelines:
        if pipeline.success != 0:
            continue
        if pipeline.pipeline_id == created_pipeline.pipeline_id:
            results.append(pipeline)

    assert len(results) >= 1
    assert results[0].pipeline_id == created_pipeline.pipeline_id


def test_cluster_crawler(ws, make_cluster, inventory_schema, sql_backend):
    created_cluster = make_cluster(single_node=True, spark_conf=_SPARK_CONF)
    cluster_crawler = ClustersCrawler(ws=ws, sbe=sql_backend, schema=inventory_schema)
    clusters = cluster_crawler.snapshot()
    results = []
    for cluster in clusters:
        if cluster.success != 0:
            continue
        if cluster.cluster_id == created_cluster.cluster_id:
            results.append(cluster)

    assert len(results) >= 1
    assert results[0].cluster_id == created_cluster.cluster_id


def test_job_crawler(ws, make_job, inventory_schema, sql_backend):
    new_job = make_job(spark_conf=_SPARK_CONF)
    job_crawler = JobsCrawler(ws=ws, sbe=sql_backend, schema=inventory_schema)
    jobs = job_crawler.snapshot()
    results = []
    for job in jobs:
        if job.success != 0:
            continue
        if int(job.job_id) == new_job.job_id:
            results.append(job)

    assert len(results) >= 1
    assert int(results[0].job_id) == new_job.job_id


def test_spn_crawler(ws, inventory_schema, make_job, make_pipeline, sql_backend):
    make_job(spark_conf=_SPARK_CONF)
    make_pipeline(configuration=_PIPELINE_CONF)
    spn_crawler = AzureServicePrincipalCrawler(ws=ws, sbe=sql_backend, schema=inventory_schema)
    spns = spn_crawler.snapshot()
    results = []
    for spn in spns:
        results.append(spn)

    assert len(results) >= 2
    assert results[0].storage_account == _TEST_STORAGE_ACCOUNT
    assert results[0].tenant_id == _TEST_TENANT_ID


def test_spn_crawler_no_config(ws, inventory_schema, make_job, make_pipeline, sql_backend, make_cluster):
    make_job()
    make_pipeline()
    make_cluster(single_node=True)
    spn_crawler = AzureServicePrincipalCrawler(ws=ws, sbe=sql_backend, schema=inventory_schema)
    spn_crawler.snapshot()


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


def test_spn_crawler_with_pipeline_unavailable_secret(ws, inventory_schema, make_job, make_pipeline, sql_backend):
    make_job(spark_conf=_SPARK_CONF)
    make_pipeline(configuration=_PIPELINE_CONF_WITH_SECRET)
    spn_crawler = AzureServicePrincipalCrawler(ws=ws, sbe=sql_backend, schema=inventory_schema)
    results = spn_crawler.snapshot()

    assert any(_ for _ in results if _.tenant_id == _TEST_TENANT_ID)
    assert any(_ for _ in results if _.storage_account == _TEST_STORAGE_ACCOUNT)


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


def test_workspace_object_crawler(ws, make_notebook, inventory_schema, sql_backend):
    notebook = make_notebook()
    workspace_listing = WorkspaceListing(ws, sql_backend, inventory_schema)
    workspace_objects = {_.path: _ for _ in workspace_listing.snapshot()}

    assert notebook in workspace_objects
    assert "NOTEBOOK" == workspace_objects[notebook].object_type

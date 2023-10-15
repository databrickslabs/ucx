import logging

from databricks.labs.ucx.assessment.aws_instance_profiles import AWSInstanceProfileCrawler
from databricks.labs.ucx.assessment.crawlers import (
    AzureServicePrincipalCrawler,
    ClustersCrawler,
    JobsCrawler,
    PipelinesCrawler,
)
from databricks.labs.ucx.workspace_access.generic import WorkspaceListing

logger = logging.getLogger(__name__)

_PIPELINE_CONF = {
    "spark.hadoop.fs.azure.account.oauth2.client.id.storage_acct_1.dfs.core.windows.net": ""
    "pipeline_dummy_application_id",
    "spark.hadoop.fs.azure.account.oauth2.client.endpoint.storage_acct_1.dfs.core.windows.net": ""
    "https://login"
    ".microsoftonline.com/directory_12345/oauth2/token",
}

_PIPELINE_CONF_WITH_SECRET = {
    "fs.azure.account.oauth2.client.id.abcde.dfs.core.windows.net": "{{secrets/reallyasecret123/sp_app_client_id}}",
    "fs.azure.account.oauth2.client.endpoint.abcde.dfs.core.windows.net": "https://login.microsoftonline.com"
    "/dummy_application/token",
}

_SPARK_CONF = {
    "spark.databricks.cluster.profile": "singleNode",
    "spark.master": "local[*]",
    "fs.azure.account.auth.type.storage_acct_1.dfs.core.windows.net": "OAuth",
    "fs.azure.account.oauth.provider.type.storage_acct_1.dfs.core.windows.net": "org.apache.hadoop.fs"
    ".azurebfs.oauth2.ClientCredsTokenProvider",
    "fs.azure.account.oauth2.client.id.storage_acct_1.dfs.core.windows.net": "dummy_application_id",
    "fs.azure.account.oauth2.client.secret.storage_acct_1.dfs.core.windows.net": "dummy",
    "fs.azure.account.oauth2.client.endpoint.storage_acct_1.dfs.core.windows.net": "https://login"
    ".microsoftonline.com/directory_12345/oauth2/token",
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


def test_instance_profile_crawler(ws, inventory_schema, make_job, make_pipeline, sql_backend):
    crawler = AWSInstanceProfileCrawler(ws=ws, sbe=sql_backend, schema=inventory_schema)
    ips = crawler.snapshot()
    results = []
    for ip in ips:
        results.append(ip)

    assert len(results) >= 2


def test_spn_crawler(ws, inventory_schema, make_job, make_pipeline, sql_backend):
    make_job(spark_conf=_SPARK_CONF)
    make_pipeline(configuration=_PIPELINE_CONF)
    spn_crawler = AzureServicePrincipalCrawler(ws=ws, sbe=sql_backend, schema=inventory_schema)
    spns = spn_crawler.snapshot()
    results = []
    for spn in spns:
        results.append(spn)

    assert len(results) >= 2
    assert results[0].storage_account == "storage_acct_1"
    assert results[0].tenant_id == "directory_12345"


def test_spn_crawler_no_config(ws, inventory_schema, make_job, make_pipeline, sql_backend, make_cluster):
    make_job()
    make_pipeline()
    make_cluster(single_node=True)
    spn_crawler = AzureServicePrincipalCrawler(ws=ws, sbe=sql_backend, schema=inventory_schema)
    spns = spn_crawler.snapshot()
    results = []
    for spn in spns:
        results.append(spn)

    assert len(results) >= 1


def test_spn_crawler_with_pipeline_unavlbl_secret(ws, inventory_schema, make_job, make_pipeline, sql_backend):
    make_job(spark_conf=_SPARK_CONF)
    make_pipeline(configuration=_PIPELINE_CONF_WITH_SECRET)
    spn_crawler = AzureServicePrincipalCrawler(ws=ws, sbe=sql_backend, schema=inventory_schema)
    spns = spn_crawler.snapshot()
    results = []
    for spn in spns:
        results.append(spn)

    assert len(results) >= 2
    assert results[0].storage_account == "storage_acct_1"
    assert results[0].tenant_id == "directory_12345"


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
    spns = spn_crawler.snapshot()
    results = []
    for spn in spns:
        results.append(spn)

    ws.secrets.delete_secret(scope=secret_scope, key=secret_key)

    assert len(results) >= 2


def test_workspace_object_crawler(ws, make_directory, inventory_schema, sql_backend):
    new_directory = make_directory()
    workspace_listing = WorkspaceListing(
        ws=ws, sql_backend=sql_backend, inventory_database=inventory_schema, start_path=new_directory
    )
    listing_results = workspace_listing.snapshot()
    results = []
    for _result in listing_results:
        if _result.path == new_directory:
            results.append(_result)

    assert len(results) == 1
    assert results[0].path == new_directory
    assert results[0].object_type == "DIRECTORY"

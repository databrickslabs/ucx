import base64
import logging
import os
import time

from databricks.sdk.service import jobs, pipelines, workspace

from databricks.labs.ucx.assessment.crawlers import (
    AzureServicePrincipalCrawler,
    ClustersCrawler,
    JobsCrawler,
    PipelinesCrawler,
)
from databricks.labs.ucx.framework.crawlers import StatementExecutionBackend

logger = logging.getLogger(__name__)


def import_notebook(w) -> str:
    notebook_path = f"/Users/{w.current_user.me().user_name}/ucx-dlt-pipeline-notebook"
    w.workspace.import_(
        path=notebook_path,
        overwrite=True,
        format=workspace.ImportFormat.SOURCE,
        language=workspace.Language.PYTHON,
        content=base64.b64encode(b"""Dummy Notebook for DLT""").decode(),
    )
    return notebook_path


def create_dlt_pipeline(ws) -> str:
    notebook_path = import_notebook(ws)
    pipeline = ws.pipelines.create(
        name=f"ucx_SPN_config_test-{time.time_ns()}",
        libraries=[pipelines.PipelineLibrary(notebook=pipelines.NotebookLibrary(path=notebook_path))],
        clusters=[
            pipelines.PipelineCluster(
                instance_pool_id=os.environ["TEST_INSTANCE_POOL_ID"],
                label="default",
                num_workers=1,
                custom_tags={
                    "cluster_type": "default",
                },
            )
        ],
        development=True,
        configuration={
            "spark.hadoop.fs.azure.account.oauth2.client.id."
            "newstorageacct.dfs.core.windows"
            ".net": "pipeline_dummy_application_id",
            "spark.hadoop.fs.azure.account.oauth2.client.endpoint."
            "newstorageacct.dfs.core.windows.net": "https://login.microsoftonline.com"
            "/directory_12345/oauth2/token",
        },
    )

    return pipeline.pipeline_id


def test_pipeline_crawler(ws, make_schema):
    warehouse_id = os.environ["TEST_DEFAULT_WAREHOUSE_ID"]
    logger.info("setting up fixtures")
    pipeline_id = create_dlt_pipeline(ws)

    inventory_schema = make_schema(catalog="hive_metastore")
    _, inventory_schema = inventory_schema.split(".")
    backend = StatementExecutionBackend(ws, warehouse_id)

    pipeline_crawler = PipelinesCrawler(ws=ws, sbe=backend, schema=inventory_schema)
    pipelines = pipeline_crawler.snapshot()
    results = []
    for pipeline in pipelines:
        if pipeline.success != 0:
            continue
        if pipeline.pipeline_id == pipeline_id:
            results.append(pipeline)

    assert len(results) == 1
    assert results[0].pipeline_id == pipeline_id


def create_cluster(ws) -> str:
    latest = ws.clusters.select_spark_version(latest=True)

    cluster_name = f"sdk-ucx-test-cluster-{time.time_ns()}"

    clstr = ws.clusters.create(
        cluster_name=cluster_name,
        spark_version=latest,
        instance_pool_id=os.environ["TEST_INSTANCE_POOL_ID"],
        autotermination_minutes=15,
        spark_conf={
            "fs.azure.account.auth.type.storage_acct_1.dfs.core.windows.net": "OAuth",
            "fs.azure.account.oauth.provider.type.storage_acct_1.dfs.core.windows"
            ".net": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
            "fs.azure.account.oauth2.client.id.storage_acct_1.dfs.core.windows.net": "dummy_application_id",
            "fs.azure.account.oauth2.client.secret.storage_acct_1.dfs.core.windows.net": "dummy",
            "fs.azure.account.oauth2.client.endpoint.storage_acct_1.dfs.core.windows"
            ".net": "https://login.microsoftonline.com/dummy_tenant_id/oauth2/token",
        },
        num_workers=1,
    ).result()
    return clstr.cluster_id


def test_cluster_crawler(ws, make_schema):
    warehouse_id = os.environ["TEST_DEFAULT_WAREHOUSE_ID"]
    cluster_id = create_cluster(ws)

    inventory_schema = make_schema(catalog="hive_metastore")
    _, inventory_schema = inventory_schema.split(".")

    backend = StatementExecutionBackend(ws, warehouse_id)
    cluster_crawler = ClustersCrawler(ws=ws, sbe=backend, schema=inventory_schema)
    clusters = cluster_crawler.snapshot()
    results = []
    for cluster in clusters:
        if cluster.success != 0:
            continue
        if cluster.cluster_id == cluster_id:
            results.append(cluster)

    assert len(results) == 1
    assert results[0].cluster_id == cluster_id


def create_job(ws) -> int:
    notebook_path = import_notebook(ws)

    ws.clusters.ensure_cluster_is_running(os.environ["DATABRICKS_CLUSTER_ID"]) and os.environ["DATABRICKS_CLUSTER_ID"]
    cluster_id = os.environ["DATABRICKS_CLUSTER_ID"]

    created_job = ws.jobs.create(
        name=f"sdk-ucx-jobs-{time.time_ns()}",
        tasks=[
            jobs.Task(
                description="test",
                existing_cluster_id=cluster_id,
                notebook_task=jobs.NotebookTask(notebook_path=notebook_path),
                task_key="test",
                timeout_seconds=0,
            )
        ],
    )
    return created_job.job_id


def test_job_crawler(ws, make_schema):
    warehouse_id = os.environ["TEST_DEFAULT_WAREHOUSE_ID"]

    job_id = create_job(ws)

    inventory_schema = make_schema(catalog="hive_metastore")
    _, inventory_schema = inventory_schema.split(".")

    backend = StatementExecutionBackend(ws, warehouse_id)
    job_crawler = JobsCrawler(ws=ws, sbe=backend, schema=inventory_schema)
    jobs = job_crawler.snapshot()
    results = []
    for job in jobs:
        if int(job.job_id) == job_id:
            if job.success != 0:
                results.append(job)

    assert len(results) == 1


def test_spn_crawler(ws, make_schema):
    warehouse_id = os.environ["TEST_DEFAULT_WAREHOUSE_ID"]
    inventory_schema = make_schema(catalog="hive_metastore")
    _, inventory_schema = inventory_schema.split(".")

    backend = StatementExecutionBackend(ws, warehouse_id)
    spn_crawler = AzureServicePrincipalCrawler(ws=ws, sbe=backend, schema=inventory_schema)
    spns = spn_crawler.snapshot()
    results = []
    for spn in spns:
        results.append(spn)

    assert len(results) > 0

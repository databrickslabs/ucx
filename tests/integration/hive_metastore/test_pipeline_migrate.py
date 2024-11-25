from databricks.sdk.service.jobs import Task, PipelineTask
from databricks.sdk.service.pipelines import NotebookLibrary, PipelineLibrary, PipelineCluster
from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.hive_metastore.pipelines_migrate import PipelinesMigrator

from ..assessment.test_assessment import _PIPELINE_CONF

_TEST_STORAGE_ACCOUNT = "storage_acct_1"
_TEST_TENANT_ID = "directory_12345"


def test_pipeline_migrate(
    ws,
    make_pipeline,
    make_notebook,
    make_random,
    watchdog_purge_suffix,
    make_directory,
    runtime_ctx,
    make_catalog,
    make_job,
):
    src_schema = runtime_ctx.make_schema(catalog_name="hive_metastore")
    dlt_notebook_path = f"{make_directory()}/dlt_notebook.py"
    src_table = runtime_ctx.make_table(catalog_name="hive_metastore", schema_name=src_schema.name, non_delta=True)
    make_notebook(
        content=f"""create or refresh streaming table st1\nas select * from stream(hive_metastore.{src_schema.name}.{src_table.name})""".encode(
            "ASCII"
        ),
        path=dlt_notebook_path,
        language=Language.SQL,
    )

    pipeline_name = f"pipeline-{make_random(4).lower()}-{watchdog_purge_suffix}"
    created_pipelines = [
        make_pipeline(
            configuration=_PIPELINE_CONF,
            name=pipeline_name,
            target=runtime_ctx.make_schema(catalog_name="hive_metastore").name,
            libraries=[PipelineLibrary(notebook=NotebookLibrary(path=dlt_notebook_path))],
            clusters=[
                PipelineCluster(
                    node_type_id=ws.clusters.select_node_type(local_disk=False, min_memory_gb=16),
                    label="default",
                    num_workers=1,
                    custom_tags={"cluster_type": "default", "RemoveAfter": watchdog_purge_suffix},
                )
            ],
        ),
        make_pipeline(
            configuration=_PIPELINE_CONF,
            name=f"skip-{pipeline_name}",
            target=runtime_ctx.make_schema(catalog_name="hive_metastore").name,
            libraries=[PipelineLibrary(notebook=NotebookLibrary(path=dlt_notebook_path))],
        ),
    ]

    job_with_pipeline_list = [
        make_job(
            tasks=[
                Task(
                    pipeline_task=PipelineTask(pipeline_id=created_pipelines[0].pipeline_id),
                    task_key=make_random(4).lower(),
                )
            ]
        ),
        make_job(
            tasks=[
                Task(
                    pipeline_task=PipelineTask(pipeline_id=created_pipelines[0].pipeline_id),
                    task_key=make_random(4).lower(),
                )
            ]
        ),
    ]
    pipelines = runtime_ctx.pipelines_crawler.snapshot()
    results = []
    for pipeline in pipelines:
        if pipeline.success != 0:
            continue
        if pipeline.pipeline_id == created_pipelines[0].pipeline_id:
            results.append(pipeline)
    assert len(results) == 1

    pipelines_migrator = PipelinesMigrator(
        ws, runtime_ctx.pipelines_crawler, make_catalog().name, skip_pipeline_ids=[created_pipelines[1].pipeline_id]
    )
    pipelines_migrator.migrate_pipelines()

    # crawl pipeline in UC and check if it is migrated
    pipelines = runtime_ctx.pipelines_crawler.snapshot(force_refresh=True)
    results = []
    for pipeline in pipelines:
        if pipeline.pipeline_name == f"{pipeline_name}":
            results.append(pipeline)

    assert len(results) == 1

    assert (
        ws.jobs.get(job_with_pipeline_list[0].job_id).settings.tasks[0].pipeline_task.pipeline_id
        == results[0].pipeline_id
    )
    assert (
        ws.jobs.get(job_with_pipeline_list[1].job_id).settings.tasks[0].pipeline_task.pipeline_id
        == results[0].pipeline_id
    )

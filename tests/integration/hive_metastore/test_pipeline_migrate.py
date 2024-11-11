from databricks.sdk.service.jobs import Task, PipelineTask
from databricks.sdk.service.pipelines import NotebookLibrary, PipelineLibrary

from databricks.labs.ucx.hive_metastore.pipelines_migrate import PipelinesMigrator

from ..assessment.test_assessment import _PIPELINE_CONF

_TEST_STORAGE_ACCOUNT = "storage_acct_1"
_TEST_TENANT_ID = "directory_12345"


# pylint: disable=too-many-locals, too-many-arguments
def test_pipeline_migrate(
    ws,
    make_pipeline,
    make_notebook,
    make_random,
    watchdog_purge_suffix,
    make_directory,
    inventory_schema,
    runtime_ctx,
    make_catalog,
    make_job,
):
    dst_catalog = make_catalog()
    src_schema = runtime_ctx.make_schema(catalog_name="hive_metastore")
    target_schemas = 2 * [runtime_ctx.make_schema(catalog_name="hive_metastore")]

    dlt_notebook_path = f"{make_directory()}/dlt_notebook.py"
    src_table = runtime_ctx.make_table(catalog_name="hive_metastore", schema_name=src_schema.name, non_delta=True)
    dlt_notebook_text = (
        f"""create streaming table st1\nas select * from stream(hive_metastore.{src_schema.name}.{src_table.name})"""
    )
    make_notebook(content=dlt_notebook_text.encode("ASCII"), path=dlt_notebook_path)

    pipeline_name = f"pipeline-{make_random(4).lower()}-{watchdog_purge_suffix}"
    created_pipeline = make_pipeline(
        configuration=_PIPELINE_CONF,
        name=pipeline_name,
        target=target_schemas[0].name,
        libraries=[PipelineLibrary(notebook=NotebookLibrary(path=dlt_notebook_path))],
    )
    skip_pipeline = make_pipeline(
        configuration=_PIPELINE_CONF,
        name=f"skip-{pipeline_name}",
        target=target_schemas[1].name,
        libraries=[PipelineLibrary(notebook=NotebookLibrary(path=dlt_notebook_path))],
    )

    job_with_pipeline = make_job(
        tasks=[
            Task(pipeline_task=PipelineTask(pipeline_id=created_pipeline.pipeline_id), task_key=make_random(4).lower())
        ]
    )
    pipelines = runtime_ctx.pipelines_crawler.snapshot()
    results = []
    for pipeline in pipelines:
        if pipeline.success != 0:
            continue
        if pipeline.pipeline_id == created_pipeline.pipeline_id:
            results.append(pipeline)
    assert len(results) == 1

    pipelines_migrator = PipelinesMigrator(
        ws, runtime_ctx.pipelines_crawler, dst_catalog.name, skip_pipelines=[skip_pipeline.pipeline_id]
    )
    pipelines_migrator.migrate_pipelines()

    # crawl pipeline in UC and check if it is migrated
    pipelines = runtime_ctx.pipelines_crawler.snapshot(force_refresh=True)
    results = []
    for pipeline in pipelines:
        if pipeline.pipeline_name == f"{pipeline_name}":
            results.append(pipeline)

    assert len(results) == 1

    assert ws.jobs.get(job_with_pipeline.job_id).settings.tasks[0].pipeline_task.pipeline_id == results[0].pipeline_id

from databricks.sdk.service.jobs import Task, PipelineTask
from databricks.sdk.service.pipelines import NotebookLibrary, PipelineLibrary, PipelineCluster
from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.hive_metastore.pipelines_migrate import PipelinesMigrator

from ..assessment.test_assessment import _PIPELINE_CONF

_TEST_STORAGE_ACCOUNT = "storage_acct_1"
_TEST_TENANT_ID = "directory_12345"


def test_pipeline_migrate(
    ws, make_pipeline, make_random, watchdog_purge_suffix, make_directory, runtime_ctx, make_mounted_location
) -> None:
    src_schema = runtime_ctx.make_schema(catalog_name="hive_metastore")

    src_tables = [
        runtime_ctx.make_table(catalog_name="hive_metastore", schema_name=src_schema.name, non_delta=True),
        runtime_ctx.make_table(
            schema_name=src_schema.name,
            external_csv=make_mounted_location,
            columns=[("`foobar`", "STRING")],
        ),
    ]

    dlt_notebooks = [
        runtime_ctx.make_notebook(
            content=f"""create or refresh streaming table st1\nas select * from stream(hive_metastore.{src_schema.name}.{src_tables[0].name})""".encode(
                "ASCII"
            ),
            path=f"{make_directory()}/dlt_notebook_1",
            language=Language.SQL,
        ),
        runtime_ctx.make_notebook(
            content=f"""create or refresh streaming table st2\nas select * from stream(hive_metastore.{src_schema.name}.{src_tables[1].name})""".encode(
                "ASCII"
            ),
            path=f"{make_directory()}/dlt_notebook_2",
            language=Language.SQL,
        ),
    ]

    pipeline_names = [
        f"pipeline-{make_random(4).lower()}-{watchdog_purge_suffix}",
        f"pipeline-{make_random(4).lower()}-{watchdog_purge_suffix}",
        f"skip-pipeline-{make_random(4).lower()}-{watchdog_purge_suffix}",
    ]
    created_pipelines = [
        make_pipeline(
            configuration=_PIPELINE_CONF,
            name=pipeline_names[0],
            target=runtime_ctx.make_schema(catalog_name="hive_metastore").name,
            libraries=[PipelineLibrary(notebook=NotebookLibrary(path=str(dlt_notebooks[0])))],
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
            name=pipeline_names[1],
            target=runtime_ctx.make_schema(catalog_name="hive_metastore").name,
            libraries=[PipelineLibrary(notebook=NotebookLibrary(path=str(dlt_notebooks[1])))],
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
            name=f"skip-{pipeline_names[2]}",
            target=runtime_ctx.make_schema(catalog_name="hive_metastore").name,
            libraries=[PipelineLibrary(notebook=NotebookLibrary(path=str(dlt_notebooks[0])))],
        ),
    ]

    job_with_pipeline_list = [
        runtime_ctx.make_job(
            tasks=[
                Task(
                    pipeline_task=PipelineTask(pipeline_id=created_pipelines[0].pipeline_id),
                    task_key=make_random(4).lower(),
                )
            ]
        ),
        runtime_ctx.make_job(
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
        if pipeline.pipeline_id in [
            created_pipelines[0].pipeline_id,
            created_pipelines[1].pipeline_id,
            created_pipelines[2].pipeline_id,
        ]:
            results.append(pipeline)
    assert len(results) == 3

    pipelines_migrator = PipelinesMigrator(
        ws,
        runtime_ctx.pipelines_crawler,
        runtime_ctx.jobs_crawler,
        runtime_ctx.make_catalog().name,
        include_pipeline_ids=[created_pipelines[0].pipeline_id, created_pipelines[1].pipeline_id],
    )
    pipelines_migrator.migrate_pipelines()

    # crawl pipeline in UC and check if it is migrated
    pipelines = runtime_ctx.pipelines_crawler.snapshot(force_refresh=True)
    results = []
    for pipeline in pipelines:
        if pipeline.pipeline_name in [pipeline_names[0], pipeline_names[1]]:
            results.append(pipeline)

    assert len(results) == 2

    for job_with_pipeline in job_with_pipeline_list:
        assert ws.jobs.get(job_with_pipeline.job_id).settings.tasks[0].pipeline_task.pipeline_id in [
            results[0].pipeline_id,
            results[1].pipeline_id,
        ]

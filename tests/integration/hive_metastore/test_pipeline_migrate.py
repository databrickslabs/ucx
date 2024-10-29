from databricks.sdk.service.pipelines import PipelineLibrary, NotebookLibrary

from databricks.labs.ucx.assessment.pipelines import PipelinesCrawler
from databricks.labs.ucx.hive_metastore.pipelines_migrate import PipelinesMigrator, PipelineRule, PipelineMapping
from ..assessment.test_assessment import _PIPELINE_CONF

_TEST_STORAGE_ACCOUNT = "storage_acct_1"
_TEST_TENANT_ID = "directory_12345"


# pylint: disable=too-many-locals
def test_pipeline_migrate(
    ws,
    make_pipeline,
    make_notebook,
    make_random,
    watchdog_purge_suffix,
    make_directory,
    inventory_schema,
    sql_backend,
    runtime_ctx,
):
    src_schema = runtime_ctx.make_schema(catalog_name="hive_metastore")
    target_schemas = [
        runtime_ctx.make_schema(catalog_name="hive_metastore"),
        runtime_ctx.make_schema(catalog_name="hive_metastore"),
        runtime_ctx.make_schema(catalog_name="hive_metastore"),
        runtime_ctx.make_schema(catalog_name="hive_metastore"),
        runtime_ctx.make_schema(catalog_name="hive_metastore"),
    ]

    dlt_notebook_path = f"{make_directory()}/dlt_notebook.py"
    src_table = runtime_ctx.make_table(catalog_name="hive_metastore", schema_name=src_schema.name, non_delta=True)
    dlt_notebook_text = (
        f"""create streaming table st1\nas select * from stream(hive_metastore.{src_schema.name}.{src_table.name})"""
    )
    make_notebook(content=dlt_notebook_text.encode("ASCII"), path=dlt_notebook_path)

    pipeline_names = [
        f"pipeline-{make_random(4).lower()}-{watchdog_purge_suffix}",
        f"pipeline-{make_random(4).lower()}-{watchdog_purge_suffix}",
        f"pipeline-{make_random(4).lower()}-{watchdog_purge_suffix}",
    ]

    created_pipelines = [
        make_pipeline(
            configuration=_PIPELINE_CONF,
            name=pipeline_names[0],
            target=target_schemas[0].name,
            libraries=[PipelineLibrary(notebook=NotebookLibrary(path=dlt_notebook_path))],
        ),
        make_pipeline(
            configuration=_PIPELINE_CONF,
            name=pipeline_names[1],
            target=target_schemas[1].name,
            libraries=[PipelineLibrary(notebook=NotebookLibrary(path=dlt_notebook_path))],
        ),
        make_pipeline(
            configuration=_PIPELINE_CONF,
            name=pipeline_names[2],
            target=target_schemas[2].name,
            libraries=[PipelineLibrary(notebook=NotebookLibrary(path=dlt_notebook_path))],
        ),
    ]

    pipeline_crawler = PipelinesCrawler(ws=ws, sql_backend=sql_backend, schema=inventory_schema)
    pipelines = pipeline_crawler.snapshot()
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

    # TODO: Add other rules as well to test the migration
    pipeline_rules = [
        PipelineRule.from_src_dst("test_workspace", created_pipelines[0].pipeline_id, "test_catalog"),
        PipelineRule.from_src_dst(
            "test_workspace", created_pipelines[1].pipeline_id, "test_catalog", target_schemas[3].name
        ),
        PipelineRule.from_src_dst(
            "test_workspace",
            created_pipelines[2].pipeline_id,
            "test_catalog",
            target_schemas[4].name,
            f"{pipeline_names[2]}-migrated",
        ),
    ]
    runtime_ctx.with_pipeline_mapping_rules(pipeline_rules)
    pipeline_mapping = PipelineMapping(runtime_ctx.installation, ws, sql_backend)

    pipelines_migrator = PipelinesMigrator(ws, pipeline_crawler, pipeline_mapping)
    pipelines_migrator.migrate_pipelines()

    # crawl pipeline in UC and check if it is migrated
    pipelines = pipeline_crawler.snapshot(force_refresh=True)
    results = []
    for pipeline in pipelines:
        if pipeline.pipeline_name in [
            f"{pipeline_names[0]} [UC]",
            f"{pipeline_names[1]} [UC]",
            f"{pipeline_names[2]}-migrated",
        ]:
            results.append(pipeline)

    assert len(results) == 3

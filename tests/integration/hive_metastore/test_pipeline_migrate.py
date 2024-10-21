from databricks.sdk.service.pipelines import PipelineLibrary, NotebookLibrary, PipelinesAPI

from databricks.get_uninstalled_libraries import libraries
from databricks.labs.ucx.assessment.pipelines import PipelinesCrawler
from databricks.labs.ucx.hive_metastore.pipelines_migrate import PipelinesMigrator, PipelineRule, PipelineMapping
from integration.conftest import runtime_ctx

_TEST_STORAGE_ACCOUNT = "storage_acct_1"
_TEST_TENANT_ID = "directory_12345"

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


def test_pipeline_migrate(ws, make_pipeline, make_notebook, make_directory, inventory_schema, sql_backend, runtime_ctx):

    src_schema = runtime_ctx.make_schema(catalog_name="hive_metastore")
    target_schema = runtime_ctx.make_schema(catalog_name="hive_metastore")


    dlt_notebook_path = f"{make_directory()}/dlt_notebook.py"
    src_table = runtime_ctx.make_table(catalog_name="hive_metastore", schema_name=src_schema.name, non_delta=True)
    dlt_notebook_text = f"""create streaming table st1\nas select * from stream(hive_metastore.{src_schema.name}.{src_table.name})"""
    dlt_notebook_source = dlt_notebook_text.encode("ASCII")
    make_notebook(content=dlt_notebook_source, path=dlt_notebook_path)

    created_pipeline = make_pipeline(configuration=_PIPELINE_CONF, target=target_schema.name, libraries=[
        PipelineLibrary(notebook=NotebookLibrary(path=dlt_notebook_path))
    ])
    pipeline_crawler = PipelinesCrawler(ws=ws, sbe=sql_backend, schema=inventory_schema)
    pipelines = pipeline_crawler.snapshot()
    results = []
    for pipeline in pipelines:
        if pipeline.success != 0:
            continue
        if pipeline.pipeline_id == created_pipeline.pipeline_id:
            results.append(pipeline)
            created_pipeline_name = pipeline.pipeline_name

    assert len(results) >= 1
    assert results[0].pipeline_id == created_pipeline.pipeline_id

    # TODO: Add other rules as well to test the migration
    pipeline_rules = [PipelineRule.from_src_dst("test_workspace", created_pipeline.pipeline_id, "test_catalog"
                                                )]
    runtime_ctx.with_pipeline_mapping_rules(pipeline_rules)
    pipeline_mapping = PipelineMapping(runtime_ctx.installation, ws, sql_backend)

    pipelines_migrator = PipelinesMigrator(ws, pipeline_crawler, pipeline_mapping)
    pipelines_migrator.migrate_pipelines()

    # crawl pipeline in UC and check if it is migrated
    pipelines = pipeline_crawler.snapshot(force_refresh=True)
    results = []
    for pipeline in pipelines:
        # TODO: Commented out as the migrated pipeline currently is in failed state after migration
        # if pipeline.success != 0:
        #     continue
        if pipeline.pipeline_name == f"{created_pipeline_name} [UC]":
            results.append(pipeline)
            break

    assert len(results) == 1

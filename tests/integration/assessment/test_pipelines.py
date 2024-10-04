from datetime import timedelta

from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried

from databricks.labs.ucx.assessment.pipelines import PipelineOwnership, PipelinesCrawler

from .test_assessment import _PIPELINE_CONF, _PIPELINE_CONF_WITH_SECRET, logger


@retried(on=[NotFound], timeout=timedelta(minutes=5))
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


@retried(on=[NotFound], timeout=timedelta(minutes=5))
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


def test_pipeline_ownership(ws, runtime_ctx, make_pipeline, inventory_schema, sql_backend) -> None:
    """Verify the ownership can be determined for crawled pipelines."""

    # Set up a pipeline.
    # Note: there doesn't seem to be a way to change the owner of a pipeline, so we can't test pipelines without an
    # owner.
    pipeline = make_pipeline()

    # Produce the crawled records.
    crawler = PipelinesCrawler(ws, sql_backend, inventory_schema)
    records = crawler.snapshot(force_refresh=True)

    # Find the crawled record for our pipeline.
    pipeline_record = next(record for record in records if record.pipeline_id == pipeline.pipeline_id)

    # Verify ownership is as expected.
    ownership = PipelineOwnership(ws, runtime_ctx.administrator_locator)
    assert ownership.owner_of(pipeline_record) == ws.current_user.me().user_name

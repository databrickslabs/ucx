from datetime import timedelta

from databricks.sdk.errors import NotFound

from databricks.labs.ucx.assessment.pipelines import PipelinesCrawler

from ..retries import retried
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

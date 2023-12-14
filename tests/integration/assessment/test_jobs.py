from datetime import timedelta

from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried

from databricks.labs.ucx.assessment.jobs import JobsCrawler
from integration.assessment.test_assessment import _SPARK_CONF


@retried(on=[NotFound], timeout=timedelta(minutes=5))
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

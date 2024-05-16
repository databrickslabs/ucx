import json
import time
from datetime import timedelta

from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service.jobs import NotebookTask, RunTask
from databricks.sdk.service.workspace import ImportFormat

from databricks.labs.ucx.assessment.jobs import JobsCrawler, SubmitRunsCrawler

from .test_assessment import _SPARK_CONF


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


@retried(on=[NotFound], timeout=timedelta(minutes=5))
def test_job_run_crawler(ws, env_or_skip, inventory_schema, sql_backend):
    cluster_id = env_or_skip("TEST_DEFAULT_CLUSTER_ID")
    dummy_notebook = """# Databricks notebook source
# MAGIC
# COMMAND ----------
pass
"""
    directory = "/tmp/ucx"
    notebook = "dummy_notebook"
    ws.workspace.mkdirs(directory)
    ws.workspace.upload(
        f"{directory}/{notebook}.py", dummy_notebook.encode("utf8"), format=ImportFormat.AUTO, overwrite=True
    )
    tasks = [
        RunTask(
            task_key="123",
            notebook_task=NotebookTask(notebook_path=f"{directory}/{notebook}"),
            existing_cluster_id=cluster_id,
        )
    ]
    run = ws.jobs.submit(run_name=f'ucx-test-{time.time_ns()}', tasks=tasks).result()
    assert run
    run_id = run.run_id

    job_run_crawler = SubmitRunsCrawler(ws=ws, sbe=sql_backend, schema=inventory_schema, num_days_history=1)
    job_runs = job_run_crawler.snapshot()

    assert len(job_runs) >= 1
    failures = None
    for job_run in job_runs:
        if run_id in json.loads(job_run.run_ids):
            failures = job_run.failures
            continue
    assert failures and failures == "[]"


def test_removeafter_tag(ws, env_or_skip, make_job):
    new_job = make_job(spark_conf=_SPARK_CONF)

    created_job = ws.jobs.get(new_job.job_id)
    assert "RemoveAfter" in created_job.settings.tags

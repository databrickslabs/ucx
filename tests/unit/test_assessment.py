from unittest.mock import Mock

import pytest
from databricks.sdk.service.compute import ComputeSpec, ComputeSpecKind, ClusterDetails, AutoScale
from databricks.sdk.service.jobs import BaseJob, Job, JobSettings, JobCompute, JobEmailNotifications, NotebookTask, Task

from databricks.labs.ucx.toolkits.assessment import AssessmentToolkit
from databricks.labs.ucx.tacl.tables import Table


@pytest.fixture
def ws():
    client = Mock()
    return client


def test_table_inventory(ws):
    assess = AssessmentToolkit(ws, "Fake_ID", "CSX", "assessment")
    assess.table_inventory()


def test_external_locations(ws):
    assess = AssessmentToolkit(ws, "Fake_ID", "CSX", "assessment")
    sample_tables = [
        Table("No_Catalog", "No_Database", "No_Name", "TABLE", "DELTA",
              location="s3://us-east-1-dev-account-staging-uc-ext-loc-bucket-1/Location/Table"),
        Table("No_Catalog", "No_Database", "No_Name", "TABLE", "DELTA",
              location="s3://us-east-1-dev-account-staging-uc-ext-loc-bucket-1/Location/Table2"),
        Table("No_Catalog", "No_Database", "No_Name", "TABLE", "DELTA",
              location="s3://us-east-1-dev-account-staging-uc-ext-loc-bucket-23/testloc/Table3"),
        Table("No_Catalog", "No_Database", "No_Name", "TABLE", "DELTA",
              location="s3://us-east-1-dev-account-staging-uc-ext-loc-bucket-23/Table4")
    ]
    result_set = assess.external_locations(sample_tables)
    assert (result_set[0] == "s3://us-east-1-dev-account-staging-uc-ext-loc-bucket-1/Location/")
    assert (result_set[1] == "s3://us-east-1-dev-account-staging-uc-ext-loc-bucket-23/")


def test_job_assessment(ws):
    assess = AssessmentToolkit(ws, "Fake_ID", "CSX", "assessment")
    sample_jobs = [
        BaseJob(created_time=1694536604319, creator_user_name='daniel.cadenas@databricks.com', job_id=536591785949415,
                settings=JobSettings(compute=None, continuous=None,
                                     tasks=[
                                         Task(task_key='Ingest', compute_key=None, condition_task=None, dbt_task=None,
                                              depends_on=None, description=None,
                                              email_notifications=None, existing_cluster_id='0807-225846-motto493',
                                              health=None, job_cluster_key=None,
                                              libraries=None, max_retries=None, min_retry_interval_millis=None,
                                              new_cluster=None,
                                              notebook_task=NotebookTask(
                                                  notebook_path='/Users/daniel.cadenas@databricks.com/Customers/Solistica/1.\
                             TMMX_Predictive_Model_Top_15_vs_Clients - Data Ingestion/Load',
                                                  base_parameters=None,
                                              )
                                              , notification_settings=None, pipeline_task=None, python_wheel_task=None,
                                              retry_on_timeout=None,
                                              spark_python_task=None, spark_submit_task=None, sql_task=None,
                                              timeout_seconds=0)],
                                     timeout_seconds=0, trigger=None, webhook_notifications=None))
    ]

    sample_clusters = [
        ClusterDetails(autoscale=AutoScale(min_workers=1, max_workers=6), spark_conf={
            'spark.databricks.delta.preview.enabled': 'true'}, spark_context_id=5134472582179565315,
                       spark_env_vars=None, spark_version='13.3.x-cpu-ml-scala2.12', cluster_id='0807-225846-motto493')
    ]
    sample_clusters_by_id = {c.cluster_id: c for c in sample_clusters}
    result_set = AssessmentToolkit.parse_jobs(sample_jobs, sample_clusters_by_id)
    assert (result_set.get("536591785949415") is None)
    assert (len(result_set) == 1)

from unittest.mock import Mock

import pytest
from databricks.sdk.service.compute import (
    AutoScale,
    ClusterDetails,
)
from databricks.sdk.service.jobs import (
    BaseJob,
    JobSettings,
    NotebookTask,
    Task,
)

from databricks.labs.ucx.assessment import AssessmentToolkit
from databricks.labs.ucx.assessment.assessment import ExternalLocationCrawler
from databricks.labs.ucx.framework.crawlers import StatementExecutionBackend
from databricks.labs.ucx.hive_metastore.list_mounts import Mount
from databricks.labs.ucx.hive_metastore.tables import Table
from databricks.labs.ucx.mixins.sql import Row
from tests.unit.framework.mocks import MockBackend


@pytest.fixture
def ws():
    client = Mock()
    return client


@pytest.fixture
def sbe():
    sbe = MockBackend()
    return sbe


def test_external_locations(ws, sbe):
    crawler = ExternalLocationCrawler(ws, sbe, "test")
    row_factory = type("Row", (Row,), {"__columns__": ["location"]})
    sample_locations = [
        row_factory(["s3://us-east-1-dev-account-staging-uc-ext-loc-bucket-1/Location/Table"]),
        row_factory(["s3://us-east-1-dev-account-staging-uc-ext-loc-bucket-1/Location/Table2"]),
        row_factory(["s3://us-east-1-dev-account-staging-uc-ext-loc-bucket-23/testloc/Table3"]),
        row_factory(["s3://us-east-1-dev-account-staging-uc-ext-loc-bucket-23/anotherloc/Table4"]),
        row_factory(["dbfs:/mnt/ucx/database1/table1"]),
        row_factory(["dbfs:/mnt/ucx/database2/table2"])
    ]
    sample_mounts = [Mount("/mnt/ucx", "s3://us-east-1-ucx-container")]
    result_set = crawler._external_locations(sample_locations, sample_mounts)
    assert len(result_set) == 3
    assert result_set[0].location == "s3://us-east-1-dev-account-staging-uc-ext-loc-bucket-1/Location/"
    assert result_set[1].location == "s3://us-east-1-dev-account-staging-uc-ext-loc-bucket-23/"
    assert result_set[2].location == "s3://us-east-1-ucx-container/"


def test_job_assessment(ws):
    crawler = AssessmentToolkit(Mock(), "UCX")
    sample_jobs = [
        BaseJob(
            created_time=1694536604319,
            creator_user_name="anonymous@databricks.com",
            job_id=536591785949415,
            settings=JobSettings(
                compute=None,
                continuous=None,
                tasks=[
                    Task(
                        task_key="Ingest",
                        existing_cluster_id="0807-225846-motto493",
                        notebook_task=NotebookTask(
                            notebook_path="/Users/daniel.cadenas@databricks.com/Customers/Solistica/1.\
                             TMMX_Predictive_Model_Top_15_vs_Clients - Data Ingestion/Load"
                        ),
                        timeout_seconds=0,
                    )
                ],
                timeout_seconds=0,
            ),
        ),
        BaseJob(
            created_time=1694536604321,
            creator_user_name="anonymous@databricks.com",
            job_id=536591785949416,
            settings=JobSettings(
                compute=None,
                continuous=None,
                tasks=[
                    Task(
                        task_key="Ingest",
                        existing_cluster_id="0810-225833-atlanta69",
                        notebook_task=NotebookTask(
                            notebook_path="/Users/daniel.cadenas@databricks.com/Customers/Solistica/1.\
                         TMMX_Predictive_Model_Top_15_vs_Clients - Data Ingestion/Load"
                        ),
                        timeout_seconds=0,
                    )
                ],
                timeout_seconds=0,
            ),
        ),
    ]

    sample_clusters = [
        ClusterDetails(
            autoscale=AutoScale(min_workers=1, max_workers=6),
            spark_conf={"spark.databricks.delta.preview.enabled": "true"},
            spark_context_id=5134472582179565315,
            spark_env_vars=None,
            spark_version="13.3.x-cpu-ml-scala2.12",
            cluster_id="0807-225846-motto493",
        ),
        ClusterDetails(
            autoscale=AutoScale(min_workers=1, max_workers=6),
            spark_conf={"spark.databricks.delta.preview.enabled": "true"},
            spark_context_id=5134472582179565315,
            spark_env_vars=None,
            spark_version="9.3.x-cpu-ml-scala2.12",
            cluster_id="0810-225833-atlanta69",
        ),
    ]
    sample_clusters_by_id = {c.cluster_id: c for c in sample_clusters}
    result_set = AssessmentToolkit._parse_jobs(sample_jobs, sample_clusters_by_id)
    assert len(result_set.get(536591785949415)) == 0
    assert len(result_set.get(536591785949416)) == 1
    assert len(result_set) == 2


def test_cluster_assessment(ws):
    sample_clusters = [
        ClusterDetails(
            autoscale=AutoScale(min_workers=1, max_workers=6),
            spark_conf={"spark.databricks.delta.preview.enabled": "true"},
            spark_context_id=5134472582179565315,
            spark_env_vars=None,
            spark_version="13.3.x-cpu-ml-scala2.12",
            cluster_id="0807-225846-motto493",
        ),
        ClusterDetails(
            autoscale=AutoScale(min_workers=1, max_workers=6),
            spark_conf={"spark.databricks.delta.preview.enabled": "true"},
            spark_context_id=5134472582179565315,
            spark_env_vars=None,
            spark_version="9.3.x-cpu-ml-scala2.12",
            cluster_id="0810-225833-atlanta69",
        ),
    ]
    result_set = AssessmentToolkit._parse_clusters(sample_clusters)
    assert len(result_set) == 2
    assert len(result_set.get("0807-225846-motto493")) == 0
    assert len(result_set.get("0810-225833-atlanta69")) == 1

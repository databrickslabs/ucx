import json
from unittest.mock import MagicMock

import pytest
from databricks.sdk.errors import DatabricksError, InternalError, NotFound
from databricks.sdk.service.compute import (
    AutoScale,
    ClusterDetails,
    ClusterSource,
    DataSecurityMode,
)

from databricks.labs.ucx.assessment.azure import AzureServicePrincipalCrawler
from databricks.labs.ucx.assessment.clusters import ClusterInfo, ClustersCrawler

from ..framework.mocks import MockBackend
from . import workspace_client_mock


def test_cluster_assessment():
    ws = workspace_client_mock(cluster_ids=['policy-single-user-with-spn', 'policy-azure-oauth'])
    crawler = ClustersCrawler(ws, MockBackend(), "ucx")
    result_set = list(crawler.snapshot())

    assert len(result_set) == 2
    assert result_set[0].success == 1
    assert result_set[1].success == 0


def test_cluster_assessment_cluster_policy_no_spark_conf():
    ws = workspace_client_mock(clusters="no-spark-conf.json")
    crawler = ClustersCrawler(ws, MockBackend(), "ucx")
    result_set1 = list(crawler.snapshot())
    assert len(result_set1) == 1
    assert result_set1[0].success == 1


def test_cluster_assessment_cluster_policy_not_found(caplog):
    ws = workspace_client_mock(cluster_ids=['policy-azure-oauth'])
    ws.cluster_policies.get = MagicMock()
    ws.cluster_policies.get.side_effect = NotFound("NO_POLICY")
    crawler = ClustersCrawler(ws, MockBackend(), "ucx")
    list(crawler.snapshot())
    assert "The cluster policy was deleted: azure-oauth" in caplog.messages


def test_cluster_assessment_cluster_policy_exception():
    ws = workspace_client_mock(cluster_ids=['policy-azure-oauth'])
    ws.cluster_policies.get = MagicMock()
    ws.cluster_policies.get.side_effect = InternalError(...)
    crawler = ClustersCrawler(ws, MockBackend(), "ucx")

    with pytest.raises(DatabricksError):
        list(crawler.snapshot())


def test_cluster_assessment_with_spn_cluster_policy_not_found(mocker):
    sample_clusters = [
        ClusterDetails(
            autoscale=AutoScale(min_workers=1, max_workers=6),
            cluster_source=ClusterSource.UI,
            spark_context_id=5134472582179565315,
            spark_env_vars=None,
            spark_conf={
                "spark.hadoop.fs.azure.account.oauth2.client.id.abcde.dfs.core.windows.net": "1234567890",
                "spark.databricks.delta.formatCheck.enabled": "false",
            },
            spark_version="9.3.x-cpu-ml-scala2.12",
            cluster_id="0810-225833-atlanta69",
            cluster_name="Tech Summit FY24 Cluster-1",
            policy_id="single-user-with-spn",
        )
    ]
    ws = workspace_client_mock()
    ws.clusters.list.return_value = sample_clusters
    ws.cluster_policies.get.side_effect = NotFound("NO_POLICY")
    crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()
    assert len(crawler) == 1


def test_cluster_assessment_with_spn_cluster_policy_exception(mocker):
    sample_clusters = [
        ClusterDetails(
            autoscale=AutoScale(min_workers=1, max_workers=6),
            cluster_source=ClusterSource.UI,
            spark_context_id=5134472582179565315,
            spark_env_vars=None,
            spark_version="9.3.x-cpu-ml-scala2.12",
            cluster_id="0810-225833-atlanta69",
            cluster_name="Tech Summit FY24 Cluster-1",
            policy_id="bdqwbdqiwd1111",
        )
    ]
    ws = mocker.Mock()
    ws.clusters.list.return_value = sample_clusters
    ws.cluster_policies.get.side_effect = InternalError(...)

    with pytest.raises(DatabricksError):
        AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()


def test_cluster_init_script():
    ws = workspace_client_mock(clusters='single-cluster-init-scripts.json')
    ws.dbfs.read().data = "JXNoCmVjaG8gIj0="
    init_crawler = ClustersCrawler(ws, MockBackend(), "ucx").snapshot()
    assert len(init_crawler) == 1


def test_cluster_init_script_check_dbfs():
    ws = workspace_client_mock(cluster_ids=['init-scripts-dbfs'])
    ws.dbfs.read().data = "JXNoCmVjaG8gIj0="
    init_crawler = ClustersCrawler(ws, MockBackend(), "ucx").snapshot()
    assert len(init_crawler) == 1


def test_cluster_without_owner_should_have_empty_creator_name():
    ws = workspace_client_mock(clusters="no-spark-conf.json")
    mockbackend = MockBackend()
    ClustersCrawler(ws, mockbackend, "ucx").snapshot()
    result = mockbackend.rows_written_for("hive_metastore.ucx.clusters", "append")
    assert result == [
        ClusterInfo(
            cluster_id="0915-190044-3dqy6751",
            cluster_name="Tech Summit FY24 Cluster-2",
            creator=None,
            success=1,
            failures='[]',
        )
    ]


def test_cluster_with_multiple_failures():
    ws = workspace_client_mock(cluster_ids=['passthrough'])
    crawler = ClustersCrawler(ws, MockBackend(), "ucx")
    result_set = list(crawler.snapshot())
    assert len(result_set) == 1
    assert result_set[0].success == 0
    failures = json.loads(result_set[0].failures)
    assert 'unsupported config: spark.databricks.passthrough.enabled' in failures
    assert 'not supported DBR: 9.3.x-cpu-ml-scala2.12' in failures


def test_cluster_with_job_source():
    ws = workspace_client_mock(cluster_ids=['job-cluster', 'policy-azure-oauth'])
    crawler = ClustersCrawler(ws, MockBackend(), "ucx")
    result_set = list(crawler.snapshot())

    assert len(result_set) == 1
    assert result_set[0].cluster_id == "0123-190044-1122334411"


def test_try_fetch():
    ws = workspace_client_mock(cluster_ids=[])
    mockBackend = MagicMock()
    mockBackend.fetch.return_value = [("000", 1, "123")]
    crawler = ClustersCrawler(ws, mockBackend, "ucx")
    result_set = list(crawler.snapshot())

    assert len(result_set) == 1
    assert result_set[0].cluster_id == "000"
    assert result_set[0].success == 1
    assert result_set[0].failures == "123"


def test_no_isolation_clusters():
    sample_clusters = [
        ClusterDetails(
            cluster_name="No isolation shared",
            spark_version="12.3.x-cpu-ml-scala2.12",
            data_security_mode=DataSecurityMode.NONE,
        )
    ]
    ws = workspace_client_mock()
    mockBackend = MagicMock()
    ws.clusters.list.return_value = sample_clusters
    crawler = ClustersCrawler(ws, mockBackend, "ucx")
    result_set = list(crawler.snapshot())
    assert len(result_set) == 1
    assert result_set[0].failures == '["No isolation shared clusters not supported in UC"]'


def test_unsupported_clusters():
    sample_clusters = [
        ClusterDetails(
            cluster_name="Passthrough cluster",
            spark_version="12.3.x-cpu-ml-scala2.12",
            data_security_mode=DataSecurityMode.LEGACY_PASSTHROUGH,
        )
    ]
    ws = workspace_client_mock()
    mockBackend = MagicMock()
    ws.clusters.list.return_value = sample_clusters
    crawler = ClustersCrawler(ws, mockBackend, "ucx")
    result_set = list(crawler.snapshot())
    assert len(result_set) == 1
    assert result_set[0].failures == '["cluster type not supported : LEGACY_PASSTHROUGH"]'

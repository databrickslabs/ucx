import json
from unittest.mock import MagicMock, PropertyMock, create_autospec, mock_open, patch

import pytest
from databricks.labs.lsql.backends import MockBackend
from databricks.sdk.errors import DatabricksError, InternalError, NotFound
from databricks.sdk.service.compute import ClusterDetails

from databricks.labs.ucx.assessment.azure import AzureServicePrincipalCrawler
from databricks.labs.ucx.assessment.clusters import ClustersCrawler, PoliciesCrawler, ClusterOwnership, ClusterInfo
from databricks.labs.ucx.framework.crawlers import SqlBackend
from databricks.labs.ucx.framework.owners import AdministratorLocator

from .. import mock_workspace_client


def test_cluster_assessment():
    ws = mock_workspace_client(
        cluster_ids=['policy-single-user-with-spn', 'policy-azure-oauth'],
    )
    crawler = ClustersCrawler(ws, MockBackend(), "ucx")
    result_set = list(crawler.snapshot())

    assert len(result_set) == 2
    assert result_set[0].success == 1
    assert result_set[1].success == 0


def test_cluster_assessment_cluster_policy_not_found(caplog):
    ws = mock_workspace_client(cluster_ids=['policy-deleted'])
    crawler = ClustersCrawler(ws, MockBackend(), "ucx")
    list(crawler.snapshot())
    assert "The cluster policy was deleted: deleted" in caplog.messages


def test_cluster_assessment_cluster_policy_exception():
    ws = mock_workspace_client(
        cluster_ids=['policy-azure-oauth'],
    )
    ws.cluster_policies.get = MagicMock(side_effect=InternalError(...))
    crawler = ClustersCrawler(ws, MockBackend(), "ucx")

    with pytest.raises(DatabricksError):
        list(crawler.snapshot())


def test_cluster_assessment_with_spn_cluster_policy_not_found():
    ws = mock_workspace_client(
        cluster_ids=['policy-azure-oauth'],
    )
    ws.cluster_policies.get.side_effect = NotFound("NO_POLICY")
    crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()
    assert len(crawler) == 1


def test_cluster_assessment_with_spn_cluster_policy_exception():
    ws = mock_workspace_client(
        cluster_ids=['policy-azure-oauth'],
    )
    ws.cluster_policies.get = MagicMock(side_effect=InternalError(...))

    with pytest.raises(DatabricksError):
        AzureServicePrincipalCrawler(ws, MockBackend(), "ucx").snapshot()


def test_cluster_init_script():
    ws = mock_workspace_client(cluster_ids=['init-scripts-dbfs'])
    ws.dbfs.read().data = "JXNoCmVjaG8gIj0="
    init_crawler = ClustersCrawler(ws, MockBackend(), "ucx").snapshot()
    assert len(init_crawler) == 1


def test_cluster_file_init_script():
    ws = mock_workspace_client(cluster_ids=['init-scripts-file'])
    with patch("builtins.open", mock_open(read_data="data")):
        init_crawler = ClustersCrawler(ws, MockBackend(), "ucx").snapshot()
        assert len(init_crawler) == 1


def test_cluster_no_match_file_init_script():
    ws = mock_workspace_client(cluster_ids=['init-scripts-no-match'])
    init_crawler = ClustersCrawler(ws, MockBackend(), "ucx").snapshot()
    assert len(init_crawler) == 1


def test_cluster_init_script_check_dbfs():
    ws = mock_workspace_client(cluster_ids=['init-scripts-dbfs'])
    ws.dbfs.read().data = "JXNoCmVjaG8gIj0="
    init_crawler = ClustersCrawler(ws, MockBackend(), "ucx").snapshot()
    assert len(init_crawler) == 1


def test_cluster_without_owner_should_have_empty_creator_name():
    ws = mock_workspace_client()
    ws.clusters.list.return_value = (
        ClusterDetails(
            creator_user_name=None,
            cluster_id="simplest-autoscale",
            policy_id="single-user-with-spn",
            cluster_name="Simplest Shard Autoscale",
            spark_version="13.3.x-cpu-ml-scala2.12",
        ),
        ClusterDetails(
            creator_user_name="",
            cluster_id="another-simple-autoscale",
            policy_id="single-user-with-spn",
            cluster_name="Another Simple Shard Autoscale",
            spark_version="13.3.x-cpu-ml-scala2.12",
        ),
    )
    mockbackend = MockBackend()
    ClustersCrawler(ws, mockbackend, "ucx").snapshot()
    result = mockbackend.rows_written_for("hive_metastore.ucx.clusters", "overwrite")
    assert [row["creator"] for row in result] == [None, None]


def test_cluster_with_multiple_failures():
    ws = mock_workspace_client(cluster_ids=['passthrough'])
    crawler = ClustersCrawler(ws, MockBackend(), "ucx")
    result_set = list(crawler.snapshot())
    assert len(result_set) == 1
    assert result_set[0].success == 0
    failures = json.loads(result_set[0].failures)
    assert 'Uses passthrough config: spark.databricks.passthrough.enabled in cluster.' in failures
    assert 'not supported DBR: 9.3.x-cpu-ml-scala2.12' in failures


def test_cluster_with_job_source():
    ws = mock_workspace_client(cluster_ids=['job-cluster', 'policy-azure-oauth'])
    crawler = ClustersCrawler(ws, MockBackend(), "ucx")
    result_set = list(crawler.snapshot())

    assert len(result_set) == 1
    assert result_set[0].cluster_id == "0123-190044-1122334411"


def test_try_fetch():
    ws = mock_workspace_client(cluster_ids=['simplest-autoscale'])
    mock_backend = create_autospec(SqlBackend)
    mock_backend.fetch.return_value = [("000", 1, "123")]
    crawler = ClustersCrawler(ws, mock_backend, "ucx")
    result_set = list(crawler.snapshot())

    assert len(result_set) == 1
    assert result_set[0].cluster_id == "000"
    assert result_set[0].success == 1
    assert result_set[0].failures == "123"


def test_no_isolation_clusters():
    ws = mock_workspace_client(cluster_ids=['no-isolation'])
    sql_backend = MockBackend()
    crawler = ClustersCrawler(ws, sql_backend, "ucx")
    result_set = list(crawler.snapshot())
    assert len(result_set) == 1
    assert result_set[0].failures == '["No isolation shared clusters not supported in UC"]'


def test_mlr_no_isolation_clusters():
    ws = mock_workspace_client(cluster_ids=['no-isolation-mlr'])
    sql_backend = MockBackend()
    crawler = ClustersCrawler(ws, sql_backend, "ucx")
    result_set = list(crawler.snapshot())
    assert len(result_set) == 1
    expected_failures = (
        '["No isolation shared clusters not supported in UC",'
        + ' "Shared Machine Learning Runtime clusters are not supported in UC"]'
    )
    assert result_set[0].failures == expected_failures


def test_unsupported_clusters():
    ws = mock_workspace_client(cluster_ids=['legacy-passthrough'])
    sql_backend = MockBackend()
    crawler = ClustersCrawler(ws, sql_backend, "ucx")
    result_set = list(crawler.snapshot())
    assert len(result_set) == 1
    assert result_set[0].failures == '["cluster type not supported : LEGACY_PASSTHROUGH"]'


def test_cluster_owner_creator(ws) -> None:
    admin_locator = create_autospec(AdministratorLocator)
    type(admin_locator).workspace_administrator = PropertyMock()

    ownership = ClusterOwnership(ws, admin_locator)
    owner = ownership.owner_of(ClusterInfo(creator="bob", cluster_id="1", success=1, failures="[]"))

    assert owner == "bob"
    admin_locator.workspace_administrator.assert_not_called()


def test_cluster_owner_creator_unknown(ws) -> None:
    admin_locator = create_autospec(AdministratorLocator)  # pylint disable=mock-no-usage
    mock_workspace_administrator = PropertyMock(return_value="an_admin")
    type(admin_locator).workspace_administrator = mock_workspace_administrator

    ownership = ClusterOwnership(ws, admin_locator)
    owner = ownership.owner_of(ClusterInfo(creator=None, cluster_id="1", success=1, failures="[]"))

    assert owner == "an_admin"
    mock_workspace_administrator.assert_called_once()


def test_policy_crawler():
    ws = mock_workspace_client(
        policy_ids=['single-user-with-spn', 'single-user-with-spn-policyid', 'single-user-with-spn-no-sparkversion'],
    )

    sql_backend = MockBackend()
    crawler = PoliciesCrawler(ws, sql_backend, "ucx")
    result_set = list(crawler.snapshot())
    failures = json.loads(result_set[0].failures)
    assert len(result_set) == 2
    assert "Uses azure service principal credentials config in policy." in failures


def test_policy_try_fetch():
    ws = mock_workspace_client(policy_ids=['single-user-with-spn-policyid'])
    mock_backend = MockBackend(
        rows={
            r"SELECT \* FROM hive_metastore.ucx.policies": [
                (
                    "single-user-with-spn-policyid",
                    "test_policy",
                    1,
                    "[]",
                    json.dumps({"type": "unlimited", "defaultValue": "auto:latest-ml"}),
                    "test",
                    "test_creator",
                )
            ]
        }
    )
    crawler = PoliciesCrawler(ws, mock_backend, "ucx")
    result_set = list(crawler.snapshot())

    assert len(result_set) == 1
    assert result_set[0].policy_id == "single-user-with-spn-policyid"
    assert result_set[0].policy_name == "test_policy"
    assert result_set[0].spark_version == json.dumps({"type": "unlimited", "defaultValue": "auto:latest-ml"})
    assert result_set[0].policy_description == "test"
    assert result_set[0].creator == "test_creator"


def test_policy_without_failure():
    ws = mock_workspace_client(
        policy_ids=['single-user-with-spn-no-sparkversion'],
    )

    crawler = PoliciesCrawler(ws, MockBackend(), "ucx")
    result_set = list(crawler.snapshot())
    assert result_set[0].failures == '[]'

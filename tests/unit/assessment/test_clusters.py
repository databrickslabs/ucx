from unittest.mock import Mock, create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError, InternalError, NotFound
from databricks.sdk.service.compute import (
    AutoScale,
    ClusterDetails,
    ClusterSource,
    DbfsStorageInfo,
    InitScriptInfo,
    WorkspaceStorageInfo,
)

from databricks.labs.ucx.assessment.azure import AzureServicePrincipalCrawler
from databricks.labs.ucx.assessment.clusters import ClusterInfo, ClustersCrawler

from ..framework.mocks import MockBackend
from . import workspace_client_mock


def test_cluster_assessment(mocker):
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
        ClusterDetails(
            cluster_name="Tech Summit FY24 Cluster",
            autoscale=AutoScale(min_workers=1, max_workers=6),
            spark_conf={
                "spark.hadoop.fs.azure.account."
                "oauth2.client.id.abcde.dfs.core.windows.net": "{{secrets/abcff/sp_app_client_id}}",
                "spark.hadoop.fs.azure.account."
                "oauth2.client.endpoint.abcde.dfs.core.windows.net": "https://login.microsoftonline.com/dedededede"
                "/token",
                "spark.hadoop.fs.azure.account."
                "oauth2.client.secret.abcde.dfs.core.windows.net": "{{secrets/abcff/sp_secret}}",
            },
            spark_context_id=5134472582179565315,
            spark_env_vars=None,
            spark_version="13.3.x-cpu-ml-scala2.12",
            cluster_id="0915-190044-3dqy6751",
        ),
        ClusterDetails(
            cluster_name="Tech Summit FY24 Cluster-1",
            autoscale=AutoScale(min_workers=1, max_workers=6),
            spark_context_id=5134472582179565315,
            spark_env_vars=None,
            policy_id="D96308F1BF0003A7",
            spark_version="13.3.x-cpu-ml-scala2.12",
            cluster_id="0915-190044-3dqy6751",
        ),
    ]

    ws = Mock()

    ws.cluster_policies.get().definition = (
        '{\n  "spark_conf.fs.azure.account.auth.type": {\n    '
        '"type": "fixed",\n    "value": "OAuth",\n   '
        ' "hidden": true\n  },\n  "spark_conf.fs.azure.account.oauth.provider.type": {\n   '
        ' "type": "fixed",\n    "value": '
        '"org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",\n    '
        '"hidden": true\n  },\n  "spark_conf.fs.azure.account.oauth2.client.id": {\n    '
        '"type": "fixed",\n    "value": "fsfsfsfsffsfsf",\n    "hidden": true\n  },\n  '
        '"spark_conf.fs.azure.account.oauth2.client.secret": {\n    "type": "fixed",\n    '
        '"value": "gfgfgfgfggfggfgfdds",\n    "hidden": true\n  },\n  '
        '"spark_conf.fs.azure.account.oauth2.client.endpoint": {\n    '
        '"type": "fixed",\n    '
        '"value": "https://login.microsoftonline.com/1234ededed/oauth2/token",\n    '
        '"hidden": true\n  }\n}'
    )
    ws.cluster_policies.get().policy_family_definition_overrides = (
        '{\n  "spark_conf.fs.azure.account.auth.type": {\n    '
        '"type": "fixed",\n    "value": "OAuth",\n   '
        ' "hidden": true\n  },\n  "spark_conf.fs.azure.account.oauth.provider.type": {\n   '
        ' "type": "fixed",\n    "value": '
        '"org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",\n    '
        '"hidden": true\n  },\n  "spark_conf.fs.azure.account.oauth2.client.id": {\n    '
        '"type": "fixed",\n    "value": "fsfsfsfsffsfsf",\n    "hidden": true\n  },\n  '
        '"spark_conf.fs.azure.account.oauth2.client.secret": {\n    "type": "fixed",\n    '
        '"value": "gfgfgfgfggfggfgfdds",\n    "hidden": true\n  },\n  '
        '"spark_conf.fs.azure.account.oauth2.client.endpoint": {\n    '
        '"type": "fixed",\n    '
        '"value": "https://login.microsoftonline.com/1234ededed/oauth2/token",\n    '
        '"hidden": true\n  }\n}'
    )

    crawler = ClustersCrawler(ws, MockBackend(), "ucx")._assess_clusters(sample_clusters)
    result_set = list(crawler)

    assert len(result_set) == 4
    assert result_set[0].success == 1
    assert result_set[1].success == 0
    assert result_set[2].success == 0
    assert result_set[3].success == 0


def test_cluster_assessment_cluster_policy_no_spark_conf():
    ws = workspace_client_mock(clusters="no-spark-conf.json")
    crawler = ClustersCrawler(ws, MockBackend(), "ucx")
    result_set1 = list(crawler.snapshot())
    assert len(result_set1) == 1
    assert result_set1[0].success == 1


def test_cluster_assessment_cluster_policy_not_found(mocker):
    sample_clusters1 = [
        ClusterDetails(
            cluster_name="cluster1",
            autoscale=AutoScale(min_workers=1, max_workers=6),
            spark_context_id=5134472582179565315,
            spark_env_vars=None,
            policy_id="D96308F1BF0003A8",
            spark_version="13.3.x-cpu-ml-scala2.12",
            cluster_id="0915-190044-3dqy6751",
        )
    ]
    ws = Mock()
    ws.cluster_policies.get.side_effect = NotFound("NO_POLICY")
    clusters_crawler = ClustersCrawler(ws, MockBackend(), "ucx")
    crawler = clusters_crawler._assess_clusters(sample_clusters1)
    results = list(crawler)
    assert len(results) == 1


def test_cluster_assessment_cluster_policy_exception(mocker):
    sample_clusters1 = [
        ClusterDetails(
            cluster_name="cluster1",
            autoscale=AutoScale(min_workers=1, max_workers=6),
            spark_context_id=5134472582179565315,
            spark_env_vars=None,
            policy_id="D96308F1BF0003A8",
            spark_version="13.3.x-cpu-ml-scala2.12",
            cluster_id="0915-190044-3dqy6751",
        )
    ]
    ws = Mock()
    ws.cluster_policies.get.side_effect = InternalError(...)
    crawler = ClustersCrawler(ws, MockBackend(), "ucx")._assess_clusters(sample_clusters1)

    with pytest.raises(DatabricksError):
        list(crawler)


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
            policy_id="bdqwbdqiwd1111",
        )
    ]
    ws = mocker.Mock()
    ws.clusters.list.return_value = sample_clusters
    ws.cluster_policies.get.side_effect = NotFound("NO_POLICY")
    crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx")._list_all_cluster_with_spn_in_spark_conf()
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
        AzureServicePrincipalCrawler(ws, MockBackend(), "ucx")._list_all_cluster_with_spn_in_spark_conf()


def test_azure_spn_info_without_matching_spark_conf(mocker):
    sample_clusters = [
        ClusterDetails(
            autoscale=AutoScale(min_workers=1, max_workers=6),
            cluster_source=ClusterSource.UI,
            spark_conf={"spark.databricks.delta.preview.enabled": "true"},
            spark_context_id=5134472582179565315,
            spark_env_vars=None,
            spark_version="9.3.x-cpu-ml-scala2.12",
            cluster_id="0810-225833-atlanta69",
            cluster_name="Tech Summit FY24 Cluster-1",
        )
    ]
    sample_spns = [{}]
    ws = mocker.Mock()
    ws.clusters.list.return_value = sample_clusters
    ws.cluster_policies.get().policy_family_definition_overrides = None
    AzureServicePrincipalCrawler(ws, MockBackend(), "ucx")._list_all_cluster_with_spn_in_spark_conf()
    crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx")._assess_service_principals(sample_spns)
    result_set = list(crawler)

    assert len(result_set) == 1
    assert result_set[0].application_id is None


def test_azure_spn_info_without_spark_conf(mocker):
    sample_clusters = [
        ClusterDetails(
            autoscale=AutoScale(min_workers=1, max_workers=6),
            cluster_source=ClusterSource.UI,
            spark_context_id=5134472582179565315,
            spark_env_vars=None,
            spark_version="9.3.x-cpu-ml-scala2.12",
            cluster_id="0810-225833-atlanta69",
            cluster_name="Tech Summit FY24 Cluster-1",
        )
    ]
    sample_spns = [{}]
    ws = mocker.Mock()
    ws.clusters.list.return_value = sample_clusters
    ws.cluster_policies.get().policy_family_definition_overrides = None
    AzureServicePrincipalCrawler(ws, MockBackend(), "ucx")._list_all_cluster_with_spn_in_spark_conf()
    crawler = AzureServicePrincipalCrawler(ws, MockBackend(), "ucx")._assess_service_principals(sample_spns)
    result_set = list(crawler)

    assert len(result_set) == 1
    assert result_set[0].application_id is None


def test_cluster_init_script(mocker):
    sample_clusters = [
        ClusterDetails(
            autoscale=AutoScale(min_workers=1, max_workers=6),
            cluster_source=ClusterSource.UI,
            spark_context_id=5134472582179565315,
            spark_env_vars=None,
            spark_version="12.3.x-cpu-ml-scala2.12",
            cluster_id="0810-225833-atlanta69",
            cluster_name="Tech Summit FY24 Cluster-1",
            init_scripts=[
                InitScriptInfo(
                    dbfs=DbfsStorageInfo(destination="dbfs:/users/test@test.com/init_scripts/test.sh"),
                    s3=None,
                    volumes=None,
                    workspace=None,
                ),
                InitScriptInfo(
                    dbfs=None,
                    s3=None,
                    volumes=None,
                    workspace=WorkspaceStorageInfo(
                        destination="/Users/dipankar.kushari@databricks.com/init_script_1.sh"
                    ),
                ),
            ],
        )
    ]
    ws = mocker.Mock()
    ws.clusters.list.return_value = sample_clusters
    ws.dbfs.read().data = "JXNoCmVjaG8gIj0="
    ws.workspace.export().content = (
        "IyEvYmluL2Jhc2gKCiMgU2V0IGEg"
        "Y3VzdG9tIFNwYXJrIGNvbmZpZ3VyYXRpb24KZWNobyAic3Bhc"
        "msuZXhlY3V0b3IubWVtb3J5IDRnIiA+PiAvZGF0YWJyaWNrcy9"
        "zcGFyay9jb25mL3NwYXJrLWRlZmF1bHRzLmNvbmYKZWNobyAic3Bhc"
        "msuZHJpdmVyLm1lbW9yeSAyZyIgPj4gL2RhdGFicmlja3Mvc3BhcmsvY2"
        "9uZi9zcGFyay1kZWZhdWx0cy5jb25mCmVjaG8gInNwYXJrLmhhZG9vcC5mcy"
        "5henVyZS5hY2NvdW50LmF1dGgudHlwZS5hYmNkZS5kZnMuY29yZS53aW5kb3d"
        "zLm5ldCBPQXV0aCIgPj4gL2RhdGFicmlja3Mvc3BhcmsvY29uZi9zcGFyay1kZWZ"
        "hdWx0cy5jb25mCmVjaG8gInNwYXJrLmhhZG9vcC5mcy5henVyZS5hY2NvdW50Lm9"
        "hdXRoLnByb3ZpZGVyLnR5cGUuYWJjZGUuZGZzLmNvcmUud2luZG93cy5uZXQgb3JnLmF"
        "wYWNoZS5oYWRvb3AuZnMuYXp1cmViZnMub2F1dGgyLkNsaWVudENyZWRzVG9rZW5Qcm92"
        "aWRlciIgPj4gL2RhdGFicmlja3Mvc3BhcmsvY29uZi9zcGFyay1kZWZhdWx0cy5jb25mC"
        "mVjaG8gInNwYXJrLmhhZG9vcC5mcy5henVyZS5hY2NvdW50Lm9hdXRoMi5jbGllbnQuaWQ"
        "uYWJjZGUuZGZzLmNvcmUud2luZG93cy5uZXQgZHVtbXlfYXBwbGljYXRpb25faWQiID4+IC"
        "9kYXRhYnJpY2tzL3NwYXJrL2NvbmYvc3BhcmstZGVmYXVsdHMuY29uZgplY2hvICJzcGFya"
        "y5oYWRvb3AuZnMuYXp1cmUuYWNjb3VudC5vYXV0aDIuY2xpZW50LnNlY3JldC5hYmNkZS5kZnMu"
        "Y29yZS53aW5kb3dzLm5ldCBkZGRkZGRkZGRkZGRkZGRkZGRkIiA+PiAvZGF0YWJyaWNrcy9zcGFy"
        "ay9jb25mL3NwYXJrLWRlZmF1bHRzLmNvbmYKZWNobyAic3BhcmsuaGFkb29wLmZzLmF6dXJlLmFj"
        "Y291bnQub2F1dGgyLmNsaWVudC5lbmRwb2ludC5hYmNkZS5kZnMuY29yZS53aW5kb3dzLm5ldCBod"
        "HRwczovL2xvZ2luLm1pY3Jvc29mdG9ubGluZS5jb20vZHVtbXlfdGVuYW50X2lkL29hdXRoMi90b2t"
        "lbiIgPj4gL2RhdGFicmlja3Mvc3BhcmsvY29uZi9zcGFyay1kZWZhdWx0cy5jb25mCg=="
    )
    init_crawler = ClustersCrawler(ws, MockBackend(), "ucx").snapshot()
    assert len(init_crawler) == 1


def test_cluster_init_script_check_dbfs(mocker):
    sample_clusters = [
        ClusterDetails(
            autoscale=AutoScale(min_workers=1, max_workers=6),
            cluster_source=ClusterSource.UI,
            spark_context_id=5134472582179565315,
            spark_env_vars=None,
            spark_version="12.3.x-cpu-ml-scala2.12",
            cluster_id="0810-225833-atlanta69",
            cluster_name="Tech Summit FY24 Cluster-1",
            init_scripts=[
                InitScriptInfo(
                    dbfs=DbfsStorageInfo(destination="dbfs:"),
                    s3=None,
                    volumes=None,
                    workspace=None,
                ),
                InitScriptInfo(
                    dbfs=DbfsStorageInfo(destination="dbfs"),
                    s3=None,
                    volumes=None,
                    workspace=None,
                ),
                InitScriptInfo(
                    dbfs=DbfsStorageInfo(destination=":/users/test@test.com/init_scripts/test.sh"),
                    s3=None,
                    volumes=None,
                    workspace=None,
                ),
                InitScriptInfo(
                    dbfs=None,
                    s3=None,
                    volumes=None,
                    workspace=WorkspaceStorageInfo(
                        destination="/Users/dipankar.kushari@databricks.com/init_script_1.sh"
                    ),
                ),
            ],
        )
    ]
    ws = create_autospec(WorkspaceClient)
    ws.clusters.list.return_value = sample_clusters
    ws.dbfs.read().data = "JXNoCmVjaG8gIj0="
    ws.workspace.export().content = "JXNoCmVjaG8gIj0="
    init_crawler = ClustersCrawler(ws, MockBackend(), "ucx").snapshot()
    assert len(init_crawler) == 1


def test_cluster_without_owner_should_have_empty_creator_name(mocker):
    sample_clusters = [
        ClusterDetails(
            autoscale=AutoScale(min_workers=1, max_workers=6),
            cluster_source=ClusterSource.UI,
            spark_context_id=5134472582179565315,
            spark_env_vars=None,
            spark_version="12.3.x-cpu-ml-scala2.12",
            cluster_id="0810-225833-atlanta69",
            cluster_name="Tech Summit FY24 Cluster-1",
        )
    ]
    ws = mocker.Mock()
    mockbackend = MockBackend()

    ws.clusters.list.return_value = sample_clusters
    ClustersCrawler(ws, mockbackend, "ucx").snapshot()
    result = mockbackend.rows_written_for("hive_metastore.ucx.clusters", "append")
    assert result == [
        ClusterInfo(
            cluster_id="0810-225833-atlanta69",
            cluster_name="Tech Summit FY24 Cluster-1",
            creator=None,
            success=1,
            failures="[]",
        )
    ]


def test_cluster_with_multiple_failures():
    sample_clusters = [
        ClusterDetails(
            autoscale=AutoScale(min_workers=1, max_workers=6),
            cluster_source=ClusterSource.UI,
            spark_context_id=5134472582179565315,
            spark_conf={
                "spark.databricks.passthrough.enabled":"True"
            },
            spark_env_vars=None,
            spark_version="9.3.x-cpu-ml-scala2.12",
            cluster_id="0810-225833-atlanta69",
            cluster_name="Tech Summit FY24 Cluster-1",
        )
    ]
    ws = create_autospec(WorkspaceClient)
    mockbackend = MockBackend()

    ws.clusters.list.return_value = sample_clusters
    ClustersCrawler(ws, mockbackend, "ucx").snapshot()
    result = mockbackend.rows_written_for("hive_metastore.ucx.clusters", "append")
    assert result == [
        ClusterInfo(
            cluster_id="0810-225833-atlanta69",
            cluster_name="Tech Summit FY24 Cluster-1",
            creator=None,
            success=0,
            failures='["not supported DBR: 9.3.x-cpu-ml-scala2.12", "unsupported config: spark.databricks.passthrough.enabled"]'
        )
    ]

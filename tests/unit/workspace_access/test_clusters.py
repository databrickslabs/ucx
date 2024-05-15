from unittest.mock import create_autospec

from databricks.labs.blueprint.installation import MockInstallation
from databricks.labs.blueprint.tui import MockPrompts
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import ClusterDetails, ClusterSource, DataSecurityMode

from databricks.labs.ucx.workspace_access.clusters import ClusterAccess


def test_map_cluster_to_uc(caplog):
    ws = create_autospec(WorkspaceClient)
    cluster_details = [
        ClusterDetails(
            cluster_id="123", cluster_name="test_cluster", data_security_mode=DataSecurityMode.LEGACY_SINGLE_USER
        )
    ]
    prompts = MockPrompts({})
    installation = MockInstallation()
    cluster = ClusterAccess(installation, ws, prompts)
    with caplog.at_level('INFO'):
        cluster.map_cluster_to_uc(cluster_id="123", cluster_details=cluster_details)
        assert 'Editing the cluster of cluster: 123 with access_mode as DataSecurityMode.SINGLE_USER' in caplog.messages
    ws.clusters.edit.assert_called_once()
    ws.clusters.list.assert_not_called()


def test_map_cluster_to_uc_shared(caplog):
    ws = create_autospec(WorkspaceClient)
    ws.clusters.list.return_value = [
        ClusterDetails(
            cluster_id="123",
            cluster_name="test_cluster",
            cluster_source=ClusterSource.UI,
            data_security_mode=DataSecurityMode.LEGACY_TABLE_ACL,
        ),
        ClusterDetails(cluster_id="1234", cluster_name="test_cluster", cluster_source=ClusterSource.JOB),
    ]
    cluster_details = [
        ClusterDetails(
            cluster_id="123",
            cluster_name="test_cluster",
            cluster_source=ClusterSource.UI,
            data_security_mode=DataSecurityMode.LEGACY_TABLE_ACL,
        ),
        ClusterDetails(cluster_id="1234", cluster_name="test_cluster", cluster_source=ClusterSource.JOB),
    ]
    prompts = MockPrompts({})
    installation = MockInstallation()
    cluster = ClusterAccess(installation, ws, prompts)
    with caplog.at_level('INFO'):
        cluster.map_cluster_to_uc(cluster_id="<ALL>", cluster_details=cluster_details)
        assert (
            'Editing the cluster of cluster: 123 with access_mode as DataSecurityMode.USER_ISOLATION' in caplog.messages
        )
    ws.clusters.edit.assert_called_once()
    ws.clusters.list.assert_not_called()


def test_list_clusters():
    ws = create_autospec(WorkspaceClient)
    ws.clusters.list.return_value = [
        ClusterDetails(cluster_id="123", cluster_name="test_cluster", cluster_source=ClusterSource.UI),
        ClusterDetails(cluster_id="1234", cluster_name="test_cluster1", cluster_source=ClusterSource.JOB),
    ]
    prompts = MockPrompts({})
    installation = MockInstallation()
    cluster = ClusterAccess(installation, ws, prompts)
    cluster_list = cluster.list_cluster()
    assert cluster_list[0].cluster_id == "123"
    assert len(cluster_list) == 1
    ws.clusters.edit.assert_not_called()
    ws.clusters.list.assert_called_once()


def test_map_cluster_to_uc_error(caplog):
    ws = create_autospec(WorkspaceClient)
    cluster_details = [ClusterDetails(cluster_id="123", cluster_name="test_cluster")]
    prompts = MockPrompts({})
    installation = MockInstallation()
    cluster = ClusterAccess(installation, ws, prompts)
    with caplog.at_level('INFO'):
        cluster.map_cluster_to_uc(cluster_id="123", cluster_details=cluster_details)
        assert 'skipping cluster remapping: Data security Mode is None for the cluster 123' in caplog.messages
    ws.clusters.edit.assert_not_called()
    ws.clusters.list.assert_not_called()


def test_revert_map_cluster_to_uc(caplog):
    ws = create_autospec(WorkspaceClient)
    installation = MockInstallation(
        {
            "backup/clusters/123.json": {
                "cluster_id": "123",
                "cluster_name": "test_cluster",
                "spark_version": "13.3.x-cpu-ml-scala2.12",
            },
        }
    )
    prompts = MockPrompts({})
    cluster = ClusterAccess(installation, ws, prompts)
    cluster.revert_cluster_remap(cluster_ids="123", total_cluster_ids=["123"])
    ws.clusters.edit.assert_called_once()
    ws.clusters.list.assert_not_called()


def test_revert_all_cluster_to_uc(caplog):
    ws = create_autospec(WorkspaceClient)
    installation = MockInstallation(
        {
            "backup/clusters/123.json": {
                "cluster_id": "123",
                "cluster_name": "test_cluster",
            },
            "backup/clusters/234.json": {
                "cluster_id": "234",
                "cluster_name": "test_cluster",
            },
        }
    )
    prompts = MockPrompts({})
    cluster = ClusterAccess(installation, ws, prompts)
    with caplog.at_level('INFO'):
        cluster.revert_cluster_remap(cluster_ids="<ALL>", total_cluster_ids=["123", "234"])
        assert "Reverting the configurations for the cluster ['123', '234']" in caplog.messages
    ws.clusters.edit.assert_not_called()
    ws.clusters.list.assert_not_called()


def test_revert_cluster_to_uc_empty_cluster(caplog):
    ws = create_autospec(WorkspaceClient)
    installation = MockInstallation(
        {
            "backup/clusters/123.json": {
                "cluster_name": "test_cluster",
                "spark_version": "13.3.x-cpu-ml-scala2.12",
            },
        }
    )
    prompts = MockPrompts({})
    cluster = ClusterAccess(installation, ws, prompts)
    with caplog.at_level('INFO'):
        cluster.revert_cluster_remap(cluster_ids="123", total_cluster_ids=["123"])
        assert (
            'skipping cluster remapping: cluster Id is not present in the config file for the cluster:123'
            in caplog.messages
        )
    ws.clusters.edit.assert_not_called()
    ws.clusters.list.assert_not_called()

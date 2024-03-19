from unittest.mock import create_autospec

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.tui import MockPrompts
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import ClusterDetails

from databricks.labs.ucx.workspace_access.clusters import ClusterAccess


def test_map_cluster_to_uc():
    ws = create_autospec(WorkspaceClient)
    ws.clusters.get.return_value = ClusterDetails(cluster_id="123", cluster_name="test_cluster")
    prompts = MockPrompts({})
    installation = create_autospec(Installation)
    installation.save.return_value = "a/b/c"
    cluster = ClusterAccess(installation, ws, prompts)
    cluster.map_cluster_to_uc(cluster_id="123")


def test_map_cluster_to_uc_error(caplog):
    ws = create_autospec(WorkspaceClient)
    ws.clusters.get.return_value = ClusterDetails(cluster_id="123", cluster_name="test_cluster")
    prompts = MockPrompts({})
    installation = create_autospec(Installation)
    installation.save.return_value = "a/b/c"
    cluster = ClusterAccess(installation, ws, prompts)
    with caplog.at_level('INFO'):
        cluster.map_cluster_to_uc("123")
        assert 'Data security Mode is None. Skipping the remapping for the cluster: 123' in caplog.messages


def test_revert_map_cluster_to_uc(caplog):
    ws = create_autospec(WorkspaceClient)
    installation = create_autospec(Installation)
    prompts = MockPrompts({})
    installation.load.return_value = ClusterDetails(
        cluster_id="123", cluster_name="test_cluster", spark_version="13.3.x-cpu-ml-scala2.12"
    )
    cluster = ClusterAccess(installation, ws, prompts)
    cluster.revert_cluster_remap(cluster_ids="123", total_cluster_ids=["123"])

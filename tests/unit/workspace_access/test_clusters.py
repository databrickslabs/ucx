from unittest.mock import create_autospec

from databricks.labs.blueprint.tui import MockPrompts
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.workspace_access.clusters import ClusterAccess


def test_map_cluster_to_uc():
    ws = create_autospec(WorkspaceClient)
    cluster_id = "test_id"
    prompts = MockPrompts(
        {
            "Single User": "1",
            "Shared": "1",
        }
    )
    cluster = ClusterAccess(prompts, ws)
    cluster.map_cluster_to_uc(cluster_id=cluster_id)


def test_map_cluster_to_uc_error(caplog):
    ws = create_autospec(WorkspaceClient)
    prompts = MockPrompts(
        {
            "Single User": "1",
            "Shared": "1",
        }
    )
    cluster = ClusterAccess(prompts, ws)
    with caplog.at_level('WARNING'):
        cluster.map_cluster_to_uc()
        assert (
            'skipping cluster remapping: Cluster Id is not Provided. Please provide the cluster id' in caplog.messages
        )

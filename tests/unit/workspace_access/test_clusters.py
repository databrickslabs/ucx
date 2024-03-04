from databricks.labs.blueprint.tui import MockPrompts
from databricks.sdk import WorkspaceClient
from databricks.labs.ucx.workspace_access.clusters import ClusterAccess
from unittest.mock import create_autospec


def test_map_cluster_to_uc():
    ws = create_autospec(WorkspaceClient)
    cluster_id = "test_id"
    prompts = MockPrompts({
        "Single User": "1",
        "Shared": "1",
    })
    cluster = ClusterAccess(prompts, ws)
    cluster.map_cluster_to_uc(cluster_id=cluster_id)



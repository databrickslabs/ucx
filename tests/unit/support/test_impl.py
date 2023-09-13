from unittest.mock import MagicMock

from databricks.labs.ucx.support.impl import SupportsProvider


def test_supports_provider():
    provider = SupportsProvider(ws=MagicMock(), num_threads=1, workspace_start_path="/")
    assert provider.supports.keys() == {
        "entitlements",
        "roles",
        "clusters",
        "cluster-policies",
        "instance-pools",
        "sql/warehouses",
        "jobs",
        "pipelines",
        "experiments",
        "registered-models",
        "tokens",
        "passwords",
        "notebooks",
        "files",
        "directories",
        "repos",
        "alerts",
        "queries",
        "dashboards",
        "secrets",
    }

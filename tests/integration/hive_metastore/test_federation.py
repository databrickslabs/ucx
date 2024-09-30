from databricks.labs.blueprint.commands import CommandExecutor
from databricks.labs.ucx.hive_metastore.federation import DbfsRootResolver


def test_dbfs_root_resolver(ws, env_or_skip):
    command_executor = CommandExecutor(ws.clusters, ws.command_execution, lambda: env_or_skip("DATABRICKS_CLUSTER_ID"))
    root_resolver = DbfsRootResolver(command_executor)
    root = root_resolver.run()
    assert root is not None
    assert root is not "Unknown"

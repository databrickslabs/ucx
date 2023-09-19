import pytest

from databricks.labs.ucx.hive_metastore.list_mounts import Mount
from databricks.labs.ucx.mixins.compute import CommandExecutor


@pytest.mark.skip(reason="Needs to have mountpoints already created ")
def test_mount_listing(ws, wsfs_wheel, make_schema, sql_fetch_all):
    _, inventory_database = make_schema(catalog="hive_metastore").split(".")
    commands = CommandExecutor(ws)
    commands.install_notebook_library(f"/Workspace{wsfs_wheel}")

    commands.run(
        f"""
        from databricks.labs.ucx.hive_metastore.list_mounts import Mounts
        from databricks.labs.ucx.config import MigrationConfig, GroupsConfig, TaclConfig
        from databricks.sdk import WorkspaceClient
        from databricks.labs.ucx.framework.crawlers import RuntimeBackend
        cfg = MigrationConfig(
            inventory_database="{inventory_database}",
            groups=GroupsConfig(auto=True),
            tacl=TaclConfig(databases=["default"]))
        ws = WorkspaceClient(config=cfg.to_databricks_config())
        mounts = Mounts(backend=RuntimeBackend(), ws=ws, inventory_database=cfg.inventory_database)
        mounts.inventorize_mounts()
        """
    )
    mounts = sql_fetch_all(f"SELECT * FROM hive_metastore.{inventory_database}.mounts")
    results = []

    for mount in mounts:
        mount_info = Mount(*mount)
        results.append(mount_info)

    assert len(results) > 0

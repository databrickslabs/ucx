import pytest
from databricks.labs.blueprint.commands import CommandExecutor

from databricks.labs.ucx.hive_metastore.locations import Mount


@pytest.mark.skip(reason="Needs to have mountpoints already created ")
def test_mount_listing(ws, inventory_schema, wsfs_wheel, make_schema, sql_fetch_all):
    commands = CommandExecutor(ws)
    commands.install_notebook_library(f"/Workspace{wsfs_wheel}")

    commands.run(
        f"""
        from databricks.labs.ucx.hive_metastore.list_mounts import Mounts
        from databricks.labs.ucx.config import WorkspaceConfig, GroupsConfig, TaclConfig
        from databricks.sdk import WorkspaceClient
        from databricks.labs.ucx.framework.crawlers import RuntimeBackend
        cfg = WorkspaceConfig(
            inventory_database="{inventory_schema}",
            groups=GroupsConfig(auto=True),
            tacl=TaclConfig(databases=["default"]))
        ws = WorkspaceClient(config=cfg.to_databricks_config())
        mounts = Mounts(backend=RuntimeBackend(), ws=ws, inventory_database=cfg.inventory_database)
        mounts.inventorize_mounts()
        """
    )
    mounts = sql_fetch_all(f"SELECT * FROM hive_metastore.{inventory_schema}.mounts")
    results = []

    for mount in mounts:
        mount_info = Mount(*mount)
        results.append(mount_info)

    assert len(results) > 0

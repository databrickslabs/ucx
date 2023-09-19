from databricks.labs.ucx.mixins.compute import CommandExecutor
from databricks.labs.ucx.mounts.list_mounts import Mount


def test_mount_listing(ws, wsfs_wheel, make_schema, sql_fetch_all):
    # TODO: create a mount point in a freshly created bucket instead of leveraging existing mount point
    _, inventory_database = make_schema(catalog="hive_metastore").split(".")
    commands = CommandExecutor(ws)
    commands.install_notebook_library(f"/Workspace{wsfs_wheel}")

    commands.run(
        f"""
        from databricks.labs.ucx.mounts import MountMigrationToolKit
        from databricks.labs.ucx.config import MigrationConfig, GroupsConfig, TaclConfig
        cfg = MigrationConfig(
            inventory_database="{inventory_database}",
            groups=GroupsConfig(auto=True),
            tacl=TaclConfig(databases=["default"]))
        MountMigrationToolKit(cfg).inventorize_mounts()
        """
    )
    mounts = sql_fetch_all(f"SELECT * FROM hive_metastore.{inventory_database}.mounts")
    results = []

    for mount in mounts:
        mount_info = Mount(*mount)
        results.append(mount_info)

    assert len(results) > 0

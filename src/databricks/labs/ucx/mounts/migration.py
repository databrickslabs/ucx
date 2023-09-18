from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.config import MigrationConfig
from databricks.labs.ucx.framework.crawlers import RuntimeBackend
from databricks.labs.ucx.mounts.list_mounts import Mounts


class MountMigrationToolKit:
    def __init__(self, config: MigrationConfig):
        self._config = config

    def inventorize_mounts(self):
        instance = Mounts(
            RuntimeBackend(),
            WorkspaceClient(config=self._config.to_databricks_config()),
            self._config.inventory_database,
        )
        instance.inventorize_mounts()

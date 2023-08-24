import sys

from databricks.labs.ucx.config import MigrationConfig
from databricks.labs.ucx.inventory.inventorizer import Crawlers
from databricks.labs.ucx.inventory.permissions import PermissionManager
from databricks.labs.ucx.inventory.workspace import WorkspaceInventory
from databricks.labs.ucx.managers.group import GroupManager
from databricks.labs.ucx.providers.client import ImprovedWorkspaceClient
from databricks.labs.ucx.providers.logger import logger


class GroupMigrationToolkit:
    def __init__(self, config: MigrationConfig):
        self._num_threads = config.num_threads

        databricks_config = config.to_databricks_config()
        self._configure_logger(config.log_level)

        self._ws = ImprovedWorkspaceClient(config=databricks_config)
        self._ws.api_client._session.adapters["https://"].max_retries.total = 20
        self._verify_ws_client(self._ws)

        self.group_manager = GroupManager(self._ws, config.groups)
        self._workspace_inventory = WorkspaceInventory(config.inventory, self._ws)
        self.permissions_manager = PermissionManager(self._ws, self._workspace_inventory)
        self._crawlers = Crawlers(self._ws, self.group_manager.migration_groups_provider, self._num_threads)

    @staticmethod
    def _verify_ws_client(w: ImprovedWorkspaceClient):
        _me = w.current_user.me()
        is_workspace_admin = any(g.display == "admins" for g in _me.groups)
        if not is_workspace_admin:
            msg = "Current user is not a workspace admin"
            raise RuntimeError(msg)

    @staticmethod
    def _configure_logger(level: str):
        logger.remove()  # TODO: why removing loggers?
        logger.add(sys.stderr, level=level)

    def prepare_environment(self):
        self.group_manager.prepare_groups_in_environment()

    def cleanup_inventory_table(self):
        self._workspace_inventory.cleanup()

    def inventorize_permissions(self):
        self.permissions_manager.inventorize_permissions(self._crawlers)

    def apply_permissions_to_backup_groups(self):
        self.permissions_manager.apply_group_permissions(
            self.group_manager.migration_groups_provider, destination="backup"
        )

    def replace_workspace_groups_with_account_groups(self):
        self.group_manager.replace_workspace_groups_with_account_groups()

    def apply_permissions_to_account_groups(self):
        self.permissions_manager.apply_group_permissions(
            self.group_manager.migration_groups_provider, destination="account"
        )

    def delete_backup_groups(self):
        self.group_manager.delete_backup_groups()

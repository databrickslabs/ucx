import logging

from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.config import MigrationConfig
from databricks.labs.ucx.inventory.permissions import PermissionManager
from databricks.labs.ucx.inventory.permissions_inventory import (
    PermissionsInventoryTable,
)
from databricks.labs.ucx.inventory.verification import VerificationManager
from databricks.labs.ucx.managers.group import GroupManager
from databricks.labs.ucx.supports.impl import SupportsProvider


class GroupMigrationToolkit:
    def __init__(self, config: MigrationConfig):
        self._num_threads = config.num_threads
        self._workspace_start_path = config.workspace_start_path

        databricks_config = config.to_databricks_config()
        self._configure_logger(config.log_level)

        # integrate with connection pool settings properly
        # https://github.com/databricks/databricks-sdk-py/pull/276
        self._ws = WorkspaceClient(config=databricks_config)
        self._ws.api_client._session.adapters["https://"].max_retries.total = 20
        self._verify_ws_client(self._ws)

        self._group_manager = GroupManager(self._ws, config.groups)
        self._permissions_inventory = PermissionsInventoryTable(config.inventory_database, self._ws)
        self._permissions_manager = PermissionManager(
            self._ws,
            self._permissions_inventory,
            supports_provider=SupportsProvider(self._ws, self._num_threads, self._workspace_start_path),
        )
        self._verification_manager = VerificationManager(self._ws)

    @staticmethod
    def _verify_ws_client(w: WorkspaceClient):
        _me = w.current_user.me()
        is_workspace_admin = any(g.display == "admins" for g in _me.groups)
        if not is_workspace_admin:
            msg = "Current user is not a workspace admin"
            raise RuntimeError(msg)

    @staticmethod
    def _configure_logger(level: str):
        ucx_logger = logging.getLogger("databricks.labs.ucx")
        ucx_logger.setLevel(level)

    def prepare_environment(self):
        self._group_manager.prepare_groups_in_environment()

    def cleanup_inventory_table(self):
        self._permissions_inventory.cleanup()

    def inventorize_permissions(self):
        self._permissions_manager.inventorize_permissions()

    def apply_permissions_to_backup_groups(self):
        self._permissions_manager.apply_group_permissions(
            self._group_manager.migration_groups_provider, destination="backup"
        )

    def verify_permissions_on_backup_groups(self, to_verify):
        self._verification_manager.verify(self._group_manager.migration_groups_provider, "backup", to_verify)

    def replace_workspace_groups_with_account_groups(self):
        self._group_manager.replace_workspace_groups_with_account_groups()

    def apply_permissions_to_account_groups(self):
        self._permissions_manager.apply_group_permissions(
            self._group_manager.migration_groups_provider, destination="account"
        )

    def verify_permissions_on_account_groups(self, to_verify):
        self._verification_manager.verify(self._group_manager.migration_groups_provider, "account", to_verify)

    def delete_backup_groups(self):
        self._group_manager.delete_backup_groups()

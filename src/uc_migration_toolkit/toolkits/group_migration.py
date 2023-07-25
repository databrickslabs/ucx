from uc_migration_toolkit.config import MigrationConfig
from uc_migration_toolkit.managers.group import GroupManager
from uc_migration_toolkit.managers.inventory.permissions import PermissionManager
from uc_migration_toolkit.managers.inventory.table import InventoryTableManager
from uc_migration_toolkit.providers.client import provider
from uc_migration_toolkit.providers.config import provider as config_provider


class GroupMigrationToolkit:
    def __init__(self, config: MigrationConfig):
        # please note the order of configs here
        config_provider.set_config(config)
        provider.set_ws_client(config.auth)
        self.group_manager = GroupManager()
        self.table_manager = InventoryTableManager()
        self.permissions_manager = PermissionManager(self.table_manager)

    def prepare_groups_in_environment(self):
        self.group_manager.prepare_groups_in_environment()

    def cleanup_inventory_table(self):
        self.table_manager.cleanup()

    def inventorize_permissions(self):
        self.permissions_manager.inventorize_permissions()

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

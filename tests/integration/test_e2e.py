from conftest import EnvironmentInfo

from uc_migration_toolkit.config import (
    GroupsConfig,
    InventoryConfig,
    InventoryTable,
    MigrationConfig,
)
from uc_migration_toolkit.providers.logger import logger
from uc_migration_toolkit.toolkits.group_migration import GroupMigrationToolkit


def test_e2e(env: EnvironmentInfo, inventory_table: InventoryTable):
    logger.info(f"Test environment: {env.test_uid}")
    config = MigrationConfig(
        with_table_acls=False,
        inventory=InventoryConfig(table=inventory_table),
        groups=GroupsConfig(selected=[g[0].display_name for g in env.groups]),
        auth=None,
    )
    logger.info(f"Starting e2e with config: {config.to_json()}")
    toolkit = GroupMigrationToolkit(config)
    toolkit.validate_groups()
    toolkit.cleanup_inventory_table()
    toolkit.inventorize_permissions()
    toolkit.create_or_update_backup_groups()
    toolkit.apply_backup_group_permissions()
    toolkit.replace_workspace_groups_with_account_groups()
    print("here")

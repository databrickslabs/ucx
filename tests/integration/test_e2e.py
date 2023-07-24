from uc_migration_toolkit.config import (
    GroupsConfig,
    InventoryConfig,
    InventoryTable,
    MigrationConfig,
)
from uc_migration_toolkit.toolkits.group_migration import GroupMigrationToolkit


def test_e2e():
    config = MigrationConfig(
        inventory=InventoryConfig(table=InventoryTable(catalog="main", database="default", name="ucx_inventory")),
        groups=GroupsConfig(selected=["analyst"]),
        with_table_acls=False,
        num_threads=80,
    )
    toolkit = GroupMigrationToolkit(config)

    toolkit.validate_groups()
    toolkit.create_or_update_backup_groups()
    toolkit.cleanup_inventory_table()
    toolkit.inventorize_permissions()

    toolkit.apply_backup_group_permissions()

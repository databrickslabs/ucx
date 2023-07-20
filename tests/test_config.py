from uc_migration_toolkit.config import GroupsConfig, InventoryTable, MigrationConfig


def test_initialization():
    MigrationConfig(
        with_table_acls=False,
        inventory_table=InventoryTable(catalog="test_catalog", database="test_database", name="test_table"),
        groups=GroupsConfig(auto=True),
    )

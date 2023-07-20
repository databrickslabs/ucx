from uc_migration_toolkit.config import (
    GroupListingConfig,
    InventoryTable,
    MigrationConfig,
)


def test_initialization():
    MigrationConfig(
        with_table_acls=False,
        inventory_table=InventoryTable(catalog="test_catalog", database="test_database", table="test_table"),
        group_listing_config=GroupListingConfig(auto=True),
    )

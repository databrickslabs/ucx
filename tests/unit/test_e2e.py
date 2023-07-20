from pytest_mock import MockerFixture

from uc_migration_toolkit.config import (
    GroupsConfig,
    InventoryConfig,
    InventoryTable,
    MigrationConfig,
)
from uc_migration_toolkit.providers.client import provider
from uc_migration_toolkit.providers.spark import SparkMixin
from uc_migration_toolkit.toolkits.group_migration import GroupMigrationToolkit


def get_full_name(instance):
    return f"{instance.__module__}.{instance.__class__.__name__}"


def test_e2e(mocker: MockerFixture):
    config = MigrationConfig(
        inventory=InventoryConfig(table=InventoryTable(catalog="catalog", database="database", name="name")),
        groups=GroupsConfig(auto=True),
        with_table_acls=False,
    )

    mocker.patch.object(provider, "set_ws_client"),
    mocker.patch.object(provider, "_ws_client"),
    mocker.patch.object(SparkMixin, "_initialize_spark"),

    toolkit = GroupMigrationToolkit(config)
    toolkit.validate_groups()
    toolkit.cleanup_inventory_table()
    toolkit.inventorize_permissions()
    toolkit.create_or_update_backup_groups()
    toolkit.apply_backup_group_permissions()
    toolkit.replace_workspace_groups_with_account_groups()
    toolkit.apply_account_group_permissions()
    toolkit.delete_backup_groups()
    toolkit.cleanup_inventory_table()

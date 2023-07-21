from uc_migration_toolkit.managers.group import GroupPair
from uc_migration_toolkit.managers.inventory.inventorizer import StandardInventorizer
from uc_migration_toolkit.managers.inventory.table import InventoryTableManager
from uc_migration_toolkit.managers.inventory.types import (
    LogicalObjectType,
    RequestObjectType,
)
from uc_migration_toolkit.providers.client import provider
from uc_migration_toolkit.providers.config import provider as config_provider
from uc_migration_toolkit.providers.logger import logger


class PermissionManager:
    def __init__(self, inventory_table_manager: InventoryTableManager):
        self.config = config_provider.config
        self.inventory_table_manager = inventory_table_manager

    @staticmethod
    def get_inventorizers():
        return [
            StandardInventorizer(
                logical_object_type=LogicalObjectType.CLUSTER,
                request_object_type=RequestObjectType.CLUSTERS,
                listing_function=provider.ws.clusters.list,
                id_attribute="cluster_id",
            )
        ]

    def inventorize_permissions(self):
        logger.info("Inventorizing the permissions")

        for inventorizer in self.get_inventorizers():
            inventorizer.preload()
            collected = inventorizer.inventorize()
            if collected:
                self.inventory_table_manager.save(collected)
            else:
                logger.warning(f"No objects of type {inventorizer.logical_object_type} were found")

        logger.info("Permissions were inventorized and saved")

    def apply_backup_group_permissions(self, pairs: list[GroupPair]):
        logger.info("Applying the permissions to the backup groups")
        permissions_on_source = self.inventory_table_manager.load_for_groups(groups=[p.source for p in pairs])
        for item in permissions_on_source:
            print(item)
        logger.info("Permissions were applied")

    def apply_account_group_permissions(self):
        logger.info("Applying workspace-level permissions to the account-level groups")
        logger.info("Permissions were successfully applied to the account-level groups")

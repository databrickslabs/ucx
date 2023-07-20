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
        logger.info("Inventorying the permissions")

        for inventorizer in self.get_inventorizers():
            inventorizer.preload()
            collected = inventorizer.inventorize()
            self.inventory_table_manager.save(collected)

        logger.info("Permissions were inventoried and saved")

from uc_migration_toolkit.accessors.generic.accessor import GenericAccessor
from uc_migration_toolkit.accessors.generic.inventory_item import (
    PermissionsInventoryItem,
)
from uc_migration_toolkit.accessors.impl import ClusterAccessor
from uc_migration_toolkit.config import MigrationConfig
from uc_migration_toolkit.providers.client import ClientMixin


class PermissionManager(ClientMixin):
    def __init__(self, config: MigrationConfig):
        super().__init__(config)
        self.config = config
        self._collected: list[PermissionsInventoryItem] = []
        self.object_accessors: list[GenericAccessor] = [ClusterAccessor(self.config)]

    def collect_from(self, accessor: GenericAccessor):
        self.logger.info(f"Collecting permissions from {accessor.object_type}")

        for item in accessor.listing_function():
            self.logger.info(f"Collecting permissions from {accessor.object_type}, object id: {accessor.get_id(item)}")
            permission_item_info = accessor.get_inventory_item(item)
            self._collected.append(permission_item_info)

        self.logger.info(f"Collected permissions from {accessor.object_type}")

    def inventorize_permissions(self):
        self.logger.info("Inventorying the permissions")

        for accessor in self.object_accessors:
            self.collect_from(accessor)

        self.logger.info("Permissions were inventoried and saved")

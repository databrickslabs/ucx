import logging

from databricks.labs.ucx.inventory.types import PermissionsInventoryItem
from databricks.labs.ucx.tacl._internal import CrawlerBase, SqlBackend

logger = logging.getLogger(__name__)


class PermissionsInventoryTable(CrawlerBase):
    def __init__(self, backend: SqlBackend, inventory_database: str):
        super().__init__(backend, "hive_metastore", inventory_database, "permissions")

    def cleanup(self):
        logger.info(f"Cleaning up inventory table {self._full_name}")
        self._exec(f"DROP TABLE IF EXISTS {self._full_name}")
        logger.info("Inventory table cleanup complete")

    def save(self, items: list[PermissionsInventoryItem]):
        # TODO: update instead of append
        logger.info(f"Saving {len(items)} items to inventory table {self._full_name}")
        self._append_records(PermissionsInventoryItem, items)
        logger.info("Successfully saved the items to inventory table")

    def load_all(self) -> list[PermissionsInventoryItem]:
        logger.info(f"Loading inventory table {self._full_name}")
        return [
            PermissionsInventoryItem(object_id, support, raw_object_permissions)
            for object_id, support, raw_object_permissions in self._fetch(
                f"SELECT object_id, support, raw_object_permissions FROM {self._full_name}"
            )
        ]

import pandas as pd

from uc_migration_toolkit.managers.inventory.types import PermissionsInventoryItem
from uc_migration_toolkit.providers.config import provider as config_provider
from uc_migration_toolkit.providers.logger import logger
from uc_migration_toolkit.providers.spark import SparkMixin


class InventoryTableManager(SparkMixin):
    def __init__(self):
        super().__init__()
        self.config = config_provider.config.inventory

    def cleanup(self):
        logger.info(f"Cleaning up inventory name {self.config.table}")
        self.spark.sql(f"DROP TABLE IF EXISTS {self.config.table.to_spark()}")
        logger.info("Inventory name cleanup complete")

    def save(self, items: list[PermissionsInventoryItem]):
        logger.info(f"Saving {len(items)} items to inventory table {self.config.table}")
        serialized_items = pd.DataFrame([item.model_dump(mode="json") for item in items])
        df = self.spark.createDataFrame(serialized_items)
        df.write.mode("append").format("delta").saveAsTable(self.config.table.to_spark())
        logger.info("Upsert complete")

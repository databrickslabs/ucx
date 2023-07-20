from uc_migration_toolkit.config import InventoryTable
from uc_migration_toolkit.providers.spark import SparkMixin


class InventoryManager(SparkMixin):
    def __init__(self, inventory_table: InventoryTable):
        super().__init__()
        self.inventory_table = inventory_table

    def cleanup(self):
        self.logger.info(f"Cleaning up inventory table {self.inventory_table}")
        self.spark.sql(f"DROP TABLE IF EXISTS {self.inventory_table}")
        self.logger.info("Inventory table cleanup complete")

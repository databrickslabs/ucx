import logging

from databricks.sdk import WorkspaceClient
from pyspark.sql import DataFrame
from pyspark.sql.types import StringType, StructField, StructType

from databricks.labs.ucx.inventory.types import PermissionsInventoryItem
from databricks.labs.ucx.providers.spark import SparkMixin

logger = logging.getLogger(__name__)


class PermissionsInventoryTable(SparkMixin):
    def __init__(self, inventory_database: str, ws: WorkspaceClient):
        super().__init__(ws)
        self._table = f"hive_metastore.{inventory_database}.permissions"

    @property
    def _table_schema(self) -> StructType:
        # TODO: generate the table schema automatically from the PermissionsInventoryItem class
        return StructType(
            [
                StructField("object_id", StringType(), True),
                StructField("support", StringType(), True),
                StructField("raw_object_permissions", StringType(), True),
            ]
        )

    @property
    def _df(self) -> DataFrame:
        return self.spark.table(self._table)

    def cleanup(self):
        logger.info(f"Cleaning up inventory table {self._table}")
        self.spark.sql(f"DROP TABLE IF EXISTS {self._table}")
        logger.info("Inventory table cleanup complete")

    def save(self, items: list[PermissionsInventoryItem]):
        # TODO: update instead of append
        logger.info(f"Saving {len(items)} items to inventory table {self._table}")
        serialized_items = [item.as_dict() for item in items]
        df = self.spark.createDataFrame(serialized_items, schema=self._table_schema)
        df.write.mode("append").format("delta").saveAsTable(self._table)
        logger.info("Successfully saved the items to inventory table")

    def load_all(self) -> list[PermissionsInventoryItem]:
        logger.info(f"Loading inventory table {self._table}")
        df = self._df.toPandas()

        logger.info("Successfully loaded the inventory table")
        return PermissionsInventoryItem.from_pandas(df)

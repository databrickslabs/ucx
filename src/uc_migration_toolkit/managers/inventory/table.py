import pandas as pd
import pyspark.sql.functions as F  # noqa: N812
from databricks.sdk.service.iam import Group
from pyspark.sql import DataFrame
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    StringType,
    StructField,
    StructType,
)

from uc_migration_toolkit.managers.inventory.types import PermissionsInventoryItem
from uc_migration_toolkit.providers.config import provider as config_provider
from uc_migration_toolkit.providers.logger import logger
from uc_migration_toolkit.providers.spark import SparkMixin


class InventoryTableManager(SparkMixin):
    def __init__(self):
        super().__init__()
        self.config = config_provider.config.inventory

    @property
    def _table_schema(self) -> StructType:
        return StructType(
            [
                StructField("object_id", StringType(), True),
                StructField("logical_object_type", StringType(), True),
                StructField("request_object_type", StringType(), True),
                StructField(
                    "object_permissions",
                    StructType(
                        [
                            StructField(
                                "access_control_list",
                                ArrayType(
                                    StructType(
                                        [
                                            StructField(
                                                "all_permissions",
                                                ArrayType(
                                                    StructType(
                                                        [
                                                            StructField("inherited", BooleanType(), True),
                                                            StructField(
                                                                "inherited_from_object",
                                                                ArrayType(StringType(), True),
                                                                True,
                                                            ),
                                                            StructField("permission_level", StringType(), True),
                                                        ]
                                                    ),
                                                    True,
                                                ),
                                                True,
                                            ),
                                            StructField("group_name", StringType(), True),
                                            StructField("service_principal_name", StringType(), True),
                                            StructField("user_name", StringType(), True),
                                        ]
                                    ),
                                    True,
                                ),
                                True,
                            ),
                            StructField("object_id", StringType(), True),
                            StructField("object_type", StringType(), True),
                        ]
                    ),
                    True,
                ),
            ]
        )

    @property
    def _table(self) -> DataFrame:
        assert self.config.table, "Inventory table name is not set"
        return self.spark.table(self.config.table.to_spark())

    def cleanup(self):
        logger.info(f"Cleaning up inventory table {self.config.table}")
        self.spark.sql(f"DROP TABLE IF EXISTS {self.config.table.to_spark()}")
        logger.info("Inventory table cleanup complete")

    def save(self, items: list[PermissionsInventoryItem]):
        logger.info(f"Saving {len(items)} items to inventory table {self.config.table}")
        serialized_items = pd.DataFrame([item.model_dump(mode="json") for item in items])
        df = self.spark.createDataFrame(serialized_items, schema=self._table_schema)
        df.write.mode("append").format("delta").saveAsTable(self.config.table.to_spark())
        logger.info("Successfully saved the items to inventory table")

    def load_all(self) -> list[PermissionsInventoryItem]:
        logger.info(f"Loading inventory table {self.config.table}")
        df = self._table.toPandas()
        logger.info("Successfully loaded the inventory table")
        return [PermissionsInventoryItem(**item) for item in df.to_dict(orient="records")]

    def load_for_groups(self, groups: list[Group]) -> list[PermissionsInventoryItem]:
        logger.info(f"Scanning inventory table {self.config.table} for {len(groups)} groups")
        group_names = [g.display_name for g in groups]
        group_names_sql_argument = ".".join([f'"{name}"' for name in group_names])
        df = (
            self._table.where(
                f"""
            size(
                array_intersect(
                  array_distinct(transform(object_permissions.access_control_list, item -> item.group_name))
                  , array({group_names_sql_argument})
                )
            ) > 0
            """
            )
            .withColumn("plain_permissions", F.to_json("object_permissions"))
            .drop("object_permissions")
            .toPandas()
        )

        logger.info(
            f"Successfully scanned the inventory table, loaded {len(df)} relevant objects for {len(groups)} groups"
        )
        return PermissionsInventoryItem.from_pandas(df)

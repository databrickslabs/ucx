import functools

from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession

from uc_migration_toolkit.providers.logger import logger


class SparkMixin:
    def __init__(self):
        super().__init__()
        self._spark = self._initialize_spark()

    @staticmethod
    @functools.lru_cache(maxsize=10_000)
    def _initialize_spark() -> SparkSession:
        logger.info("Initializing Spark session")
        if "spark" in locals():
            logger.info("Using the Spark session from runtime")
            return locals()["spark"]
        else:
            logger.info("Using DB Connect")
            spark = DatabricksSession.builder.getOrCreate()
            return spark

    @property
    def spark(self):
        return self._spark

from databricks.connect import DatabricksSession
from pyspark.sql import SparkSession

from uc_migration_toolkit.providers.logger import LoggerMixin


class SparkMixin(LoggerMixin):
    def __init__(self):
        super().__init__()
        self._spark = self._initialize_spark()

    @staticmethod
    def _initialize_spark() -> SparkSession:
        if "spark" in locals():
            return locals()["spark"]
        else:
            spark = DatabricksSession.builder.getOrCreate()
            return spark

    @property
    def spark(self):
        return self._spark

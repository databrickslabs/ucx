from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession

from databricks.labs.ucx.providers.logger import logger


class SparkMixin:
    def __init__(self, ws: WorkspaceClient):
        self._spark = self._initialize_spark(ws)

    @staticmethod
    def _initialize_spark(ws: WorkspaceClient) -> SparkSession:
        logger.info("Initializing Spark session")
        try:
            from databricks.sdk.runtime import spark

            return spark
        except ImportError:
            logger.info("Using DB Connect")
            from databricks.connect import DatabricksSession

            if ws.config.cluster_id is None:
                msg = "DATABRICKS_CLUSTER_ID environment variable is not set, cannot use DB Connect"
                raise RuntimeError(msg) from None

            cluster_id = ws.config.cluster_id
            cluster_info = ws.clusters.get(cluster_id)

            logger.info(f"Ensuring that cluster {cluster_id} ({cluster_info.cluster_name}) is running")
            ws.clusters.ensure_cluster_is_running(cluster_id)

            logger.info("Cluster is ready, creating the DBConnect session")
            spark = DatabricksSession.builder.sdkConfig(ws.config).getOrCreate()
            return spark

    @property
    def spark(self):
        return self._spark

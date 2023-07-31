import functools
import os
import time

from databricks.sdk.service.compute import State
from pyspark.sql import SparkSession

from uc_migration_toolkit.providers.client import provider
from uc_migration_toolkit.providers.logger import logger


class SparkMixin:
    def __init__(self):
        super().__init__()
        self._spark = self._initialize_spark()

    @staticmethod
    def _initialize_spark() -> SparkSession:
        logger.info("Initializing Spark session")
        if "spark" in locals():
            logger.info("Using the Spark session from runtime")
            return locals()["spark"]
        else:
            logger.info("Using DB Connect")
            from databricks.connect import DatabricksSession

            if "DATABRICKS_CLUSTER_ID" not in os.environ:
                msg = "DATABRICKS_CLUSTER_ID environment variable is not set, cannot use DB Connect"
                raise RuntimeError(msg)
            cluster_id = os.environ["DATABRICKS_CLUSTER_ID"]
            cluster_info = provider.ws.clusters.get(cluster_id)

            logger.info(f"Using cluster {cluster_id} with name {cluster_info.cluster_name}")

            if cluster_info.state not in [State.RUNNING, State.PENDING, State.RESTARTING]:
                logger.info("Cluster is not running, starting it")
                provider.ws.clusters.start(cluster_id)
                time.sleep(2)

            logger.info("Waiting for the cluster to get running")
            provider.ws.clusters.wait_get_cluster_running(cluster_id)
            logger.info("Cluster is ready, creating the DBConnect session")
            provider.ws.config.cluster_id = cluster_id
            spark = DatabricksSession.builder.sdkConfig(provider.ws.config).getOrCreate()
            return spark

    @property
    def spark(self):
        return self._spark

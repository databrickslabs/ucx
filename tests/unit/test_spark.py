import logging
import pytest
import unittest
from unittest.mock import Mock

from databricks.connect import DatabricksSession
from databricks.sdk.service.compute import ClusterDetails
from pyspark.sql.connect.session import SparkSession

from databricks.labs.ucx.providers.spark import SparkMixin

logger = logging.getLogger(__name__)


def test_initialize_spark():
    ws = Mock()
    ws.clusters.get.return_value = ClusterDetails(cluster_name="cn1")
    ws.clusters.ensure_cluster_is_running.return_value = True

    DatabricksSession.Builder = Mock()
    DatabricksSession.Builder.getOrCreate.return_value = SparkSession

    session = SparkMixin(ws=ws)
    # This log messages shows if the except code path was used
    logger.debug(f"Get cluster call count: {ws.clusters.get.call_count}")
    assert session.spark is not None

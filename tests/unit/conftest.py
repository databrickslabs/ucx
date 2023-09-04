import logging
import shutil
import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

from databricks.labs.ucx.tacl._internal import SqlBackend

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def spark() -> SparkSession:
    """
    This fixture provides preconfigured SparkSession with Hive and Delta support.
    After the test session, temporary warehouse directory is deleted.
    :return: SparkSession
    """
    logger.info("Configuring Spark session for testing environment")
    warehouse_dir = tempfile.TemporaryDirectory().name
    _builder = (
        SparkSession.builder.master("local[1]")
        .config("spark.hive.metastore.warehouse.dir", Path(warehouse_dir).as_uri())
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )
    spark: SparkSession = configure_spark_with_delta_pip(_builder).getOrCreate()
    logger.info("Spark session configured")
    yield spark
    logger.info("Shutting down Spark session")
    spark.stop()
    if Path(warehouse_dir).exists():
        shutil.rmtree(warehouse_dir)


class MockBackend(SqlBackend):
    def __init__(self, *, fails_on_first: dict | None = None, rows: dict | None = None):
        self._fails_on_first = fails_on_first
        if not rows:
            rows = {}
        self._rows = rows
        self.queries = []

    def _sql(self, sql):
        logger.debug(f"Mock backend.sql() received SQL: {sql}")
        seen_before = sql in self.queries
        self.queries.append(sql)
        if not seen_before and self._fails_on_first is not None:
            for match, failure in self._fails_on_first.items():
                if match in sql:
                    raise RuntimeError(failure)

    def execute(self, sql):
        self._sql(sql)

    def fetch(self, sql) -> Iterator[any]:
        self._sql(sql)
        first = sql.upper().split(" ")[0]
        rows = []
        if first in self._rows:
            rows = self._rows[first]
        return iter(rows)


@pytest.fixture
def mock_backend():
    return MockBackend()

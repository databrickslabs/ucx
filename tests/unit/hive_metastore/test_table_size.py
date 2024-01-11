import sys

from databricks.sdk.errors import NotFound

from databricks.labs.ucx.hive_metastore.table_size import TableSize, TableSizeCrawler
from tests.unit.framework.mocks import MockBackend


class SparkSession:
    pass


def test_table_size_crawler(mocker):
    errors = {}
    rows = {
        "table_size": [],
        "hive_metastore.inventory_database.tables": [
            ("hive_metastore", "db1", "table1", "MANAGED", "DELTA", "dbfs:/location/table", None),
            ("hive_metastore", "db1", "table2", "MANAGED", "DELTA", "/dbfs/location/table", None),
            ("hive_metastore", "db1", "table3", "MANAGED", "DELTA", "dbfs:/mnt/location/table", None),
            ("hive_metastore", "db1", "table4", "MANAGED", "DELTA", "s3:/location/table", None),
            ("hive_metastore", "db1", "table5", "MANAGED", "DELTA", "/dbfs/mnt/location/table", None),
            ("hive_metastore", "db1", "table6", "MANAGED", "DELTA", "/dbfs/databricks-datasets/location/table", None),
            ("hive_metastore", "db1", "table7", "MANAGED", "DELTA", "dbfs:/databricks-datasets/location/table", None),
            ("hive_metastore", "db1", "table8", "MANAGED", "DELTA", "/databricks-datasets/location/table", None),
            ("hive_metastore", "db1", "view", "VIEW", "DELTA", None, "SELECT * FROM TABLE"),
        ],
        "SHOW DATABASES": [("db1",)],
    }
    backend = MockBackend(fails_on_first=errors, rows=rows)
    pyspark_sql_session = mocker.Mock()
    sys.modules["pyspark.sql.session"] = pyspark_sql_session
    tsc = TableSizeCrawler(backend, "inventory_database")
    tsc._spark._jsparkSession.table().queryExecution().analyzed().stats().sizeInBytes.side_effect = [100, 200, 300]
    results = tsc.snapshot()
    assert len(results) == 2
    assert TableSize("hive_metastore", "db1", "table1", 100) in results
    assert TableSize("hive_metastore", "db1", "table2", 200) in results


def test_table_size_table_not_found(mocker):
    errors = {}
    rows = {
        "table_size": [],
        "hive_metastore.inventory_database.tables": [
            ("hive_metastore", "db1", "table1", "MANAGED", "DELTA", "dbfs:/location/table", None),
        ],
        "SHOW DATABASES": [("db1",)],
    }
    backend = MockBackend(fails_on_first=errors, rows=rows)
    pyspark_sql_session = mocker.Mock()
    sys.modules["pyspark.sql.session"] = pyspark_sql_session
    tsc = TableSizeCrawler(backend, "inventory_database")

    # table removed after crawling
    tsc._spark._jsparkSession.table().queryExecution().analyzed().stats().sizeInBytes.side_effect = NotFound(...)

    results = tsc.snapshot()

    assert len(results) == 0

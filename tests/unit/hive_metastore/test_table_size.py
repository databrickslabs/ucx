import logging
import sys

from databricks.labs.lsql.backends import MockBackend

from databricks.labs.ucx.hive_metastore.table_size import TableSize, TableSizeCrawler

# pylint: disable=protected-access


class SparkSession:
    pass


def test_table_size_crawler(mocker):
    errors = {}
    rows = {
        "table_size": [],
        "`hive_metastore`.`inventory_database`.`tables`": [
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
    assert "ANALYZE table `hive_metastore`.`db1`.`table1` compute STATISTICS NOSCAN" in backend.queries
    assert "ANALYZE table `hive_metastore`.`db1`.`table2` compute STATISTICS NOSCAN" in backend.queries
    assert len(results) == 2
    assert TableSize("hive_metastore", "db1", "table1", 100) in results
    assert TableSize("hive_metastore", "db1", "table2", 200) in results


def test_table_size_unknown_error(mocker, caplog):
    errors = {}
    rows = {
        "table_size": [],
        "`hive_metastore`.`inventory_database`.`tables`": [
            ("hive_metastore", "db1", "table1", "MANAGED", "DELTA", "dbfs:/location/table", None),
        ],
        "SHOW DATABASES": [("db1",)],
    }
    backend = MockBackend(fails_on_first=errors, rows=rows)
    pyspark_sql_session = mocker.Mock()
    sys.modules["pyspark.sql.session"] = pyspark_sql_session
    tsc = TableSizeCrawler(backend, "inventory_database")
    tsc._spark._jsparkSession.table().queryExecution().analyzed().stats().sizeInBytes.side_effect = Exception(...)

    with caplog.at_level(logging.WARNING):
        results = tsc.snapshot()

    assert len(results) == 0


def test_table_size_table_or_view_not_found(mocker, caplog):
    errors = {}
    rows = {
        "table_size": [],
        "`hive_metastore`.`inventory_database`.`tables`": [
            ("hive_metastore", "db1", "table1", "MANAGED", "DELTA", "dbfs:/location/table", None),
        ],
        "SHOW DATABASES": [("db1",)],
    }
    backend = MockBackend(fails_on_first=errors, rows=rows)
    pyspark_sql_session = mocker.Mock()
    sys.modules["pyspark.sql.session"] = pyspark_sql_session
    tsc = TableSizeCrawler(backend, "inventory_database")

    # table removed after crawling
    tsc._spark._jsparkSession.table().queryExecution().analyzed().stats().sizeInBytes.side_effect = Exception(
        "[TABLE_OR_VIEW_NOT_FOUND]"
    )

    with caplog.at_level(logging.WARNING):
        results = tsc.snapshot()

    assert len(results) == 0
    assert "Failed to evaluate hive_metastore.db1.table1 table size. Table not found" in caplog.text


def test_table_size_delta_table_not_found(mocker, caplog):
    errors = {}
    rows = {
        "table_size": [],
        "`hive_metastore`.`inventory_database`.`tables`": [
            ("hive_metastore", "db1", "table1", "MANAGED", "DELTA", "dbfs:/location/table", None),
        ],
        "SHOW DATABASES": [("db1",)],
    }
    backend = MockBackend(fails_on_first=errors, rows=rows)
    pyspark_sql_session = mocker.Mock()
    sys.modules["pyspark.sql.session"] = pyspark_sql_session
    tsc = TableSizeCrawler(backend, "inventory_database")

    # table removed after crawling
    tsc._spark._jsparkSession.table().queryExecution().analyzed().stats().sizeInBytes.side_effect = Exception(
        "[DELTA_TABLE_NOT_FOUND]"
    )

    with caplog.at_level(logging.WARNING):
        results = tsc.snapshot()

    assert len(results) == 0
    assert "Failed to evaluate hive_metastore.db1.table1 table size. Table not found" in caplog.text


def test_table_size_when_table_corrupted(mocker, caplog):
    errors = {}
    rows = {
        "table_size": [],
        "`hive_metastore`.`inventory_database`.`tables`": [
            ("hive_metastore", "db1", "table1", "MANAGED", "DELTA", "dbfs:/location/table", None),
        ],
        "SHOW DATABASES": [("db1",)],
    }
    backend = MockBackend(fails_on_first=errors, rows=rows)
    pyspark_sql_session = mocker.Mock()
    sys.modules["pyspark.sql.session"] = pyspark_sql_session
    tsc = TableSizeCrawler(backend, "inventory_database")

    tsc._spark._jsparkSession.table().queryExecution().analyzed().stats().sizeInBytes.side_effect = Exception(
        "[DELTA_MISSING_TRANSACTION_LOG]"
    )

    with caplog.at_level(logging.WARNING):
        results = tsc.snapshot()

    assert len(results) == 0
    assert "Delta table hive_metastore.db1.table1 is corrupted: missing transaction log" in caplog.text


def test_table_size_when_delta_invalid_format_error(mocker, caplog):
    errors = {}
    rows = {
        "table_size": [],
        "`hive_metastore`.`inventory_database`.`tables`": [
            ("hive_metastore", "db1", "table1", "MANAGED", "DELTA", "dbfs:/location/table", None),
        ],
        "SHOW DATABASES": [("db1",)],
    }
    backend = MockBackend(fails_on_first=errors, rows=rows)
    pyspark_sql_session = mocker.Mock()
    sys.modules["pyspark.sql.session"] = pyspark_sql_session
    tsc = TableSizeCrawler(backend, "inventory_database")

    tsc._spark._jsparkSession.table().queryExecution().analyzed().stats().sizeInBytes.side_effect = Exception(
        "[DELTA_INVALID_FORMAT]"
    )

    with caplog.at_level(logging.WARNING):
        results = tsc.snapshot()

    assert len(results) == 0
    assert (
        "Unable to read Delta table hive_metastore.db1.table1, please check table structure and try again."
        in caplog.text
    )

from databricks.labs.ucx.hive_metastore.table_size import TableSize, TableSizeCrawler
from tests.unit.framework.mocks import MockRuntimeBackend


def test_table_size_crawler():
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
    backend = MockRuntimeBackend(fails_on_first=errors, rows=rows, table_sizes=[100, 200, 300])
    tsc = TableSizeCrawler(backend, "inventory_database")
    results = tsc.snapshot()
    assert len(results) == 2
    assert TableSize("hive_metastore", "db1", "table1", 100) in results
    assert TableSize("hive_metastore", "db1", "table2", 200) in results

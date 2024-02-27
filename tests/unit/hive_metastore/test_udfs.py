from databricks.labs.ucx.hive_metastore.udfs import Udf, UdfsCrawler

from ..framework.mocks import MockBackend
from .test_grants import make_row


def test_key():
    udf = Udf(
        catalog="CATALOG",
        database="DB",
        name="function",
        func_type="_",
        func_input="_",
        func_returns="_",
        deterministic=True,
        data_access="",
        body="",
    )
    assert udf.key == "catalog.db.function"


def test_udfs_returning_error_when_describing():
    errors = {"DESCRIBE FUNCTION EXTENDED hive_metastore.database.function1": "error"}
    rows = {
        "SHOW DATABASES": [
            make_row(("database",), ["databaseName"]),
        ],
        "SHOW USER FUNCTIONS FROM hive_metastore.database": [
            make_row(("hive_metastore.database.function1",), ["function"]),
        ],
    }
    backend = MockBackend(fails_on_first=errors, rows=rows)
    udf_crawler = UdfsCrawler(backend, "default")
    results = udf_crawler.snapshot()
    assert len(results) == 0

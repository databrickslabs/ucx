from unittest.mock import create_autospec

from databricks.labs.lsql.backends import MockBackend

from databricks.labs.ucx.framework.owners import AdministratorLocator
from databricks.labs.ucx.hive_metastore.udfs import Udf, UdfsCrawler, UdfOwnership


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
        comment="",
    )
    assert udf.key == "catalog.db.function"


SHOW_DATABASES = MockBackend.rows("databaseName")
SHOW_FUNCTIONS = MockBackend.rows("function")


def test_udfs_returning_error_when_describing():
    errors = {"DESCRIBE FUNCTION EXTENDED hive_metastore.database.function1": "error"}
    rows = {
        "SHOW DATABASES": SHOW_DATABASES[("database",),],
        "SHOW USER FUNCTIONS FROM hive_metastore.database": SHOW_FUNCTIONS[("hive_metastore.database.function1",),],
    }
    backend = MockBackend(fails_on_first=errors, rows=rows)
    udf_crawler = UdfsCrawler(backend, "default")
    results = udf_crawler.snapshot()
    assert len(results) == 0


def test_tables_crawler_should_filter_by_database():
    rows = {
        "SHOW USER FUNCTIONS FROM `hive_metastore`.`database`": SHOW_FUNCTIONS[("hive_metastore.database.function1",),],
    }
    backend = MockBackend(rows=rows)
    udf_crawler = UdfsCrawler(backend, "default", ["database"])
    results = udf_crawler.snapshot()
    assert len(results) == 1


def test_udf_owner() -> None:
    """Verify that the owner of a crawled UDF is an administrator."""
    admin_locator = create_autospec(AdministratorLocator)
    admin_locator.get_workspace_administrator.return_value = "an_admin"

    ownership = UdfOwnership(admin_locator)
    udf = Udf(
        catalog="main",
        database="foo",
        name="bar",
        func_type="UNKNOWN",
        func_input="UNKNOWN",
        func_returns="UNKNOWN",
        deterministic=True,
        data_access="UNKNOWN",
        body="UNKNOWN",
        comment="UNKNOWN",
    )
    owner = ownership.owner_of(udf)

    assert owner == "an_admin"
    admin_locator.get_workspace_administrator.assert_called_once()

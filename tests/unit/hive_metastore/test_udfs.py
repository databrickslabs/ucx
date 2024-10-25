from unittest.mock import create_autospec

import pytest
from databricks.labs.lsql.backends import MockBackend
from databricks.labs.lsql.core import Row

from databricks.labs.ucx.__about__ import __version__ as ucx_version
from databricks.labs.ucx.framework.owners import AdministratorLocator
from databricks.labs.ucx.hive_metastore.udfs import Udf, UdfsCrawler, UdfOwnership
from databricks.labs.ucx.progress.history import ProgressEncoder


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


@pytest.mark.parametrize(
    "udf_record,history_record",
    (
        (
            Udf(
                catalog="hive_metastore",
                database="foo",
                name="bar",
                func_type="UNKNOWN-1",
                func_input="UNKNOWN-2",
                func_returns="UNKNOWN-3",
                deterministic=True,
                data_access="UNKNOWN-4",
                body="UNKNOWN-5",
                comment="UNKNOWN-6",
            ),
            Row(
                workspace_id=2,
                job_run_id=1,
                object_type="Udf",
                object_id=["hive_metastore", "foo", "bar"],
                data={
                    "catalog": "hive_metastore",
                    "database": "foo",
                    "name": "bar",
                    "func_type": "UNKNOWN-1",
                    "func_input": "UNKNOWN-2",
                    "func_returns": "UNKNOWN-3",
                    "deterministic": "true",
                    "data_access": "UNKNOWN-4",
                    "body": "UNKNOWN-5",
                    "comment": "UNKNOWN-6",
                    "success": "1",
                },
                failures=[],
                owner="the_admin",
                ucx_version=ucx_version,
            ),
        ),
        (
            Udf(
                catalog="hive_metastore",
                database="foo",
                name="bar",
                func_type="UNKNOWN-1",
                func_input="UNKNOWN-2",
                func_returns="UNKNOWN-3",
                deterministic=True,
                data_access="UNKNOWN-4",
                body="UNKNOWN-5",
                comment="UNKNOWN-6",
                success=0,
                # Note: NOT json-encoded as is the convention elsewhere.
                failures="something_is_wrong_with_this_udf",
            ),
            Row(
                workspace_id=2,
                job_run_id=1,
                object_type="Udf",
                object_id=["hive_metastore", "foo", "bar"],
                data={
                    "catalog": "hive_metastore",
                    "database": "foo",
                    "name": "bar",
                    "func_type": "UNKNOWN-1",
                    "func_input": "UNKNOWN-2",
                    "func_returns": "UNKNOWN-3",
                    "deterministic": "true",
                    "data_access": "UNKNOWN-4",
                    "body": "UNKNOWN-5",
                    "comment": "UNKNOWN-6",
                    "success": "0",
                },
                failures=["something_is_wrong_with_this_udf"],
                owner="the_admin",
                ucx_version=ucx_version,
            ),
        ),
    ),
)
def test_udf_supports_history(mock_backend, udf_record: Udf, history_record: Row) -> None:
    """Verify that Udf records are written as expected to the history log."""
    mock_ownership = create_autospec(UdfOwnership)
    mock_ownership.owner_of.return_value = "the_admin"
    history_log = ProgressEncoder[Udf](mock_backend, mock_ownership, Udf, run_id=1, workspace_id=2, catalog="a_catalog")

    history_log.append_inventory_snapshot([udf_record])

    rows = mock_backend.rows_written_for("`a_catalog`.`multiworkspace`.`historical`", mode="append")

    assert rows == [history_record]

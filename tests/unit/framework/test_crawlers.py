import os
import sys
from dataclasses import dataclass
from unittest import mock

import pytest
from databricks.sdk.errors import BadRequest, NotFound, PermissionDenied, Unknown
from databricks.sdk.service import sql

from databricks.labs.ucx.framework.crawlers import (
    CrawlerBase,
    RuntimeBackend,
    StatementExecutionBackend,
)

from ..framework.mocks import MockBackend


@dataclass
class Foo:
    first: str
    second: bool


@dataclass
class Baz:
    first: str
    second: str | None = None


@dataclass
class Bar:
    first: str
    second: bool
    third: float


def test_invalid():
    with pytest.raises(ValueError):
        CrawlerBase(MockBackend(), "a.a.a", "b", "c", Bar)


def test_full_name():
    cb = CrawlerBase(MockBackend(), "a", "b", "c", Bar)
    assert "a.b.c" == cb._full_name


def test_snapshot_appends_to_existing_table():
    b = MockBackend()
    cb = CrawlerBase(b, "a", "b", "c", Bar)

    result = cb._snapshot(fetcher=lambda: [], loader=lambda: [Foo(first="first", second=True)])

    assert [Foo(first="first", second=True)] == result
    assert [Foo(first="first", second=True)] == b.rows_written_for("a.b.c", "append")


def test_snapshot_appends_to_new_table():
    b = MockBackend()
    cb = CrawlerBase(b, "a", "b", "c", Bar)

    def fetcher():
        msg = ".. TABLE_OR_VIEW_NOT_FOUND .."
        raise NotFound(msg)

    result = cb._snapshot(fetcher=fetcher, loader=lambda: [Foo(first="first", second=True)])

    assert [Foo(first="first", second=True)] == result
    assert [Foo(first="first", second=True)] == b.rows_written_for("a.b.c", "append")


def test_snapshot_wrong_error():
    b = MockBackend()
    cb = CrawlerBase(b, "a", "b", "c", Bar)

    def fetcher():
        msg = "always fails"
        raise ValueError(msg)

    with pytest.raises(ValueError):
        cb._snapshot(fetcher=fetcher, loader=lambda: [Foo(first="first", second=True)])


def test_statement_execution_backend_execute_happy(mocker):
    execute_statement = mocker.patch("databricks.sdk.service.sql.StatementExecutionAPI.execute_statement")
    execute_statement.return_value = sql.ExecuteStatementResponse(
        status=sql.StatementStatus(state=sql.StatementState.SUCCEEDED)
    )

    seb = StatementExecutionBackend(mocker.Mock(), "abc")

    seb.execute("CREATE TABLE foo")

    execute_statement.assert_called_with(
        warehouse_id="abc",
        statement="CREATE TABLE foo",
        catalog=None,
        schema=None,
        disposition=sql.Disposition.INLINE,
        format=sql.Format.JSON_ARRAY,
        byte_limit=None,
        wait_timeout=None,
    )


def test_statement_execution_backend_fetch_happy(mocker):
    # this will improve once https://github.com/databricks/databricks-sdk-py/pull/295 merged
    execute_fetch_all = mocker.patch(
        "databricks.labs.ucx.mixins.sql.StatementExecutionExt.execute_fetch_all", return_value=[1, 2, 3]
    )

    seb = StatementExecutionBackend(mocker.Mock(), "abc")

    result = seb.fetch("SELECT id FROM range(3)")

    assert [1, 2, 3] == result

    execute_fetch_all.assert_called_with("abc", "SELECT id FROM range(3)")


def test_statement_execution_backend_save_table_overwrite(mocker):
    seb = StatementExecutionBackend(mocker.Mock(), "abc")
    with pytest.raises(NotImplementedError):
        seb.save_table("a.b.c", [1, 2, 3], Bar, mode="overwrite")


def test_statement_execution_backend_save_table_empty_records(mocker):
    execute_sql = mocker.patch("databricks.labs.ucx.mixins.sql.StatementExecutionExt.execute")

    seb = StatementExecutionBackend(mocker.Mock(), "abc")

    seb.save_table("a.b.c", [], Bar)

    execute_sql.assert_called()


def test_statement_execution_backend_save_table_two_records(mocker):
    execute_sql = mocker.patch("databricks.labs.ucx.mixins.sql.StatementExecutionExt.execute")

    seb = StatementExecutionBackend(mocker.Mock(), "abc")

    seb.save_table("a.b.c", [Foo("aaa", True), Foo("bbb", False)], Foo)

    assert [
        mocker.call(
            "abc", "CREATE TABLE IF NOT EXISTS a.b.c (first STRING NOT NULL, second BOOLEAN NOT NULL) USING DELTA"
        ),
        mocker.call("abc", "INSERT INTO a.b.c (first, second) VALUES ('aaa', TRUE), ('bbb', FALSE)"),
    ] == execute_sql.mock_calls


def test_statement_execution_backend_save_table_in_batches_of_two(mocker):
    execute_sql = mocker.patch("databricks.labs.ucx.mixins.sql.StatementExecutionExt.execute")

    seb = StatementExecutionBackend(mocker.Mock(), "abc", max_records_per_batch=2)

    seb.save_table("a.b.c", [Foo("aaa", True), Foo("bbb", False), Foo("ccc", True)], Foo)
    assert [
        mocker.call(
            "abc", "CREATE TABLE IF NOT EXISTS a.b.c (first STRING NOT NULL, second BOOLEAN NOT NULL) USING DELTA"
        ),
        mocker.call("abc", "INSERT INTO a.b.c (first, second) VALUES ('aaa', TRUE), ('bbb', FALSE)"),
        mocker.call("abc", "INSERT INTO a.b.c (first, second) VALUES ('ccc', TRUE)"),
    ] == execute_sql.mock_calls


def test_runtime_backend_execute(mocker):
    with mock.patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "14.0"}):
        pyspark_sql_session = mocker.Mock()
        sys.modules["pyspark.sql.session"] = pyspark_sql_session

        rb = RuntimeBackend()

        rb.execute("CREATE TABLE foo")

        rb._spark.sql.assert_called_with("CREATE TABLE foo")


def test_runtime_backend_fetch(mocker):
    with mock.patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "14.0"}):
        pyspark_sql_session = mocker.Mock()
        sys.modules["pyspark.sql.session"] = pyspark_sql_session

        rb = RuntimeBackend()
        rb._spark.sql().collect.return_value = [1, 2, 3]

        result = rb.fetch("SELECT id FROM range(3)")

        assert [1, 2, 3] == result

        rb._spark.sql.assert_called_with("SELECT id FROM range(3)")


def test_runtime_backend_save_table(mocker):
    with mock.patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "14.0"}):
        pyspark_sql_session = mocker.Mock()
        sys.modules["pyspark.sql.session"] = pyspark_sql_session

        rb = RuntimeBackend()

        rb.save_table("a.b.c", [Foo("aaa", True), Foo("bbb", False)], Foo)

        rb._spark.createDataFrame.assert_called_with(
            [Foo(first="aaa", second=True), Foo(first="bbb", second=False)],
            "first STRING NOT NULL, second BOOLEAN NOT NULL",
        )
        rb._spark.createDataFrame().write.saveAsTable.assert_called_with("a.b.c", mode="append")


def test_runtime_backend_save_table_with_row_containing_none_with_nullable_class(mocker):
    with mock.patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "14.0"}):
        pyspark_sql_session = mocker.Mock()
        sys.modules["pyspark.sql.session"] = pyspark_sql_session

        rb = RuntimeBackend()

        rb.save_table("a.b.c", [Baz("aaa", "ccc"), Baz("bbb", None)], Baz)

        rb._spark.createDataFrame.assert_called_with(
            [Baz(first="aaa", second="ccc"), Baz(first="bbb", second=None)],
            "first STRING NOT NULL, second STRING",
        )
        rb._spark.createDataFrame().write.saveAsTable.assert_called_with("a.b.c", mode="append")


@dataclass
class TestClass:
    key: str
    value: str | None = None


def test_save_table_with_not_null_constraint_violated(mocker):
    rows = [TestClass("1", "test"), TestClass("2", None), TestClass(None, "value")]

    with mock.patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "14.0"}):
        pyspark_sql_session = mocker.Mock()
        sys.modules["pyspark.sql.session"] = pyspark_sql_session

        rb = RuntimeBackend()

        with pytest.raises(Exception) as exc_info:
            rb.save_table("a.b.c", rows, TestClass)

        assert (
            str(exc_info.value) == "Not null constraint violated for column key, row = {'key': None, 'value': 'value'}"
        )


# def test_raise_if_needed(mocker):
#     with mock.patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "14.0"}):
#         pyspark_sql_session = mocker.Mock()
#         sys.modules["pyspark.sql.session"] = pyspark_sql_session
#
#         status_invalid_schema = StatementStatus(
#             state=StatementState.FAILED, error=ServiceError(message="SCHEMA_NOT_FOUND")
#         )
#         with pytest.raises(NotFound):
#             RuntimeBackend._raise_if_needed(status_invalid_schema)
#
#         status_invalid_table = StatementStatus(
#             state=StatementState.FAILED, error=ServiceError(message="TABLE_OR_VIEW_NOT_FOUND")
#         )
#         with pytest.raises(NotFound):
#             RuntimeBackend._raise_if_needed(status_invalid_table)
#
#         status_syntax_error = StatementStatus(
#             state=StatementState.FAILED, error=ServiceError(message="PARSE_SYNTAX_ERROR")
#         )
#         with pytest.raises(BadRequest):
#             RuntimeBackend._raise_if_needed(status_syntax_error)
#
#         status_permission_denied = StatementStatus(
#             state=StatementState.FAILED, error=ServiceError(message="Operation not allowed")
#         )
#         with pytest.raises(PermissionDenied):
#             RuntimeBackend._raise_if_needed(status_permission_denied)
#
#         status_unknown_error = StatementStatus(state=StatementState.FAILED, error=None)
#         with pytest.raises(Unknown):
#             RuntimeBackend._raise_if_needed(status_unknown_error)


def test_raise_spark_sql_exceptions(mocker):
    with mock.patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "14.0"}):
        pyspark_sql_session = mocker.Mock()
        sys.modules["pyspark.sql.session"] = pyspark_sql_session

        rb = RuntimeBackend
        error_message_invalid_schema = "SCHEMA_NOT_FOUND foo schema does not exist"
        with pytest.raises(NotFound):
            rb._raise_spark_sql_exceptions(error_message_invalid_schema)

        error_message_invalid_table = "TABLE_OR_VIEW_NOT_FOUND foo table does not exist"
        with pytest.raises(NotFound):
            rb._raise_spark_sql_exceptions(error_message_invalid_table)

        error_message_invalid_syntax = "PARSE_SYNTAX_ERROR foo"
        with pytest.raises(BadRequest):
            rb._raise_spark_sql_exceptions(error_message_invalid_syntax)

        error_message_permission_denied = "foo Operation not allowed"
        with pytest.raises(PermissionDenied):
            rb._raise_spark_sql_exceptions(error_message_permission_denied)

        error_message_invalid_schema = "foo error failure"
        with pytest.raises(Unknown):
            rb._raise_spark_sql_exceptions(error_message_invalid_schema)


def test_execute(mocker):
    with mock.patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "14.0"}):
        pyspark_sql_session = mocker.Mock()
        sys.modules["pyspark.sql.session"] = pyspark_sql_session
        rb = RuntimeBackend()

        sql_query = "SELECT * from bar"

        pyspark_sql_session.SparkSession.builder.getOrCreate.return_value.sql.side_effect = Exception(
            "SCHEMA_NOT_FOUND"
        )
        with pytest.raises(NotFound):
            rb.execute(sql_query)

        pyspark_sql_session.SparkSession.builder.getOrCreate.return_value.sql.side_effect = Exception(
            "TABLE_OR_VIEW_NOT_FOUND"
        )
        with pytest.raises(NotFound):
            rb.execute(sql_query)

        pyspark_sql_session.SparkSession.builder.getOrCreate.return_value.sql.side_effect = Exception(
            "PARSE_SYNTAX_ERROR"
        )
        with pytest.raises(BadRequest):
            rb.execute(sql_query)

        pyspark_sql_session.SparkSession.builder.getOrCreate.return_value.sql.side_effect = Exception(
            "Operation not allowed"
        )
        with pytest.raises(PermissionDenied):
            rb.execute(sql_query)

        pyspark_sql_session.SparkSession.builder.getOrCreate.return_value.sql.side_effect = Exception(
            "foo error occurred"
        )
        with pytest.raises(Unknown):
            rb.execute(sql_query)


def test_fetch(mocker):
    with mock.patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "14.0"}):
        pyspark_sql_session = mocker.Mock()
        sys.modules["pyspark.sql.session"] = pyspark_sql_session
        rb = RuntimeBackend()

        sql_query = "SELECT * from bar"

        pyspark_sql_session.SparkSession.builder.getOrCreate.return_value.sql.side_effect = Exception(
            "SCHEMA_NOT_FOUND"
        )
        with pytest.raises(NotFound):
            rb.fetch(sql_query)

        pyspark_sql_session.SparkSession.builder.getOrCreate.return_value.sql.side_effect = Exception(
            "TABLE_OR_VIEW_NOT_FOUND"
        )
        with pytest.raises(NotFound):
            rb.fetch(sql_query)

        pyspark_sql_session.SparkSession.builder.getOrCreate.return_value.sql.side_effect = Exception(
            "PARSE_SYNTAX_ERROR"
        )
        with pytest.raises(BadRequest):
            rb.fetch(sql_query)

        pyspark_sql_session.SparkSession.builder.getOrCreate.return_value.sql.side_effect = Exception(
            "Operation not allowed"
        )
        with pytest.raises(PermissionDenied):
            rb.fetch(sql_query)

        pyspark_sql_session.SparkSession.builder.getOrCreate.return_value.sql.side_effect = Exception(
            "foo error occurred"
        )
        with pytest.raises(Unknown):
            rb.fetch(sql_query)

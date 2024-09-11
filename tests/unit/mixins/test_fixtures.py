from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import ColumnInfo, ColumnTypeName

# pylint: disable-next=unused-wildcard-import,wildcard-import
from databricks.labs.ucx.mixins.fixtures import *  # noqa: F403


@pytest.fixture
def ws():
    return create_autospec(WorkspaceClient)


@pytest.fixture
def sql_backend(mock_backend):
    return mock_backend


def test_make_table_query_contains_default_schema(make_table, sql_backend):
    make_table()
    assert "(id INT, value STRING)" in sql_backend.queries[-1]


def test_make_table_query_with_columns_contains_backticks(make_table, sql_backend):
    columns = [
        ColumnInfo(name="id", type_name=ColumnTypeName.INT),
        ColumnInfo(name="value", type_name=ColumnTypeName.STRING),
    ]
    make_table(columns=columns)
    assert "(`id` INT, `value` STRING)" in sql_backend.queries[-1]

from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import ColumnInfo, ColumnTypeName

# Fixtures are used implicitly and ignored in test signatures using `# noqa: F811`
# pylint: disable-next=unused-import
from databricks.labs.ucx.mixins.fixtures import make_table, make_schema, make_random  # fmt: skip  # noqa: F401


@pytest.fixture
def ws():
    workspace_client = create_autospec(WorkspaceClient)
    yield workspace_client
    # Circumvent linter warning; workspace client instance is never called at class level
    workspace_client.assert_not_called()


@pytest.fixture
def sql_backend(mock_backend):
    return mock_backend


def test_make_table_query_contains_default_schema(make_table, sql_backend):  # noqa: F811
    make_table()
    assert "(id INT, value STRING)" in sql_backend.queries[-1]


def test_make_table_query_with_columns_contains_backticks(make_table, sql_backend):  # noqa: F811
    columns = [
        ColumnInfo(name="id", type_name=ColumnTypeName.INT),
        ColumnInfo(name="value", type_name=ColumnTypeName.STRING),
    ]
    make_table(columns=columns)
    assert "(`id` INT, `value` STRING)" in sql_backend.queries[-1]

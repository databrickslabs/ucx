from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient

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

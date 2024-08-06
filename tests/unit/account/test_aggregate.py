import logging
from unittest.mock import create_autospec

import pytest
from databricks.sdk import AccountClient, Workspace, WorkspaceClient
from databricks.sdk.service import iam
from databricks.labs.lsql.backends import MockBackend

from databricks.labs.ucx.account.aggregate import AccountAggregate
from databricks.labs.ucx.account.workspaces import AccountWorkspaces
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.contexts.workspace_cli import WorkspaceContext


UCX_OBJECTS = MockBackend.rows("object_type", "object_id", "failures")
UCX_TABLES = MockBackend.rows("catalog", "database", "table", "object_type", "table_format", "location", "view_text")


@pytest.fixture
def ws() -> WorkspaceClient:
    workspace_client = create_autospec(WorkspaceClient)
    workspace_client.current_user.me.return_value = iam.User(
        user_name="user",
        groups=[iam.ComplexValue(display="admins")],
    )
    workspace_client.get_workspace_id.return_value = 123
    return workspace_client


@pytest.fixture
def account_client(ws, acc_client) -> AccountClient:
    workspace = Workspace(workspace_name="test", workspace_id=123)
    acc_client.workspaces.list.return_value = [workspace]
    acc_client.get_workspace_client.return_value = ws
    return acc_client


def test_basic_readiness_report_no_workspaces(caplog, ws, account_client):
    rows = UCX_OBJECTS[("tables", "34234324", '["listTables returned null"]'),]
    mock_backend = MockBackend(rows={"SELECT \\* FROM hive_metastore.ucx.objects": rows})
    ctx = WorkspaceContext(ws).replace(config=WorkspaceConfig(inventory_database="ucx"), sql_backend=mock_backend)

    account_workspaces = AccountWorkspaces(account_client)
    account_aggregate = AccountAggregate(account_workspaces, workspace_context_factory=lambda _: ctx)
    with caplog.at_level(logging.INFO, logger="databricks.labs.ucx.account.aggregate"):
        account_aggregate.readiness_report()
    assert "UC compatibility" in caplog.text


def test_readiness_report_ucx_installed(caplog, ws, account_client):
    rows = UCX_OBJECTS[
        (
            "jobs",
            "32432123",
            '["cluster type not supported : LEGACY_TABLE_ACL", "cluster type not supported : LEGACY_SINGLE_USER"]',
        ),
        ("jobs", "234234234", '["cluster type not supported : LEGACY_SINGLE_USER"]'),
        ("clusters", "21312312", '[]'),
        ("tables", "34234324", '["listTables returned null"]'),
    ]
    mock_backend = MockBackend(rows={"SELECT \\* FROM hive_metastore.ucx.objects": rows})
    ctx = WorkspaceContext(ws).replace(config=WorkspaceConfig(inventory_database="ucx"), sql_backend=mock_backend)

    account_workspaces = AccountWorkspaces(account_client)
    account_aggregate = AccountAggregate(account_workspaces, workspace_context_factory=lambda _: ctx)
    with caplog.at_level(logging.INFO, logger="databricks.labs.ucx.account.aggregate"):
        account_aggregate.readiness_report()
    assert "UC compatibility: 25.0% (3/4)" in caplog.text
    assert "cluster type not supported : LEGACY_TABLE_ACL: 1 objects" in caplog.text
    assert "cluster type not supported : LEGACY_SINGLE_USER: 2 objects" in caplog.text


@pytest.mark.parametrize(
    "rows",
    [
        [
            ("c1", "d1", "t1", "TABLE", "DELTA", "/foo", None),
            ("c2", "d1", "t1", "TABLE", "DELTA", "/bar", None),
        ],
        [
            ("c1", "d1", "t1", "TABLE", "DELTA", "/foo", None),
            ("c1", "d2", "t1", "TABLE", "DELTA", "/bar", None),
        ],
        [
            ("c1", "d1", "t1", "TABLE", "DELTA", "/foo", None),
            ("c1", "d1", "t2", "TABLE", "DELTA", "/bar", None),
        ],
        [
            ("c1", "d1", "t1", "TABLE", "DELTA", "/foo", None),
            ("c1", "d1", "t2", "TABLE", "PARQUET", "/bar", None),
        ],
        [
            ("c1", "d1", "t1", "TABLE", "DELTA", None, None),
            ("c2", "d2", "t2", "TABLE", "DELTA", "/foo", None),
        ],
        [
            ("c1", "d1", "t1", "VIEW", "DELTA", None, "SELECT * FROM c1.d1.t2"),
            ("c1", "d1", "t2", "TABLE", "DELTA", "/foo", None),
        ],
    ],
)
def test_account_aggregate_finds_no_overlapping_tables(caplog, ws, account_client, rows):
    mock_backend = MockBackend(rows={"SELECT \\* FROM hive_metastore.ucx.tables": UCX_TABLES[rows]})
    ctx = WorkspaceContext(ws).replace(config=WorkspaceConfig(inventory_database="ucx"), sql_backend=mock_backend)

    account_ws = AccountWorkspaces(account_client)
    account_aggregate = AccountAggregate(account_ws, workspace_context_factory=lambda _: ctx)
    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.account.aggregate"):
        conflicts = account_aggregate.validate_table_locations()
    assert "Overlapping table locations" not in caplog.text
    assert not conflicts


@pytest.mark.parametrize(
    "rows",
    [
        [
            ("c1", "d1", "t1", "TABLE", "DELTA", "s3://bucket/foo/bar/", None),
            ("c2", "d1", "t1", "TABLE", "DELTA", "s3://bucket/foo/bar/", None),
        ],
        [
            ("c1", "d1", "t1", "TABLE", "DELTA", "s3://bucket/foo/bar/", None),
            ("c1", "d2", "t1", "TABLE", "DELTA", "s3://bucket/foo/bar/", None),
        ],
        [
            ("c1", "d1", "t1", "TABLE", "DELTA", "s3://bucket/foo/bar/", None),
            ("c1", "d1", "t2", "TABLE", "DELTA", "s3://bucket/foo/bar/", None),
        ],
        [
            ("c1", "d1", "t1", "TABLE", "DELTA", "s3://bucket/foo/bar/", None),
            ("c1", "d1", "t2", "TABLE", "PARQUET", "s3://bucket/foo/bar/", None),
        ],
        [
            ("c2", "d2", "t2", "TABLE", "DELTA", "s3://bucket/foo/", None),
            ("c1", "d2", "t1", "TABLE", "DELTA", "s3://bucket/foo", None),
        ],
        [
            ("c1", "d1", "t1", "TABLE", "DELTA", "s3://bucket/foo/bar/fiz", None),
            ("c2", "d2", "t2", "TABLE", "DELTA", "s3://bucket/foo/bar", None),
        ],
        [
            ("c1", "d1", "t1", "TABLE", "DELTA", None, None),
            ("c2", "d2", "t2", "TABLE", "DELTA", "s3://bucket/foo", None),
            ("c3", "d3", "t3", "TABLE", "DELTA", "s3://bucket/foo", None),
        ],
    ],
)
def test_account_aggregate_finds_overlapping_tables(caplog, ws, account_client, rows):
    mock_backend = MockBackend(rows={"SELECT \\* FROM hive_metastore.ucx.tables": UCX_TABLES[rows]})
    ctx = WorkspaceContext(ws).replace(config=WorkspaceConfig(inventory_database="ucx"), sql_backend=mock_backend)

    account_ws = AccountWorkspaces(account_client)
    account_aggregate = AccountAggregate(account_ws, workspace_context_factory=lambda _: ctx)
    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.account.aggregate"):
        conflicts = account_aggregate.validate_table_locations()
    assert "Overlapping table locations" in caplog.text
    assert len(conflicts) == 1
    assert len(conflicts[0]) == 2


def test_account_aggregate_logs_multiple_overlapping_tables(caplog, ws, account_client):
    rows = UCX_TABLES[
        # It happens that a table is a partition of another table
        ("c1", "d1", "t2", "TABLE", "DELTA", "s3://bucket/foo/bar/", None),
        ("c2", "d1", "t1", "TABLE", "DELTA", "s3://bucket/foo/", None),
        ("c1", "d1", "t3", "TABLE", "DELTA", "s3://bucket/foo/fizz/", None),
    ]
    mock_backend = MockBackend(rows={"SELECT \\* FROM hive_metastore.ucx.tables": rows})
    ctx = WorkspaceContext(ws).replace(config=WorkspaceConfig(inventory_database="ucx"), sql_backend=mock_backend)

    account_ws = AccountWorkspaces(account_client)
    account_aggregate = AccountAggregate(account_ws, workspace_context_factory=lambda _: ctx)
    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.account.aggregate"):
        conflicts = account_aggregate.validate_table_locations()
    assert "Overlapping table locations: 123:c2.d1.t1 and 123:c1.d1.t2 and 123:c1.d1.t3" in caplog.text
    assert len(conflicts) == 1
    assert str(conflicts[0][0]) == "123:c2.d1.t1"
    assert str(conflicts[0][1]) == "123:c1.d1.t2"
    assert str(conflicts[0][2]) == "123:c1.d1.t3"

from unittest.mock import create_autospec, call

import pytest
from databricks.labs.lsql.backends import MockBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.iam import PermissionMigrationResponse

from databricks.labs.ucx.workspace_access.workflows import (
    RemoveWorkspaceLocalGroups,
    GroupMigration,
    PermissionsMigrationAPI,
)
from tests.unit import GROUPS, PERMISSIONS


def test_runtime_delete_backup_groups(run_workflow):
    ctx = run_workflow(RemoveWorkspaceLocalGroups.delete_backup_groups)
    assert 'SELECT * FROM hive_metastore.ucx.groups' in ctx.sql_backend.queries


def test_runtime_apply_permissions_to_account_groups(run_workflow):
    ctx = run_workflow(GroupMigration.apply_permissions_to_account_groups)
    assert 'SELECT * FROM hive_metastore.ucx.groups' in ctx.sql_backend.queries


def test_rename_workspace_local_group(run_workflow):
    ctx = run_workflow(GroupMigration.rename_workspace_local_groups)
    assert 'SELECT * FROM hive_metastore.ucx.groups' in ctx.sql_backend.queries


def test_reflect_account_groups_on_workspace(run_workflow):
    ctx = run_workflow(PermissionsMigrationAPI.reflect_account_groups_on_workspace)
    assert 'SELECT * FROM hive_metastore.ucx.groups' in ctx.sql_backend.queries


def test_migrate_permissions_experimental(run_workflow):
    rows = {
        'SELECT \\* FROM hive_metastore.ucx.groups': GROUPS[
            ("", "workspace_group_1", "account_group_1", "temp_1", "", "", "", ""),
            ("", "workspace_group_2", "account_group_2", "temp_2", "", "", "", ""),
            ("", "workspace_group_3", "account_group_3", "temp_3", "", "", "", ""),
        ],
        'SELECT COUNT\\(\\*\\) as cnt FROM hive_metastore.ucx.permissions': PERMISSIONS[("123", "QUERIES", "temp")],
    }
    ws = create_autospec(WorkspaceClient)
    ws.get_workspace_id.return_value = "12345678"
    ws.permission_migration.migrate_permissions.return_value = PermissionMigrationResponse(0)
    sql_backend = MockBackend(rows=rows)

    run_workflow(PermissionsMigrationAPI.apply_permissions, sql_backend=sql_backend, workspace_client=ws)

    calls = [
        call("12345678", "temp_1", "account_group_1", size=1000),
        call("12345678", "temp_2", "account_group_2", size=1000),
        call("12345678", "temp_3", "account_group_3", size=1000),
    ]
    ws.permission_migration.migrate_permissions.assert_has_calls(calls, any_order=True)


def test_migrate_permissions_experimental_paginated(run_workflow):
    rows = {
        'SELECT \\* FROM hive_metastore.ucx.groups': GROUPS[
            ("", "workspace_group_1", "account_group_1", "temp_1", "", "", "", ""),
            ("", "workspace_group_2", "account_group_2", "temp_2", "", "", "", ""),
            ("", "workspace_group_3", "account_group_3", "temp_3", "", "", "", ""),
        ],
        'SELECT COUNT\\(\\*\\) as cnt FROM hive_metastore.ucx.permissions': PERMISSIONS[("123", "QUERIES", "temp")],
    }
    ws = create_autospec(WorkspaceClient)
    ws.get_workspace_id.return_value = "12345678"
    ws.permission_migration.migrate_permissions.side_effect = [
        PermissionMigrationResponse(i) for i in (1000, None, 1000, 10, 0, 1000, 10, 0)
    ]
    sql_backend = MockBackend(rows=rows)

    run_workflow(PermissionsMigrationAPI.apply_permissions, sql_backend=sql_backend, workspace_client=ws)

    calls = [
        call("12345678", "temp_1", "account_group_1", size=1000),
        call("12345678", "temp_2", "account_group_2", size=1000),
        call("12345678", "temp_3", "account_group_3", size=1000),
    ]
    ws.permission_migration.migrate_permissions.assert_has_calls(calls, any_order=True)


def test_migrate_permissions_experimental_error(run_workflow):
    rows = {
        'SELECT \\* FROM hive_metastore.ucx.groups': GROUPS[
            ("", "workspace_group_1", "account_group_1", "temp_1", "", "", "", ""),
            ("", "workspace_group_2", "account_group_2", "temp_2", "", "", "", ""),
            ("", "workspace_group_3", "account_group_3", "temp_3", "", "", "", ""),
        ],
    }
    sql_backend = MockBackend(rows=rows)
    ws = create_autospec(WorkspaceClient)
    ws.get_workspace_id.return_value = "12345678"
    ws.permission_migration.migrate_permissions.side_effect = NotImplementedError("api not enabled")
    with pytest.raises(NotImplementedError):
        run_workflow(PermissionsMigrationAPI.apply_permissions, sql_backend=sql_backend, workspace_client=ws)

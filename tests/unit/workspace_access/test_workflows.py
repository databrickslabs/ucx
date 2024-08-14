import logging
from unittest.mock import create_autospec, call

import pytest
from databricks.labs.blueprint.parallel import ManyError
from databricks.labs.lsql.backends import MockBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError
from databricks.sdk.service.iam import MigratePermissionsResponse

from databricks.labs.ucx.workspace_access.groups import GroupManager
from databricks.labs.ucx.workspace_access.workflows import (
    RemoveWorkspaceLocalGroups,
    GroupMigration,
    PermissionsMigrationAPI,
)
from tests.unit import GROUPS, PERMISSIONS


def test_runtime_delete_backup_groups(run_workflow) -> None:
    ctx = run_workflow(RemoveWorkspaceLocalGroups.delete_backup_groups)
    assert 'SELECT * FROM hive_metastore.ucx.groups' in ctx.sql_backend.queries


def test_runtime_apply_permissions_to_account_groups(run_workflow) -> None:
    ctx = run_workflow(GroupMigration.apply_permissions_to_account_groups)
    assert 'SELECT * FROM hive_metastore.ucx.groups' in ctx.sql_backend.queries


def test_rename_workspace_local_group(run_workflow) -> None:
    ctx = run_workflow(GroupMigration.rename_workspace_local_groups)
    assert 'SELECT * FROM hive_metastore.ucx.groups' in ctx.sql_backend.queries


def test_reflect_account_groups_on_workspace(run_workflow) -> None:
    ctx = run_workflow(PermissionsMigrationAPI.reflect_account_groups_on_workspace)
    assert 'SELECT * FROM hive_metastore.ucx.groups' in ctx.sql_backend.queries


def test_migrate_permissions_experimental(run_workflow) -> None:
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
    ws.permission_migration.migrate_permissions.return_value = MigratePermissionsResponse(0)
    sql_backend = MockBackend(rows=rows)

    run_workflow(PermissionsMigrationAPI.apply_permissions, sql_backend=sql_backend, workspace_client=ws)

    calls = [
        call("12345678", "temp_1", "account_group_1", size=1000),
        call("12345678", "temp_2", "account_group_2", size=1000),
        call("12345678", "temp_3", "account_group_3", size=1000),
    ]
    ws.permission_migration.migrate_permissions.assert_has_calls(calls, any_order=True)


def test_migrate_permissions_experimental_paginated(run_workflow) -> None:
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
        MigratePermissionsResponse(i) for i in (1000, None, 1000, 10, 0, 1000, 10, 0)
    ]
    sql_backend = MockBackend(rows=rows)

    run_workflow(PermissionsMigrationAPI.apply_permissions, sql_backend=sql_backend, workspace_client=ws)

    calls = [
        call("12345678", "temp_1", "account_group_1", size=1000),
        call("12345678", "temp_2", "account_group_2", size=1000),
        call("12345678", "temp_3", "account_group_3", size=1000),
    ]
    ws.permission_migration.migrate_permissions.assert_has_calls(calls, any_order=True)


def test_migrate_permissions_not_enabled_error(run_workflow) -> None:
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


def test_migrate_permissions_continue_on_error(run_workflow, caplog) -> None:
    """Check that permission migration continues for other groups even if it fails for a single group."""
    rows = {
        'SELECT \\* FROM hive_metastore.ucx.groups': GROUPS[
            ("", "workspace_group_1", "account_group_1", "temp_1", "", "", "", ""),  # Will fail immediately.
            ("", "workspace_group_2", "account_group_2", "temp_2", "", "", "", ""),  # Will fail midway.
            ("", "workspace_group_3", "account_group_3", "temp_3", "", "", "", ""),  # Will succeed.
        ],
    }
    sql_backend = MockBackend(rows=rows)
    ws = create_autospec(WorkspaceClient)
    ws.get_workspace_id.return_value = "12345678"
    ws.permission_migration.migrate_permissions.side_effect = [
        # First group: fails immediately.
        DatabricksError("simulate group failure: immediately"),
        # Second group; fails mid-migration.
        MigratePermissionsResponse(permissions_migrated=10),
        DatabricksError("simulate group failure: midway"),
        # Third group.
        MigratePermissionsResponse(permissions_migrated=50),
        MigratePermissionsResponse(permissions_migrated=0),
    ]

    with pytest.raises(ManyError) as exc_info, caplog.at_level(logging.INFO):
        run_workflow(PermissionsMigrationAPI.apply_permissions, sql_backend=sql_backend, workspace_client=ws)

    raised_exception = exc_info.value
    assert len(raised_exception.errs) == 2
    expected_exceptions = {"simulate group failure: immediately", "simulate group failure: midway"}
    assert {str(e) for e in raised_exception.errs} == expected_exceptions
    assert "failed-group-migration: temp_1 -> account_group_1: simulate group failure: immediately" in caplog.text
    assert "failed-group-migration: temp_2 -> account_group_2: simulate group failure: midway" in caplog.text
    assert "Migrated 50 permissions for 1/3 groups successfully." in caplog.messages
    assert "Migrating permissions failed for 2/3 groups." in caplog.messages


def test_migrate_permissions_non_raised_error(run_workflow, migration_state, mocker) -> None:
    """The internal API for permission migration can report failure via the return value; verify this fails the task."""

    # Set up the mocking plumbing.
    mock_gm = create_autospec(GroupManager)
    mock_gm.get_migration_state.return_value = migration_state

    # Set up the injected "failure" where we return false without raising a specific error.
    migration_state.apply_to_renamed_groups = mocker.Mock(return_value=False)

    # Run the test.
    with pytest.raises(RuntimeError) as exc_info:
        run_workflow(PermissionsMigrationAPI.apply_permissions, group_manager=mock_gm)

    # Verify the migration failure was converted into a task exception.
    assert str(exc_info.value) == "Permission migration for groups failed; reason unknown."

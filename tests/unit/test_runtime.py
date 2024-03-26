import os.path
import sys
from unittest.mock import call, create_autospec, patch

import pytest
from databricks.labs.blueprint.installation import MockInstallation
from databricks.labs.lsql.backends import MockBackend, SqlBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config
from databricks.sdk.service.iam import PermissionMigrationResponse

from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.framework.tasks import (  # pylint: disable=import-private-name
    _TASKS,
    Task,
)
from databricks.labs.ucx.runtime import (
    apply_permissions_to_account_groups,
    assess_azure_service_principals,
    crawl_grants,
    migrate_dbfs_root_delta_tables,
    migrate_external_tables_sync,
)
from tests.unit import GROUPS


def azure_mock_config() -> WorkspaceConfig:
    config = WorkspaceConfig(
        connect=Config(
            host="adb-9999999999999999.14.azuredatabricks.net",
            token="dapifaketoken",
        ),
        inventory_database="ucx",
        use_permission_migration_api=True,
    )
    return config


def mock_installation() -> MockInstallation:
    return MockInstallation(
        {
            'mapping.csv': [
                {
                    'catalog_name': 'catalog',
                    'dst_schema': 'schema',
                    'dst_table': 'table',
                    'src_schema': 'schema',
                    'src_table': 'table',
                    'workspace_name': 'workspace',
                },
            ]
        }
    )


def test_azure_crawler(mocker):
    with patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "14.0"}):
        pyspark_sql_session = mocker.Mock()
        sys.modules["pyspark.sql.session"] = pyspark_sql_session
        cfg = azure_mock_config()

        ws = create_autospec(WorkspaceClient)
        sql_backend = create_autospec(SqlBackend)
        sql_backend.fetch.return_value = [
            ["1", "secret_scope", "secret_key", "tenant_id", "storage_account"],
        ]
        assess_azure_service_principals(cfg, ws, sql_backend, mock_installation())


def test_tasks():
    tasks = [
        Task(task_id=0, workflow="wl_1", name="n3", doc="d3", fn=lambda: None, cloud="azure"),
        Task(task_id=1, workflow="wl_2", name="n2", doc="d2", fn=lambda: None, cloud="aws"),
        Task(task_id=2, workflow="wl_1", name="n1", doc="d1", fn=lambda: None, cloud="gcp"),
    ]

    assert len([_ for _ in tasks if _.cloud == "azure"]) == 1
    assert len([_ for _ in tasks if _.cloud == "aws"]) == 1
    assert len([_ for _ in tasks if _.cloud == "gcp"]) == 1


def test_assessment_tasks():
    """Test task decorator"""
    assert len(_TASKS) >= 19
    azure = [v for k, v in _TASKS.items() if v.cloud == "azure"]
    assert len(azure) >= 1


def test_runtime_grants(mocker):
    with patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "14.0"}):
        pyspark_sql_session = mocker.Mock()
        sys.modules["pyspark.sql.session"] = pyspark_sql_session
        cfg = azure_mock_config()
        ws = create_autospec(WorkspaceClient)
        sql_backend = MockBackend()
        crawl_grants(cfg, ws, sql_backend, mock_installation())

        assert "SHOW DATABASES FROM hive_metastore" in sql_backend.queries
        assert "SHOW DATABASES" in sql_backend.queries


def test_migrate_external_tables_sync():
    ws = create_autospec(WorkspaceClient)
    migrate_external_tables_sync(azure_mock_config(), ws, MockBackend(), mock_installation())
    ws.catalogs.list.assert_called_once()


def test_migrate_dbfs_root_delta_tables():
    ws = create_autospec(WorkspaceClient)
    migrate_dbfs_root_delta_tables(azure_mock_config(), ws, MockBackend(), mock_installation())
    ws.catalogs.list.assert_called_once()


def test_migrate_permissions_private_api():
    rows = {
        'SELECT \\* FROM hive_metastore.ucx.groups': GROUPS[
            ("", "workspace_group_1", "account_group_1", "temp_1", "", "", "", ""),
            ("", "workspace_group_2", "account_group_2", "temp_2", "", "", "", ""),
            ("", "workspace_group_3", "account_group_3", "temp_3", "", "", "", ""),
        ],
    }
    ws = create_autospec(WorkspaceClient)
    ws.get_workspace_id.return_value = "12345678"
    ws.permission_migration.migrate_permissions.return_value = PermissionMigrationResponse(0)
    apply_permissions_to_account_groups(azure_mock_config(), ws, MockBackend(rows=rows), mock_installation())
    calls = [
        call("12345678", "workspace_group_1", "account_group_1", size=1000),
        call("12345678", "workspace_group_2", "account_group_2", size=1000),
        call("12345678", "workspace_group_3", "account_group_3", size=1000),
    ]
    ws.permission_migration.migrate_permissions.assert_has_calls(calls, any_order=True)


def test_migrate_permissions_private_api_paginated():
    rows = {
        'SELECT \\* FROM hive_metastore.ucx.groups': GROUPS[
            ("", "workspace_group_1", "account_group_1", "temp_1", "", "", "", ""),
            ("", "workspace_group_2", "account_group_2", "temp_2", "", "", "", ""),
            ("", "workspace_group_3", "account_group_3", "temp_3", "", "", "", ""),
        ],
    }
    ws = create_autospec(WorkspaceClient)
    ws.get_workspace_id.return_value = "12345678"
    ws.permission_migration.migrate_permissions.side_effect = [
        PermissionMigrationResponse(i) for i in (1000, None, 1000, 10, 0, 1000, 10, 0)
    ]
    apply_permissions_to_account_groups(azure_mock_config(), ws, MockBackend(rows=rows), mock_installation())
    calls = [
        call("12345678", "workspace_group_1", "account_group_1", size=1000),
        call("12345678", "workspace_group_2", "account_group_2", size=1000),
        call("12345678", "workspace_group_3", "account_group_3", size=1000),
    ]
    ws.permission_migration.migrate_permissions.assert_has_calls(calls, any_order=True)


def test_migrate_permissions_private_api_error(caplog):
    rows = {
        'SELECT \\* FROM hive_metastore.ucx.groups': GROUPS[
            ("", "workspace_group_1", "account_group_1", "temp_1", "", "", "", ""),
            ("", "workspace_group_2", "account_group_2", "temp_2", "", "", "", ""),
            ("", "workspace_group_3", "account_group_3", "temp_3", "", "", "", ""),
        ],
    }
    ws = create_autospec(WorkspaceClient)
    ws.get_workspace_id.return_value = "12345678"
    ws.permission_migration.migrate_permissions.side_effect = RuntimeError("internal error")
    with pytest.raises(RuntimeError):
        apply_permissions_to_account_groups(azure_mock_config(), ws, MockBackend(rows=rows), mock_installation())
    assert "Detected 3 errors while applying permissions" in caplog.messages

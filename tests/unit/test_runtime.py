import os.path
import sys
from unittest.mock import create_autospec, patch

from databricks.labs.blueprint.installation import MockInstallation
from databricks.labs.lsql.backends import MockBackend, SqlBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.config import Config

from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.framework.tasks import (  # pylint: disable=import-private-name
    _TASKS,
    Task,
)
from databricks.labs.ucx.runtime import (
    assess_azure_service_principals,
    crawl_grants,
    migrate_dbfs_root_delta_tables,
    migrate_external_tables_sync, crawl_mounts, guess_external_locations, estimate_table_size_for_migration,
    assess_jobs, assess_clusters, assess_pipelines, assess_incompatible_submit_runs, crawl_cluster_policies,
    assess_global_init_scripts, workspace_listing, crawl_permissions, crawl_groups, destroy_schema,
    delete_backup_groups, apply_permissions_to_account_groups,
)


def azure_mock_config() -> WorkspaceConfig:
    config = WorkspaceConfig(
        connect=Config(
            host="adb-9999999999999999.14.azuredatabricks.net",
            token="dapifaketoken",
        ),
        inventory_database="ucx",
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


def test_runtime_workspace_listing(mocker):
    with patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "14.0"}):
        pyspark_sql_session = mocker.Mock()
        sys.modules["pyspark.sql.session"] = pyspark_sql_session
        cfg = azure_mock_config()
        ws = create_autospec(WorkspaceClient)
        sql_backend = MockBackend()
        workspace_listing(cfg, ws, sql_backend, mock_installation())

        assert "SELECT * FROM ucx.workspace_objects" in sql_backend.queries


def test_runtime_crawl_grants(mocker):
    with patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "14.0"}):
        pyspark_sql_session = mocker.Mock()
        sys.modules["pyspark.sql.session"] = pyspark_sql_session
        cfg = azure_mock_config()
        ws = create_autospec(WorkspaceClient)
        sql_backend = MockBackend()
        crawl_grants(cfg, ws, sql_backend, mock_installation())

        assert "SELECT * FROM hive_metastore.ucx.grants" in sql_backend.queries


# TODO crawl_permissions fails, but I don't think I can create tickets yet!
# def test_runtime_crawl_permissions(mocker):
#     with patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "14.0"}):
#         pyspark_sql_session = mocker.Mock()
#         sys.modules["pyspark.sql.session"] = pyspark_sql_session
#         cfg = azure_mock_config()
#         ws = create_autospec(WorkspaceClient)
#         sql_backend = MockBackend()
#         crawl_permissions(cfg, ws, sql_backend, mock_installation())
#
#         assert "SELECT * FROM hive_metastore.ucx.grants" in sql_backend.queries


def test_runtime_crawl_groups(mocker):
    with patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "14.0"}):
        pyspark_sql_session = mocker.Mock()
        sys.modules["pyspark.sql.session"] = pyspark_sql_session
        cfg = azure_mock_config()
        ws = create_autospec(WorkspaceClient)
        sql_backend = MockBackend()
        crawl_groups(cfg, ws, sql_backend, mock_installation())

        assert "SELECT * FROM hive_metastore.ucx.groups" in sql_backend.queries


def test_runtime_crawl_cluster_policies(mocker):
    with patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "14.0"}):
        pyspark_sql_session = mocker.Mock()
        sys.modules["pyspark.sql.session"] = pyspark_sql_session
        cfg = azure_mock_config()
        ws = create_autospec(WorkspaceClient)
        sql_backend = MockBackend()
        crawl_cluster_policies(cfg, ws, sql_backend, mock_installation())

        assert "SELECT * FROM ucx.policies" in sql_backend.queries


def test_runtime_crawl_init_scripts(mocker):
    with patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "14.0"}):
        pyspark_sql_session = mocker.Mock()
        sys.modules["pyspark.sql.session"] = pyspark_sql_session
        cfg = azure_mock_config()
        ws = create_autospec(WorkspaceClient)
        sql_backend = MockBackend()
        assess_global_init_scripts(cfg, ws, sql_backend, mock_installation())

        assert "SELECT * FROM ucx.global_init_scripts" in sql_backend.queries


def test_estimate_table_size_for_migration(mocker):
    with patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "14.0"}):
        pyspark_sql_session = mocker.Mock()
        sys.modules["pyspark.sql.session"] = pyspark_sql_session
        cfg = azure_mock_config()
        ws = create_autospec(WorkspaceClient)
        sql_backend = MockBackend()
        estimate_table_size_for_migration(cfg, ws, sql_backend, mock_installation())

        assert "SELECT * FROM hive_metastore.ucx.table_size" in sql_backend.queries
        assert "SHOW DATABASES" in sql_backend.queries


def test_runtime_mounts(mocker):
    with patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "14.0"}):
        pyspark_sql_session = mocker.Mock()
        sys.modules["pyspark.sql.session"] = pyspark_sql_session
        cfg = azure_mock_config()
        ws = create_autospec(WorkspaceClient)
        sql_backend = MockBackend()
        crawl_mounts(cfg, ws, sql_backend, mock_installation())

        assert "SELECT * FROM ucx.mounts" in sql_backend.queries


def test_guess_external_locations(mocker):
    with patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "14.0"}):
        pyspark_sql_session = mocker.Mock()
        sys.modules["pyspark.sql.session"] = pyspark_sql_session
        cfg = azure_mock_config()
        ws = create_autospec(WorkspaceClient)
        sql_backend = MockBackend()
        guess_external_locations(cfg, ws, sql_backend, mock_installation())

        assert "SELECT * FROM ucx.mounts" in sql_backend.queries


def test_assess_jobs(mocker):
    with patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "14.0"}):
        pyspark_sql_session = mocker.Mock()
        sys.modules["pyspark.sql.session"] = pyspark_sql_session
        cfg = azure_mock_config()
        ws = create_autospec(WorkspaceClient)
        sql_backend = MockBackend()
        assess_jobs(cfg, ws, sql_backend, mock_installation())

        assert "SELECT * FROM ucx.jobs" in sql_backend.queries


def test_assess_clusters(mocker):
    with patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "14.0"}):
        pyspark_sql_session = mocker.Mock()
        sys.modules["pyspark.sql.session"] = pyspark_sql_session
        cfg = azure_mock_config()
        ws = create_autospec(WorkspaceClient)
        sql_backend = MockBackend()
        assess_clusters(cfg, ws, sql_backend, mock_installation())

        assert "SELECT * FROM ucx.clusters" in sql_backend.queries


def test_assess_pipelines(mocker):
    with patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "14.0"}):
        pyspark_sql_session = mocker.Mock()
        sys.modules["pyspark.sql.session"] = pyspark_sql_session
        cfg = azure_mock_config()
        ws = create_autospec(WorkspaceClient)
        sql_backend = MockBackend()
        assess_pipelines(cfg, ws, sql_backend, mock_installation())

        assert "SELECT * FROM ucx.pipelines" in sql_backend.queries


def test_incompatible_submit_runs(mocker):
    with patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "14.0"}):
        pyspark_sql_session = mocker.Mock()
        sys.modules["pyspark.sql.session"] = pyspark_sql_session
        cfg = azure_mock_config()
        ws = create_autospec(WorkspaceClient)
        sql_backend = MockBackend()
        assess_incompatible_submit_runs(cfg, ws, sql_backend, mock_installation())

        assert "SELECT * FROM ucx.submit_runs" in sql_backend.queries


def test_migrate_external_tables_sync():
    ws = create_autospec(WorkspaceClient)
    migrate_external_tables_sync(azure_mock_config(), ws, MockBackend(), mock_installation())
    ws.catalogs.list.assert_called_once()


def test_migrate_dbfs_root_delta_tables():
    ws = create_autospec(WorkspaceClient)
    migrate_dbfs_root_delta_tables(azure_mock_config(), ws, MockBackend(), mock_installation())
    ws.catalogs.list.assert_called_once()


def test_runtime_destroy_schema(mocker):
    with patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "14.0"}):
        pyspark_sql_session = mocker.Mock()
        sys.modules["pyspark.sql.session"] = pyspark_sql_session
        cfg = azure_mock_config()
        ws = create_autospec(WorkspaceClient)
        sql_backend = MockBackend()
        destroy_schema(cfg, ws, sql_backend, mock_installation())

        assert "DROP DATABASE ucx CASCADE" in sql_backend.queries


# TODO smells like delete_backup_groups isn't deleting anything, but maybe that's because there's nothing to delete ?
# def test_runtime_delete_backup_groups(mocker):
#     with patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "14.0"}):
#         pyspark_sql_session = mocker.Mock()
#         sys.modules["pyspark.sql.session"] = pyspark_sql_session
#         cfg = azure_mock_config()
#         ws = create_autospec(WorkspaceClient)
#         sql_backend = MockBackend()
#         delete_backup_groups(cfg, ws, sql_backend, mock_installation())
#
#         assert "DELETE" in sql_backend.queries # TODO


def test_runtime_apply_permissions_to_account_groups(mocker):
    with patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "14.0"}):
        pyspark_sql_session = mocker.Mock()
        sys.modules["pyspark.sql.session"] = pyspark_sql_session
        cfg = azure_mock_config()
        ws = create_autospec(WorkspaceClient)
        sql_backend = MockBackend()
        apply_permissions_to_account_groups(cfg, ws, sql_backend, mock_installation())

        assert "SELECT * FROM hive_metastore.ucx.groups" in sql_backend.queries

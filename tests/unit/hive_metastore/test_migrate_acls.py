import logging
from unittest.mock import create_autospec
import pytest
from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.account.workspaces import WorkspaceInfo
from databricks.labs.ucx.hive_metastore.grants import MigrateGrants, ACLMigrator, Grant
from databricks.labs.ucx.hive_metastore.migration_status import (
    MigrationStatusRefresher,
    MigrationIndex,
)
from databricks.labs.ucx.hive_metastore.tables import TablesCrawler, Table
from databricks.labs.ucx.workspace_access.groups import GroupManager, MigratedGroup

logger = logging.getLogger(__name__)


@pytest.fixture
def ws():
    client = create_autospec(WorkspaceClient)
    client.get_workspace_id.return_value = "12345"
    return client


@pytest.fixture
def ws_info():
    info = create_autospec(WorkspaceInfo)
    info.current.return_value = "hms_fed"
    return info


def test_migrate_acls_should_produce_proper_queries(ws, ws_info, caplog):
    table_crawler = create_autospec(TablesCrawler)
    src = Table('hive_metastore', 'db1_src', 'view_src', 'VIEW', 'UNKNOWN')
    table_crawler.snapshot.return_value = [src]

    workspace_info = ws_info
    migration_status_refresher = create_autospec(MigrationStatusRefresher)

    migrate_grants = create_autospec(MigrateGrants)
    acl_migrate = ACLMigrator(
        table_crawler,
        workspace_info,
        migration_status_refresher,
        migrate_grants,
    )
    migration_status_refresher.get_seen_tables.return_value = {
        "ucx_default.db1_dst.view_dst": "hive_metastore.db1_src.view_src",
    }
    acl_migrate.migrate_acls()
    migrate_grants.apply.assert_called_with(src, 'ucx_default.db1_dst.view_dst')


def test_migrate_acls_hms_fed_proper_queries(ws, ws_info, caplog):
    table_crawler = create_autospec(TablesCrawler)
    src = Table('hive_metastore', 'db1_src', 'managed_dbfs', 'TABLE', 'DELTA', "/foo/bar/test")
    table_crawler.snapshot.return_value = [src]
    workspace_info = ws_info
    migrate_grants = create_autospec(MigrateGrants)

    migration_index = create_autospec(MigrationIndex)
    migration_index.is_migrated.return_value = True

    migration_status_refresher = create_autospec(MigrationStatusRefresher)
    migration_status_refresher.get_seen_tables.return_value = {
        "ucx_default.db1_dst.managed_dbfs": "hive_metastore.db1_src.managed_dbfs",
    }
    migration_status_refresher.index.return_value = migration_index

    acl_migrate = ACLMigrator(
        table_crawler,
        workspace_info,
        migration_status_refresher,
        migrate_grants,
    )
    acl_migrate.migrate_acls(hms_fed=True)

    migrate_grants.apply.assert_called_with(src, 'hms_fed.db1_src.managed_dbfs')


def test_migrate_matched_grants_applies():
    sql_backend = create_autospec(SqlBackend)
    group_manager = create_autospec(GroupManager)
    src = Table('hive_metastore', 'default', 'foo', 'MANAGED', 'DELTA')
    one_grant = [lambda: [Grant('me', 'SELECT', database='default', table='foo')]]

    migrate_grants = MigrateGrants(sql_backend, group_manager, one_grant)
    migrate_grants.apply(src, 'catalog.schema.table')

    group_manager.snapshot.assert_called()
    sql_backend.execute.assert_called_with('GRANT SELECT ON TABLE catalog.schema.table TO `me`')


def test_migrate_matched_grants_applies_and_remaps_group():
    sql_backend = create_autospec(SqlBackend)
    group_manager = create_autospec(GroupManager)
    group_manager.snapshot.return_value = [
        MigratedGroup(
            name_in_workspace='me',
            name_in_account='myself',
            id_in_workspace='..',
            temporary_name='..',
        ),
    ]
    src = Table('hive_metastore', 'default', 'foo', 'MANAGED', 'DELTA')
    one_grant = [lambda: [Grant('me', 'SELECT', database='default', table='foo')]]

    migrate_grants = MigrateGrants(sql_backend, group_manager, one_grant)
    migrate_grants.apply(src, 'catalog.schema.table')

    group_manager.snapshot.assert_called()
    sql_backend.execute.assert_called_with('GRANT SELECT ON TABLE catalog.schema.table TO `myself`')


def test_migrate_no_matched_grants_no_apply():
    sql_backend = create_autospec(SqlBackend)
    group_manager = create_autospec(GroupManager)
    src = Table('hive_metastore', 'default', 'bar', 'MANAGED', 'DELTA')
    one_grant = [lambda: [Grant('me', 'SELECT', database='default', table='foo')]]

    migrate_grants = MigrateGrants(sql_backend, group_manager, one_grant)
    migrate_grants.apply(src, 'catalog.schema.table')

    group_manager.snapshot.assert_not_called()
    sql_backend.execute.assert_not_called()

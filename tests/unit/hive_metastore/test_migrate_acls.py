import logging
from collections.abc import Callable, Iterable
from unittest.mock import create_autospec

import pytest
from databricks.labs.lsql.backends import SqlBackend

from databricks.labs.ucx.account.workspaces import WorkspaceInfo
from databricks.labs.ucx.hive_metastore.grants import MigrateGrants, ACLMigrator, Grant
from databricks.labs.ucx.hive_metastore.table_migration_status import (
    TableMigrationStatusRefresher,
    TableMigrationIndex,
)
from databricks.labs.ucx.hive_metastore.tables import TablesCrawler, Table
from databricks.labs.ucx.workspace_access.groups import GroupManager, MigratedGroup

logger = logging.getLogger(__name__)


@pytest.fixture
def ws_info():
    info = create_autospec(WorkspaceInfo)
    info.current.return_value = "hms_fed"
    return info


def test_migrate_acls_should_produce_proper_queries(ws, ws_info, mock_backend, caplog):
    table_crawler = create_autospec(TablesCrawler)
    src = Table('hive_metastore', 'db1_src', 'view_src', 'VIEW', 'UNKNOWN')
    dst = Table('ucx_default', 'db1_dst', 'view_dst', 'VIEW', 'UNKNOWN')
    table_crawler.snapshot.return_value = [src]

    workspace_info = ws_info
    migration_status_refresher = create_autospec(TableMigrationStatusRefresher)

    migrate_grants = create_autospec(MigrateGrants)
    acl_migrate = ACLMigrator(
        table_crawler, workspace_info, migration_status_refresher, migrate_grants, mock_backend, "ucx"
    )
    migration_status_refresher.get_seen_tables.return_value = {
        "ucx_default.db1_dst.view_dst": "hive_metastore.db1_src.view_src",
    }
    acl_migrate.migrate_acls()
    migrate_grants.apply.assert_called_with(src, dst)


def test_migrate_acls_hms_fed_proper_queries(ws, ws_info, mock_backend, caplog):
    table_crawler = create_autospec(TablesCrawler)
    src = Table('hive_metastore', 'db1_src', 'managed_dbfs', 'TABLE', 'DELTA', "/foo/bar/test")
    dst = Table('hms_fed', 'db1_src', 'managed_dbfs', 'TABLE', 'DELTA', "/foo/bar/test")

    table_crawler.snapshot.return_value = [src]
    workspace_info = ws_info
    migrate_grants = create_autospec(MigrateGrants)

    migration_index = create_autospec(TableMigrationIndex)
    migration_index.is_migrated.return_value = True

    migration_status_refresher = create_autospec(TableMigrationStatusRefresher)
    migration_status_refresher.get_seen_tables.return_value = {
        "ucx_default.db1_dst.managed_dbfs": "hive_metastore.db1_src.managed_dbfs",
    }
    migration_status_refresher.index.return_value = migration_index

    acl_migrate = ACLMigrator(
        table_crawler,
        workspace_info,
        migration_status_refresher,
        migrate_grants,
        mock_backend,
        "ucx",
    )
    acl_migrate.migrate_acls(hms_fed=True, target_catalog='hms_fed')

    migrate_grants.apply.assert_called_with(src, dst)


def test_tacl_crawler(ws, ws_info, caplog):
    table_crawler = create_autospec(TablesCrawler)
    sql_backend = create_autospec(SqlBackend)
    src = [
        Table('hive_metastore', 'db1_src', 'table1', 'TABLE', 'DELTA', "/foo/bar/table1"),
        Table('hive_metastore', 'db1_src', 'table2', 'TABLE', 'DELTA', "/foo/bar/table2"),
    ]
    table_crawler.snapshot.return_value = src
    workspace_info = ws_info

    user_grants = [
        Grant('user1', 'SELECT', database='db1_src', table='table1'),
        Grant('user2', 'MODIFY', database='db1_src', table='table1'),
        Grant('user1', 'SELECT', database='db1_src', table='table2'),
        Grant('user2', 'MODIFY', database='db1_src', table='table2'),
        Grant('user1', 'SELECT', database='db1_src', table='table2'),
    ]

    group_grants = [
        Grant('group1', 'SELECT', database='db1_src', table='table1'),
    ]

    def grant_loader():
        return user_grants + group_grants

    group_manager = create_autospec(GroupManager)
    group_manager.snapshot.return_value = [
        MigratedGroup(
            name_in_workspace='group1',
            name_in_account='acc_group1',
            id_in_workspace='123',
            temporary_name='temp_group1',
        ),
    ]
    migrate_grants = MigrateGrants(sql_backend, group_manager, [grant_loader])

    migration_index = create_autospec(TableMigrationIndex)
    migration_index.is_migrated.return_value = True

    migration_status_refresher = create_autospec(TableMigrationStatusRefresher)
    migration_status_refresher.get_seen_tables.return_value = {
        "ucx_default.db1_dst.table1": "hive_metastore.db1_src.table1",
        "ucx_default.db1_dst.table2": "hive_metastore.db1_src.table2",
    }
    migration_status_refresher.index.return_value = migration_index

    acl_migrate = ACLMigrator(
        table_crawler, workspace_info, migration_status_refresher, migrate_grants, sql_backend, "ucx"
    )
    tacls = acl_migrate.snapshot()
    sql_backend.fetch.assert_called_with('SELECT * FROM `hive_metastore`.`ucx`.`hms_table_access`')
    for grant in user_grants:
        assert grant in tacls
    assert Grant('acc_group1', 'SELECT', database='db1_src', table='table1') in tacls


def test_migrate_matched_grants_applies() -> None:
    sql_backend = create_autospec(SqlBackend)
    group_manager = create_autospec(GroupManager)
    src = Table('hive_metastore', 'default', 'foo', 'MANAGED', 'DELTA')
    dst = Table('catalog', 'schema', 'table', 'MANAGED', 'DELTA')
    one_grant: list[Callable[[], Iterable[Grant]]] = [lambda: [Grant('me', 'SELECT', database='default', table='foo')]]

    migrate_grants = MigrateGrants(sql_backend, group_manager, one_grant)
    migrate_grants.apply(src, dst)

    group_manager.snapshot.assert_called()
    sql_backend.execute.assert_called_with('GRANT SELECT ON TABLE `catalog`.`schema`.`table` TO `me`')


def test_migrate_matched_grants_applies_and_remaps_group() -> None:
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
    dst = Table('catalog', 'schema', 'table', 'MANAGED', 'DELTA')
    one_grant: list[Callable[[], Iterable[Grant]]] = [lambda: [Grant('me', 'SELECT', database='default', table='foo')]]

    migrate_grants = MigrateGrants(sql_backend, group_manager, one_grant)
    migrate_grants.apply(src, dst)

    group_manager.snapshot.assert_called()
    sql_backend.execute.assert_called_with('GRANT SELECT ON TABLE `catalog`.`schema`.`table` TO `myself`')


def test_migrate_no_matched_grants_no_apply() -> None:
    sql_backend = create_autospec(SqlBackend)
    group_manager = create_autospec(GroupManager)
    src = Table('hive_metastore', 'default', 'bar', 'MANAGED', 'DELTA')
    dst = Table('catalog', 'schema', 'table', 'MANAGED', 'DELTA')
    one_grant: list[Callable[[], Iterable[Grant]]] = [lambda: [Grant('me', 'SELECT', database='default', table='foo')]]

    migrate_grants = MigrateGrants(sql_backend, group_manager, one_grant)
    migrate_grants.apply(src, dst)

    group_manager.snapshot.assert_not_called()
    sql_backend.execute.assert_not_called()

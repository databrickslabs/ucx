import json

from databricks.sdk.service.iam import Group

from databricks.labs.ucx.hive_metastore import GrantsCrawler, TablesCrawler
from databricks.labs.ucx.workspace_access.base import Permissions
from databricks.labs.ucx.workspace_access.groups import (
    GroupMigrationState,
    MigrationGroupInfo,
)
from databricks.labs.ucx.workspace_access.tacl import TableAclSupport

from ..framework.mocks import MockBackend


def test_tacl_crawler():
    sql_backend = MockBackend(
        rows={
            "SELECT \\* FROM hive_metastore.test.grants": [
                ("foo@example.com", "SELECT", "catalog_a", "database_b", "table_c", None, False, False)
            ]
        }
    )
    tables_crawler = TablesCrawler(sql_backend, "test")
    grants_crawler = GrantsCrawler(tables_crawler)
    table_acl_support = TableAclSupport(grants_crawler, sql_backend)

    crawler_tasks = table_acl_support.get_crawler_tasks()
    first_task = next(crawler_tasks)
    x = first_task()

    assert "TABLE" == x.object_type
    assert "catalog_a.database_b.table_c" == x.object_id


def test_tacl_applier(mocker):
    sql_backend = MockBackend()
    table_acl_support = TableAclSupport(mocker.Mock(), sql_backend)

    permissions = Permissions(
        object_type="TABLE",
        object_id="catalog_a.database_b.table_c",
        raw=json.dumps(
            {
                "principal": "abc",
                "action_type": "SELECT",
                "catalog": "catalog_a",
                "database": "database_b",
                "table": "table_c",
            }
        ),
    )
    migration_state = GroupMigrationState()
    migration_state.add(Group(display_name="abc"), Group(display_name="tmp-backup-abc"), Group(display_name="account-abc"))
    task = table_acl_support.get_apply_task(permissions, migration_state, "backup")
    task()

    assert ["GRANT SELECT ON TABLE catalog_a.database_b.table_c TO `tmp-backup-abc`"] == sql_backend.queries


def test_tacl_applier_no_target_principal(mocker):
    sql_backend = MockBackend()
    table_acl_support = TableAclSupport(mocker.Mock(), sql_backend)

    permissions = Permissions(
        object_type="TABLE",
        object_id="catalog_a.database_b.table_c",
        raw=json.dumps(
            {
                "principal": "foo@example.com",
                "action_type": "SELECT",
                "catalog": "catalog_a",
                "database": "database_b",
                "table": "table_c",
            }
        ),
    )
    migration_state = GroupMigrationState()
    migration_state.add(
        Group(display_name="abc"),
        Group(display_name="tmp-backup-abc"),
        Group(display_name="account-abc"))
    task = table_acl_support.get_apply_task(permissions, migration_state, "backup")
    assert task is None

    assert [] == sql_backend.queries

import json

from databricks.labs.ucx.hive_metastore import GrantsCrawler, TablesCrawler
from databricks.labs.ucx.hive_metastore.grants import Grant
from databricks.labs.ucx.workspace_access.base import Permissions
from databricks.labs.ucx.workspace_access.groups import MigratedGroup, MigrationState
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


def test_tacl_crawler_multiple_permissions():
    sql_backend = MockBackend(
        rows={
            "SELECT \\* FROM hive_metastore.test.grants": [
                ("foo@example.com", "SELECT", "catalog_a", "database_b", "table_c", None, False, False),
                ("foo@example.com", "MODIFY", "catalog_a", "database_b", "table_c", None, False, False),
                ("foo@example.com", "OWN", "catalog_a", "database_b", "table_c", None, False, False),
                # different table name (object_id)
                ("foo@example.com", "SELECT", "catalog_a", "database_b", "table_d", None, False, False),
                # different principal
                ("foo2@example.com", "SELECT", "catalog_a", "database_b", "table_c", None, False, False),
                # duplicate
                ("foo2@example.com", "SELECT", "catalog_a", "database_b", "table_c", None, False, False),
                # view
                ("foo3@example.com", "SELECT", "catalog_a", "database_b", None, "view_c", False, False),
                # database
                ("foo3@example.com", "SELECT", "catalog_a", "database_b", None, None, False, False),
                # catalog
                ("foo3@example.com", "SELECT", "catalog_a", None, None, None, False, False),
                # any file
                ("foo3@example.com", "SELECT", None, None, None, None, True, False),
                # function
                ("foo3@example.com", "SELECT", None, None, None, None, False, True),
            ]
        }
    )
    tables_crawler = TablesCrawler(sql_backend, "test")
    grants_crawler = GrantsCrawler(tables_crawler)
    table_acl_support = TableAclSupport(grants_crawler, sql_backend)

    crawler_tasks = table_acl_support.get_crawler_tasks()

    permissions = next(crawler_tasks)()

    assert "TABLE" == permissions.object_type
    assert "catalog_a.database_b.table_c" == permissions.object_id
    assert Grant(
        principal="foo@example.com",
        action_type="MODIFY, OWN, SELECT",
        catalog="catalog_a",
        database="database_b",
        table="table_c",
        view=None,
        any_file=False,
        anonymous_function=False,
    ) == Grant(**json.loads(permissions.raw))

    permissions = next(crawler_tasks)()

    assert "TABLE" == permissions.object_type
    assert "catalog_a.database_b.table_d" == permissions.object_id
    assert Grant(
        principal="foo@example.com",
        action_type="SELECT",
        catalog="catalog_a",
        database="database_b",
        table="table_d",
        view=None,
        any_file=False,
        anonymous_function=False,
    ) == Grant(**json.loads(permissions.raw))

    permissions = next(crawler_tasks)()

    assert "TABLE" == permissions.object_type
    assert "catalog_a.database_b.table_c" == permissions.object_id
    assert Grant(
        principal="foo2@example.com",
        action_type="SELECT",
        catalog="catalog_a",
        database="database_b",
        table="table_c",
        view=None,
        any_file=False,
        anonymous_function=False,
    ) == Grant(**json.loads(permissions.raw))

    permissions = next(crawler_tasks)()

    assert "VIEW" == permissions.object_type
    assert "catalog_a.database_b.view_c" == permissions.object_id
    assert Grant(
        principal="foo3@example.com",
        action_type="SELECT",
        catalog="catalog_a",
        database="database_b",
        table=None,
        view="view_c",
        any_file=False,
        anonymous_function=False,
    ) == Grant(**json.loads(permissions.raw))

    permissions = next(crawler_tasks)()

    assert "DATABASE" == permissions.object_type
    assert "catalog_a.database_b" == permissions.object_id
    assert Grant(
        principal="foo3@example.com",
        action_type="SELECT",
        catalog="catalog_a",
        database="database_b",
        table=None,
        view=None,
        any_file=False,
        anonymous_function=False,
    ) == Grant(**json.loads(permissions.raw))

    permissions = next(crawler_tasks)()

    assert "CATALOG" == permissions.object_type
    assert "catalog_a" == permissions.object_id
    assert Grant(
        principal="foo3@example.com",
        action_type="SELECT",
        catalog="catalog_a",
        database=None,
        table=None,
        view=None,
        any_file=False,
        anonymous_function=False,
    ) == Grant(**json.loads(permissions.raw))

    permissions = next(crawler_tasks)()

    assert "ANY FILE" == permissions.object_type
    assert permissions.object_id == ""
    assert Grant(
        principal="foo3@example.com",
        action_type="SELECT",
        catalog="",
        database=None,
        table=None,
        view=None,
        any_file=True,
        anonymous_function=False,
    ) == Grant(**json.loads(permissions.raw))

    permissions = next(crawler_tasks)()

    assert "ANONYMOUS FUNCTION" == permissions.object_type
    assert permissions.object_id == ""
    assert Grant(
        principal="foo3@example.com",
        action_type="SELECT",
        catalog="",
        database=None,
        table=None,
        view=None,
        any_file=False,
        anonymous_function=True,
    ) == Grant(**json.loads(permissions.raw))


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
    grp = [
        MigratedGroup(
            id_in_workspace=None,
            name_in_workspace="abc",
            name_in_account="account-abc",
            temporary_name="tmp-backup-abc",
            members=None,
            entitlements=None,
            external_id=None,
            roles=None,
        )
    ]
    migration_state = MigrationState(grp)
    task = table_acl_support.get_apply_task(permissions, migration_state)
    task()

    assert ["GRANT SELECT ON TABLE catalog_a.database_b.table_c TO `account-abc`"] == sql_backend.queries


def test_tacl_applier_multiple_actions(mocker):
    sql_backend = MockBackend()
    table_acl_support = TableAclSupport(mocker.Mock(), sql_backend)

    permissions = Permissions(
        object_type="TABLE",
        object_id="catalog_a.database_b.table_c",
        raw=json.dumps(
            {
                "principal": "abc",
                "action_type": "SELECT, MODIFY",
                "catalog": "catalog_a",
                "database": "database_b",
                "table": "table_c",
            }
        ),
    )
    grp = [
        MigratedGroup(
            id_in_workspace=None,
            name_in_workspace="abc",
            name_in_account="account-abc",
            temporary_name="tmp-backup-abc",
            members=None,
            entitlements=None,
            external_id=None,
            roles=None,
        )
    ]
    migration_state = MigrationState(grp)
    task = table_acl_support.get_apply_task(permissions, migration_state)
    task()

    assert ["GRANT SELECT, MODIFY ON TABLE catalog_a.database_b.table_c TO `account-abc`"] == sql_backend.queries


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
    grp = [
        MigratedGroup(
            id_in_workspace=None,
            name_in_workspace="abc",
            name_in_account="account-abc",
            temporary_name="tmp-backup-abc",
            members=None,
            entitlements=None,
            external_id=None,
            roles=None,
        )
    ]
    migration_state = MigrationState(grp)
    task = table_acl_support.get_apply_task(permissions, migration_state)
    assert task is None

    assert [] == sql_backend.queries

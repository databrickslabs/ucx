import json

import pytest
from databricks.labs.lsql.backends import MockBackend
from databricks.sdk.errors import NotFound

from databricks.labs.ucx.hive_metastore import TablesCrawler
from databricks.labs.ucx.hive_metastore.grants import Grant, GrantsCrawler
from databricks.labs.ucx.hive_metastore.udfs import UdfsCrawler
from databricks.labs.ucx.workspace_access.base import Permissions
from databricks.labs.ucx.workspace_access.groups import MigratedGroup, MigrationState
from databricks.labs.ucx.workspace_access.tacl import TableAclSupport

UCX_TABLES = MockBackend.rows("catalog", "database", "table", "object_type", "table_format", "location", "view_text")
UCX_GRANTS = MockBackend.rows(
    "principal", "action_type", "catalog", "database", "table", "view", "udf", "any_file", "anonymous_function"
)
DESCRIBE_TABLE = MockBackend.rows("key", "value", "ignored")
SHOW_DATABASES = MockBackend.rows("databaseName")
SHOW_FUNCTIONS = MockBackend.rows("function")
SHOW_GRANTS = MockBackend.rows("principal", "action_type", "object_type", "ignored")
SHOW_TABLES = MockBackend.rows("databaseName", "tableName", "isTmp")


def test_tacl_crawler():
    sql_backend = MockBackend(
        rows={
            "SELECT \\* FROM hive_metastore.test.grants": UCX_GRANTS[
                ("foo@example.com", "SELECT", "catalog_a", "database_b", "table_c", None, None, False, False)
            ]
        }
    )
    tables_crawler = TablesCrawler(sql_backend, "test")
    udf_crawler = UdfsCrawler(sql_backend, "test")
    grants_crawler = GrantsCrawler(tables_crawler, udf_crawler)
    table_acl_support = TableAclSupport(grants_crawler, sql_backend)

    crawler_tasks = table_acl_support.get_crawler_tasks()
    first_task = next(crawler_tasks)
    obj = first_task()

    assert obj.object_type == "TABLE"
    assert obj.object_id == "catalog_a.database_b.table_c"


def test_tacl_udf_crawler():
    sql_backend = MockBackend(
        rows={
            "SELECT \\* FROM hive_metastore.test.grants": UCX_GRANTS[
                ("foo@example.com", "READ_METADATA", "catalog_a", "database_b", None, None, "function_c", False, False)
            ]
        }
    )
    tables_crawler = TablesCrawler(sql_backend, "test")
    udf_crawler = UdfsCrawler(sql_backend, "test")
    grants_crawler = GrantsCrawler(tables_crawler, udf_crawler)
    table_acl_support = TableAclSupport(grants_crawler, sql_backend)

    crawler_tasks = table_acl_support.get_crawler_tasks()
    first_task = next(crawler_tasks)
    obj = first_task()

    assert obj.object_type == "FUNCTION"
    assert obj.object_id == "catalog_a.database_b.function_c"


def test_tacl_crawler_multiple_permissions():
    sql_backend = MockBackend(
        rows={
            "SELECT \\* FROM hive_metastore.test.grants": UCX_GRANTS[
                ("foo@example.com", "SELECT", "catalog_a", "database_b", "table_c", None, None, False, False),
                ("foo@example.com", "MODIFY", "catalog_a", "database_b", "table_c", None, None, False, False),
                ("foo@example.com", "OWN", "catalog_a", "database_b", "table_c", None, None, False, False),
                # different table name (object_id)
                ("foo@example.com", "SELECT", "catalog_a", "database_b", "table_d", None, None, False, False),
                # different principal
                ("foo2@example.com", "SELECT", "catalog_a", "database_b", "table_c", None, None, False, False),
                # duplicate
                ("foo2@example.com", "SELECT", "catalog_a", "database_b", "table_c", None, None, False, False),
                # view
                ("foo3@example.com", "SELECT", "catalog_a", "database_b", None, "view_c", None, False, False),
                # database
                ("foo3@example.com", "SELECT", "catalog_a", "database_b", None, None, None, False, False),
                # catalog
                ("foo3@example.com", "SELECT", "catalog_a", None, None, None, None, False, False),
                # any file
                ("foo3@example.com", "SELECT", None, None, None, None, None, True, False),
                # anonymous function
                ("foo3@example.com", "SELECT", None, None, None, None, None, False, True),
                # udf (user defined function)
                ("foo3@example.com", "SELECT", "catalog_a", "database_b", None, None, "function_c", False, False),
            ]
        }
    )
    tables_crawler = TablesCrawler(sql_backend, "test")
    udf_crawler = UdfsCrawler(sql_backend, "test")
    grants_crawler = GrantsCrawler(tables_crawler, udf_crawler)
    table_acl_support = TableAclSupport(grants_crawler, sql_backend)

    crawler_tasks = table_acl_support.get_crawler_tasks()

    permissions = next(crawler_tasks)()

    assert permissions.object_type == "TABLE"
    assert permissions.object_id == "catalog_a.database_b.table_c"
    assert Grant(
        principal="foo@example.com",
        action_type="MODIFY, OWN, SELECT",
        catalog="catalog_a",
        database="database_b",
        table="table_c",
        view=None,
        udf=None,
        any_file=False,
        anonymous_function=False,
    ) == Grant(**json.loads(permissions.raw))

    permissions = next(crawler_tasks)()

    assert permissions.object_type == "TABLE"
    assert permissions.object_id == "catalog_a.database_b.table_d"
    assert Grant(
        principal="foo@example.com",
        action_type="SELECT",
        catalog="catalog_a",
        database="database_b",
        table="table_d",
        view=None,
        udf=None,
        any_file=False,
        anonymous_function=False,
    ) == Grant(**json.loads(permissions.raw))

    permissions = next(crawler_tasks)()

    assert permissions.object_type == "TABLE"
    assert permissions.object_id == "catalog_a.database_b.table_c"
    assert Grant(
        principal="foo2@example.com",
        action_type="SELECT",
        catalog="catalog_a",
        database="database_b",
        table="table_c",
        view=None,
        udf=None,
        any_file=False,
        anonymous_function=False,
    ) == Grant(**json.loads(permissions.raw))

    permissions = next(crawler_tasks)()

    assert permissions.object_type == "VIEW"
    assert permissions.object_id == "catalog_a.database_b.view_c"
    assert Grant(
        principal="foo3@example.com",
        action_type="SELECT",
        catalog="catalog_a",
        database="database_b",
        table=None,
        view="view_c",
        udf=None,
        any_file=False,
        anonymous_function=False,
    ) == Grant(**json.loads(permissions.raw))

    permissions = next(crawler_tasks)()

    assert permissions.object_type == "DATABASE"
    assert permissions.object_id == "catalog_a.database_b"
    assert Grant(
        principal="foo3@example.com",
        action_type="SELECT",
        catalog="catalog_a",
        database="database_b",
        table=None,
        view=None,
        udf=None,
        any_file=False,
        anonymous_function=False,
    ) == Grant(**json.loads(permissions.raw))

    permissions = next(crawler_tasks)()

    assert permissions.object_type == "CATALOG"
    assert permissions.object_id == "catalog_a"
    assert Grant(
        principal="foo3@example.com",
        action_type="SELECT",
        catalog="catalog_a",
        database=None,
        table=None,
        view=None,
        udf=None,
        any_file=False,
        anonymous_function=False,
    ) == Grant(**json.loads(permissions.raw))

    permissions = next(crawler_tasks)()

    assert permissions.object_type == "ANY FILE"
    assert permissions.object_id == ""
    assert Grant(
        principal="foo3@example.com",
        action_type="SELECT",
        catalog="",
        database=None,
        table=None,
        view=None,
        udf=None,
        any_file=True,
        anonymous_function=False,
    ) == Grant(**json.loads(permissions.raw))

    permissions = next(crawler_tasks)()

    assert permissions.object_type == "ANONYMOUS FUNCTION"
    assert permissions.object_id == ""
    assert Grant(
        principal="foo3@example.com",
        action_type="SELECT",
        catalog="",
        database=None,
        table=None,
        view=None,
        udf=None,
        any_file=False,
        anonymous_function=True,
    ) == Grant(**json.loads(permissions.raw))

    permissions = next(crawler_tasks)()

    assert permissions.object_type == "FUNCTION"
    assert permissions.object_id == "catalog_a.database_b.function_c"
    assert Grant(
        principal="foo3@example.com",
        action_type="SELECT",
        catalog="catalog_a",
        database="database_b",
        table=None,
        view=None,
        udf="function_c",
        any_file=False,
        anonymous_function=False,
    ) == Grant(**json.loads(permissions.raw))


def test_tacl_applier():
    sql_backend = MockBackend(
        rows={
            "SELECT \\* FROM hive_metastore.test.grants": UCX_GRANTS[
                ("abc", "SELECT", "catalog_a", "database_b", "table_c", None, None, False, False)
            ],
            "SHOW GRANTS ON TABLE catalog_a.database_b.table_c": SHOW_GRANTS[
                ("account-abc", "SELECT", "TABLE", "table_c"),
            ],
        }
    )
    tables_crawler = TablesCrawler(sql_backend, "test")
    udf_crawler = UdfsCrawler(sql_backend, "test")
    grants_crawler = GrantsCrawler(tables_crawler, udf_crawler)
    table_acl_support = TableAclSupport(grants_crawler, sql_backend)

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
    validation_res = task()

    assert [
        "GRANT SELECT ON TABLE catalog_a.database_b.table_c TO `account-abc`",
        'SHOW GRANTS ON TABLE catalog_a.database_b.table_c',
    ] == sql_backend.queries
    assert validation_res


def test_tacl_applier_not_applied():
    sql_backend = MockBackend(rows={"SELECT \\* FROM hive_metastore.test.grants": []})
    tables_crawler = TablesCrawler(sql_backend, "test")
    udf_crawler = UdfsCrawler(sql_backend, "test")
    grants_crawler = GrantsCrawler(tables_crawler, udf_crawler)
    table_acl_support = TableAclSupport(grants_crawler, sql_backend)

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
    validation_res = task()

    assert [
        "GRANT SELECT ON TABLE catalog_a.database_b.table_c TO `account-abc`",
        'SHOW GRANTS ON TABLE catalog_a.database_b.table_c',
    ] == sql_backend.queries
    assert not validation_res


def test_tacl_udf_applier(mocker):
    sql_backend = MockBackend(
        rows={
            "SELECT \\* FROM hive_metastore.test.grants": UCX_GRANTS[
                ("abc", "SELECT", "catalog_a", "database_b", "table_c", None, None, False, False)
            ],
            "SHOW GRANTS ON FUNCTION catalog_a.database_b.function_c": SHOW_GRANTS[
                ("account-abc", "SELECT", "FUNCTION", "function_c"),
            ],
        }
    )
    tables_crawler = TablesCrawler(sql_backend, "test")
    udf_crawler = UdfsCrawler(sql_backend, "test")
    grants_crawler = GrantsCrawler(tables_crawler, udf_crawler)
    table_acl_support = TableAclSupport(grants_crawler, sql_backend)

    permissions = Permissions(
        object_type="FUNCTION",
        object_id="catalog_a.database_b.function_c",
        raw=json.dumps(
            {
                "principal": "abc",
                "action_type": "SELECT",
                "catalog": "catalog_a",
                "database": "database_b",
                "udf": "function_c",
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
    validation_res = task()

    assert [
        "GRANT SELECT ON FUNCTION catalog_a.database_b.function_c TO `account-abc`",
        "SHOW GRANTS ON FUNCTION catalog_a.database_b.function_c",
    ] == sql_backend.queries
    assert validation_res


def test_tacl_applier_multiple_actions(mocker):
    sql_backend = MockBackend(
        rows={
            "SELECT \\* FROM hive_metastore.test.grants": UCX_GRANTS[
                ("abc", "SELECT", "catalog_a", "database_b", "table_c", None, None, False, False)
            ],
            "SHOW GRANTS ON TABLE catalog_a.database_b.table_c": SHOW_GRANTS[
                ("account-abc", "SELECT", "TABLE", "table_c"),
                ("account-abc", "MODIFY", "TABLE", "table_c"),
            ],
        }
    )
    tables_crawler = TablesCrawler(sql_backend, "test")
    udf_crawler = UdfsCrawler(sql_backend, "test")
    grants_crawler = GrantsCrawler(tables_crawler, udf_crawler)
    table_acl_support = TableAclSupport(grants_crawler, sql_backend)

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
    validation_res = task()

    assert [
        "GRANT SELECT, MODIFY ON TABLE catalog_a.database_b.table_c TO `account-abc`",
        "SHOW GRANTS ON TABLE catalog_a.database_b.table_c",
    ] == sql_backend.queries
    assert validation_res


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

    assert not sql_backend.queries


def test_verify_task_should_return_true_if_permissions_applied():
    sql_backend = MockBackend(
        rows={
            "SHOW GRANTS ON TABLE catalog_a.database_b.table_c": SHOW_GRANTS[("abc", "SELECT", "TABLE", "table_c"),],
        }
    )
    tables_crawler = TablesCrawler(sql_backend, "test")
    udf_crawler = UdfsCrawler(sql_backend, "test")
    grants_crawler = GrantsCrawler(tables_crawler, udf_crawler)
    table_acl_support = TableAclSupport(grants_crawler, sql_backend)

    item = Permissions(
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

    task = table_acl_support.get_verify_task(item)
    result = task()
    assert result


def test_verify_task_should_fail_if_permissions_not_applied():
    sql_backend = MockBackend(
        rows={
            "SHOW GRANTS ON TABLE catalog_a.database_b.table_c": SHOW_GRANTS[("abc", "MODIFY", "TABLE", "table_c"),],
        }
    )
    tables_crawler = TablesCrawler(sql_backend, "test")
    udf_crawler = UdfsCrawler(sql_backend, "test")
    grants_crawler = GrantsCrawler(tables_crawler, udf_crawler)
    table_acl_support = TableAclSupport(grants_crawler, sql_backend)

    item = Permissions(
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

    task = table_acl_support.get_verify_task(item)
    with pytest.raises(NotFound):
        task()


def test_verify_task_should_return_false_if_not_grants_present():
    sql_backend = MockBackend()
    tables_crawler = TablesCrawler(sql_backend, "test")
    udf_crawler = UdfsCrawler(sql_backend, "test")
    grants_crawler = GrantsCrawler(tables_crawler, udf_crawler)
    table_acl_support = TableAclSupport(grants_crawler, sql_backend)

    item = Permissions(
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

    task = table_acl_support.get_verify_task(item)
    result = task()
    assert not result

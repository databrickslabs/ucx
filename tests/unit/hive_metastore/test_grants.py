import logging
from unittest.mock import create_autospec

import pytest
from databricks.labs.lsql.backends import MockBackend

from databricks.labs.ucx.framework.owners import AdministratorLocator
from databricks.labs.ucx.hive_metastore.grants import Grant, GrantsCrawler, MigrateGrants, GrantOwnership
from databricks.labs.ucx.hive_metastore.tables import Table, TablesCrawler
from databricks.labs.ucx.hive_metastore.udfs import UdfsCrawler
from databricks.labs.ucx.workspace_access.groups import GroupManager


def test_type_and_key_table() -> None:
    grant = Grant.type_and_key(catalog="hive_metastore", database="mydb", table="mytable")
    assert grant == ("TABLE", "hive_metastore.mydb.mytable")

    grant = Grant(principal="user", action_type="SELECT", catalog="hive_metastore", database="mydb", table="mytable")
    assert grant.this_type_and_key()[0] == "TABLE"
    assert grant.object_key == "hive_metastore.mydb.mytable"


def test_type_and_key_view() -> None:
    grant = Grant.type_and_key(catalog="hive_metastore", database="mydb", view="myview")
    assert grant == ("VIEW", "hive_metastore.mydb.myview")

    grant = Grant(principal="user", action_type="SELECT", catalog="hive_metastore", database="mydb", view="myview")
    assert grant.this_type_and_key()[0] == "VIEW"
    assert grant.object_key == "hive_metastore.mydb.myview"


def test_type_and_key_database() -> None:
    grant = Grant.type_and_key(catalog="hive_metastore", database="mydb")
    assert grant == ("DATABASE", "hive_metastore.mydb")

    grant = Grant(principal="user", action_type="SELECT", catalog="hive_metastore", database="mydb")
    assert grant.this_type_and_key()[0] == "DATABASE"
    assert grant.object_key == "hive_metastore.mydb"


def test_type_and_key_catalog() -> None:
    grant = Grant.type_and_key(catalog="mycatalog")
    assert grant == ("CATALOG", "mycatalog")

    grant = Grant(principal="user", action_type="SELECT", catalog="mycatalog")
    assert grant.this_type_and_key()[0] == "CATALOG"
    assert grant.object_key == "mycatalog"


def test_type_and_key_any_file() -> None:
    grant = Grant.type_and_key(any_file=True)
    assert grant == ("ANY FILE", "")

    grant = Grant(principal="user", action_type="SELECT", catalog="hive_metastore", any_file=True)
    assert grant.this_type_and_key()[0] == "ANY FILE"
    assert grant.object_key == ""


def test_type_and_key_anonymous_function() -> None:
    grant = Grant.type_and_key(anonymous_function=True)
    assert grant == ("ANONYMOUS FUNCTION", "")

    grant = Grant(principal="user", action_type="SELECT", catalog="hive_metastore", anonymous_function=True)
    assert grant.this_type_and_key()[0] == "ANONYMOUS FUNCTION"
    assert grant.object_key == ""


def test_type_and_key_udf() -> None:
    grant = Grant.type_and_key(catalog="hive_metastore", database="mydb", udf="myfunction")
    assert grant == ("FUNCTION", "hive_metastore.mydb.myfunction")

    grant = Grant(principal="user", action_type="SELECT", catalog="hive_metastore", database="mydb", udf="myfunction")
    assert grant.this_type_and_key()[0] == "FUNCTION"
    assert grant.object_key == "hive_metastore.mydb.myfunction"


def test_type_and_key_invalid() -> None:
    with pytest.raises(ValueError):
        Grant.type_and_key()


def test_object_key() -> None:
    grant = Grant(principal="user", action_type="SELECT", catalog="hive_metastore", database="mydb", table="mytable")
    assert grant.object_key == "hive_metastore.mydb.mytable"


def test_hive_sql() -> None:
    grant = Grant(principal="user", action_type="SELECT", catalog="hive_metastore", database="mydb", table="mytable")
    assert grant.hive_grant_sql() == ["GRANT SELECT ON TABLE `hive_metastore`.`mydb`.`mytable` TO `user`"]
    assert grant.hive_revoke_sql() == "REVOKE SELECT ON TABLE `hive_metastore`.`mydb`.`mytable` FROM `user`"


def test_hive_table_own_sql() -> None:
    grant = Grant(principal="user", action_type="OWN", catalog="hive_metastore", database="mydb", table="mytable")
    assert grant.hive_grant_sql() == ["ALTER TABLE `hive_metastore`.`mydb`.`mytable` OWNER TO `user`"]


def test_hive_database_own_sql() -> None:
    grant = Grant(principal="user", action_type="OWN", catalog="hive_metastore", database="mydb")
    assert grant.hive_grant_sql() == ["ALTER DATABASE `hive_metastore`.`mydb` OWNER TO `user`"]


def test_hive_udf_own_sql() -> None:
    grant = Grant(principal="user", action_type="OWN", catalog="hive_metastore", database="mydb", udf="myfunction")
    assert grant.hive_grant_sql() == ["ALTER FUNCTION `hive_metastore`.`mydb`.`myfunction` OWNER TO `user`"]


def test_hive_revoke_sql() -> None:
    grant = Grant(principal="user", action_type="SELECT", catalog="hive_metastore", database="mydb", table="mytable")
    assert grant.hive_revoke_sql() == "REVOKE SELECT ON TABLE `hive_metastore`.`mydb`.`mytable` FROM `user`"


def test_hive_deny_sql() -> None:
    grant = Grant(
        principal="user", action_type="DENIED_SELECT", catalog="hive_metastore", database="mydb", table="mytable"
    )
    assert grant.hive_grant_sql() == ["DENY `SELECT` ON TABLE `hive_metastore`.`mydb`.`mytable` TO `user`"]


@pytest.mark.parametrize(
    "grant,query",
    [
        (
            Grant("user", "READ_METADATA", catalog="hive_metastore", database="mydb", table="mytable"),
            None,
        ),
        (
            Grant("me", "OWN", catalog="hive_metastore", database="mydb", table="mytable"),
            "ALTER TABLE `hive_metastore`.`mydb`.`mytable` OWNER TO `me`",
        ),
        (
            Grant("me", "USAGE", catalog="hive_metastore", database="mydb"),
            "GRANT USE SCHEMA ON DATABASE `hive_metastore`.`mydb` TO `me`",
        ),
        (
            Grant("me", "INVALID", catalog="hive_metastore", database="mydb"),
            None,
        ),
        (
            Grant("me", "SELECT", catalog="hive_metastore", database="mydb", udf="myfunction"),
            "GRANT EXECUTE ON FUNCTION `hive_metastore`.`mydb`.`myfunction` TO `me`",
        ),
    ],
)
def test_uc_sql(grant, query) -> None:
    assert grant.uc_grant_sql() == query


UCX_TABLES = MockBackend.rows("catalog", "database", "table", "object_type", "table_format", "location", "view_text")
DESCRIBE_TABLE = MockBackend.rows("key", "value", "ignored")
DESCRIBE_FUNCTION = MockBackend.rows("function_desc")
SHOW_DATABASES = MockBackend.rows("databaseName")
SHOW_FUNCTIONS = MockBackend.rows("function")
SHOW_GRANTS = MockBackend.rows("principal", "action_type", "object_type", "ignored")
SHOW_TABLES = MockBackend.rows("databaseName", "tableName", "isTmp")

ROWS = {
    "SELECT.*": UCX_TABLES[
        ("foo", "bar", "test_table", "type", "DELTA", "/foo/bar/test", None),
        ("foo", "bar", "test_view", "type", "VIEW", None, "SELECT * FROM table"),
        ("foo", None, None, "type", "CATALOG", None, None),
    ],
    "SHOW.*": SHOW_GRANTS[
        ("princ1", "SELECT", "TABLE", "ignored"),
        ("princ1", "SELECT", "VIEW", "ignored"),
        ("princ1", "USE", "CATALOG$", "ignored"),
    ],
    "DESCRIBE.*": DESCRIBE_TABLE[
        ("Catalog", "foo", "ignored"),
        ("Type", "TABLE", "ignored"),
        ("Provider", "", "ignored"),
        ("Location", "/foo/bar/test", "ignored"),
        ("View Text", "SELECT * FROM table", "ignored"),
    ],
}


def test_crawler_no_data() -> None:
    sql_backend = MockBackend()
    table = TablesCrawler(sql_backend, "schema")
    udf = UdfsCrawler(sql_backend, "schema")
    crawler = GrantsCrawler(table, udf)
    grants = list(crawler.snapshot())
    assert len(grants) == 0


def test_crawler_crawl() -> None:
    sql_backend = MockBackend(
        rows={
            "SHOW DATABASES": SHOW_DATABASES[
                ("database_one",),
                ("database_two",),
            ],
            "SHOW TABLES FROM `hive_metastore`\\.`database_one`": SHOW_TABLES[
                ("database_one", "table_one", "true"),
                ("database_one", "table_two", "true"),
            ],
            "DESCRIBE TABLE EXTENDED `hive_metastore`\\.`database_one`\\.`table_one`": DESCRIBE_TABLE[
                ("Catalog", "foo", "ignored"),
                ("Type", "TABLE", "ignored"),
                ("Provider", "", "ignored"),
                ("View Text", "SELECT * FROM table", "ignored"),
            ],
            "DESCRIBE TABLE EXTENDED `hive_metastore`\\.`database_one`\\.`table_two`": DESCRIBE_TABLE[
                ("Catalog", "foo", "ignored"),
                ("Type", "TABLE", "ignored"),
                ("Provider", "", "ignored"),
                ("Location", "/foo/bar/test", "ignored"),
            ],
            "SHOW GRANTS ON CATALOG `hive_metastore`": SHOW_GRANTS[("princ1", "USE", "CATALOG$", "hive_metastore"),],
            "SHOW GRANTS ON DATABASE `hive_metastore`\\.`database_one`": SHOW_GRANTS[
                ("princ2", "OWN", "DATABASE", "database_one"),
                # Enumerating database grants can include some grants for the catalog.
                ("princ1", "SELECT", "CATALOG$", None),
            ],
            "SHOW GRANTS ON VIEW `hive_metastore`\\.`database_one`\\.`table_one`": SHOW_GRANTS[
                ("princ3", "SELECT", "TABLE", "table_one"),
            ],
            "SHOW GRANTS ON TABLE `hive_metastore`\\.`database_one`\\.`table_two`": SHOW_GRANTS[
                ("princ4", "SELECT", "TABLE", "table_two"),
            ],
        }
    )
    expected_grants = {
        Grant(principal="princ1", catalog="hive_metastore", action_type="USE"),
        Grant(principal="princ2", catalog="hive_metastore", database="database_one", action_type="OWN"),
        Grant(
            principal="princ3",
            catalog="hive_metastore",
            database="database_one",
            view="table_one",
            action_type="SELECT",
        ),
        Grant(
            principal="princ4",
            catalog="hive_metastore",
            database="database_one",
            table="table_two",
            action_type="SELECT",
        ),
    }
    table = TablesCrawler(sql_backend, "schema")
    udf = UdfsCrawler(sql_backend, "schema")
    crawler = GrantsCrawler(table, udf)
    grants = list(crawler.snapshot())
    assert len(grants) == len(expected_grants) and set(grants) == expected_grants


def test_crawler_udf_crawl() -> None:
    sql_backend = MockBackend(
        rows={
            "SHOW DATABASES": SHOW_DATABASES[("database_one",),],
            "SHOW USER FUNCTIONS FROM `hive_metastore`\\.`database_one`": SHOW_FUNCTIONS[
                ("hive_metastore.database_one.function_one",),
                ("hive_metastore.database_one.function_two",),
            ],
            "DESCRIBE FUNCTION EXTENDED `hive_metastore`\\.`database_one`.*": DESCRIBE_FUNCTION[
                ("Type: SCALAR",),
                ("Input: p INT",),
                ("Returns: FLOAT",),
                ("Deterministic: true",),
                ("Data Access: CONTAINS SQL",),
                ("Body: 1",),
                ("ignore",),
            ],
            "SHOW GRANTS ON FUNCTION `hive_metastore`\\.`database_one`.`function_one`": SHOW_GRANTS[
                ("princ1", "SELECT", "FUNCTION", "function_one"),
            ],
            "SHOW GRANTS ON FUNCTION `hive_metastore`\\.`database_one`.`function_two`": SHOW_GRANTS[
                ("princ2", "SELECT", "FUNCTION", "function_two"),
            ],
        }
    )
    expected_grants = {
        Grant(
            principal="princ1",
            catalog="hive_metastore",
            database="database_one",
            udf="function_one",
            action_type="SELECT",
        ),
        Grant(
            principal="princ2",
            catalog="hive_metastore",
            database="database_one",
            udf="function_two",
            action_type="SELECT",
        ),
    }

    table = TablesCrawler(sql_backend, "schema")
    udf = UdfsCrawler(sql_backend, "schema")
    crawler = GrantsCrawler(table, udf)
    grants = list(crawler.snapshot())

    assert len(grants) == len(expected_grants) and set(grants) == expected_grants


def test_crawler_snapshot_when_no_data() -> None:
    sql_backend = MockBackend()
    table = TablesCrawler(sql_backend, "schema")
    udf = UdfsCrawler(sql_backend, "schema")
    crawler = GrantsCrawler(table, udf)
    snapshot = list(crawler.snapshot())
    assert len(snapshot) == 0


def test_crawler_snapshot_with_data() -> None:
    sql_backend = MockBackend(rows=ROWS)
    table = TablesCrawler(sql_backend, "schema")
    udf = UdfsCrawler(sql_backend, "schema")
    crawler = GrantsCrawler(table, udf)
    snapshot = list(crawler.snapshot())
    assert len(snapshot) == 3


def test_grants_returning_error_when_showing_grants() -> None:
    errors = {"SHOW GRANTS ON TABLE `hive_metastore`.`test_database`.`table1`": "error"}
    rows = {
        "SHOW DATABASES": SHOW_DATABASES[
            ("test_database",),
            ("other_database",),
        ],
        "SHOW TABLES FROM `hive_metastore`.`test_database`": SHOW_TABLES[
            ("test_database", "table1", False),
            ("test_database", "table2", False),
        ],
        "SHOW GRANTS ON TABLE `hive_metastore`.`test_database`.`table2`": SHOW_GRANTS[
            ("principal1", "OWNER", "TABLE", ""),
        ],
        "DESCRIBE *": DESCRIBE_TABLE[
            ("Catalog", "catalog", ""),
            ("Type", "delta", ""),
        ],
    }

    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = TablesCrawler(backend, "default")
    udf = UdfsCrawler(backend, "default")
    crawler = GrantsCrawler(table_crawler, udf)

    results = list(crawler.snapshot())
    assert results == [
        Grant(
            principal="principal1",
            action_type="OWNER",
            catalog="hive_metastore",
            database="test_database",
            table="table2",
            any_file=False,
            anonymous_function=False,
        )
    ]


def test_grants_returning_error_when_describing() -> None:
    errors = {"DESCRIBE TABLE EXTENDED `hive_metastore`.`test_database`.`table1`": "error"}
    rows = {
        "SHOW DATABASES": SHOW_DATABASES[("test_database",),],
        "SHOW TABLES FROM `hive_metastore`.`test_database`": SHOW_TABLES[
            ("test_database", "table1", False),
            ("test_database", "table2", False),
        ],
        "SHOW GRANTS ON TABLE `hive_metastore`.`test_database`.`table2`": SHOW_GRANTS[
            ("principal1", "OWNER", "TABLE", ""),
        ],
        "DESCRIBE *": DESCRIBE_TABLE[
            ("Catalog", "catalog", ""),
            ("Type", "delta", ""),
        ],
    }

    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = TablesCrawler(backend, "default")
    udf = UdfsCrawler(backend, "default")
    crawler = GrantsCrawler(table_crawler, udf)

    results = list(crawler.snapshot())
    assert results == [
        Grant(
            principal="principal1",
            action_type="OWNER",
            catalog="hive_metastore",
            database="test_database",
            table="table2",
            any_file=False,
            anonymous_function=False,
        )
    ]


def test_udf_grants_returning_error_when_showing_grants() -> None:
    errors = {"SHOW GRANTS ON FUNCTION `hive_metastore`.`test_database`.`function_bad`": "error"}
    rows = {
        "SHOW DATABASES": SHOW_DATABASES[
            ("test_database",),
            ("other_database",),
        ],
        "SHOW USER FUNCTIONS FROM `hive_metastore`.`test_database`": SHOW_FUNCTIONS[
            ("hive_metastore.test_database.function_bad",),
            ("hive_metastore.test_database.function_good",),
        ],
        "SHOW GRANTS ON FUNCTION `hive_metastore`.`test_database`.`function_good`": SHOW_GRANTS[
            ("principal1", "OWN", "FUNCTION", "")
        ],
        "DESCRIBE *": DESCRIBE_FUNCTION[
            ("Type: SCALAR",),
            ("Body: 1",),
        ],
    }

    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = TablesCrawler(backend, "default")
    udf = UdfsCrawler(backend, "default")
    crawler = GrantsCrawler(table_crawler, udf)

    results = list(crawler.snapshot())
    assert results == [
        Grant(
            principal="principal1",
            action_type="OWN",
            catalog="hive_metastore",
            database="test_database",
            udf="function_good",
            any_file=False,
            anonymous_function=False,
        )
    ]


def test_udf_grants_returning_error_when_describing() -> None:
    errors = {"DESCRIBE FUNCTION EXTENDED `hive_metastore`.`test_database`.`function_bad`": "error"}
    rows = {
        "SHOW DATABASES": SHOW_DATABASES[("test_database",),],
        "SHOW USER FUNCTIONS FROM `hive_metastore`.`test_database`": SHOW_FUNCTIONS[
            ("hive_metastore.test_database.function_bad",),
            ("hive_metastore.test_database.function_good",),
        ],
        "SHOW GRANTS ON FUNCTION `hive_metastore`.`test_database`.`function_good`": SHOW_GRANTS[
            ("principal1", "OWN", "FUNCTION", ""),
        ],
        "DESCRIBE *": DESCRIBE_FUNCTION[
            ("Type: SCALAR",),
            ("Body: 1",),
        ],
    }

    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = TablesCrawler(backend, "default")
    udf = UdfsCrawler(backend, "default")
    crawler = GrantsCrawler(table_crawler, udf)

    results = list(crawler.snapshot())
    assert results == [
        Grant(
            principal="principal1",
            action_type="OWN",
            catalog="hive_metastore",
            database="test_database",
            udf="function_good",
            any_file=False,
            anonymous_function=False,
        )
    ]


def test_crawler_should_filter_databases() -> None:
    sql_backend = MockBackend(
        rows={
            "SHOW TABLES FROM `hive_metastore`\\.`database_one`": SHOW_TABLES[("database_one", "table_one", "true"),],
            "DESCRIBE TABLE EXTENDED `hive_metastore`\\.`database_one`\\.`table_one`": DESCRIBE_TABLE[
                ("Catalog", "foo", "ignored"),
                ("Type", "TABLE", "ignored"),
                ("Provider", "", "ignored"),
                ("Location", "/foo/bar/test", "ignored"),
            ],
            "SHOW GRANTS ON CATALOG `hive_metastore`": SHOW_GRANTS[("princ1", "USE", "CATALOG$", "hive_metastore"),],
            "SHOW GRANTS ON TABLE `hive_metastore`\\.`database_one`\\.`table_one`": SHOW_GRANTS[
                ("princ2", "SELECT", "TABLE", "table_one"),
            ],
        }
    )
    expected_grants = {
        Grant(principal="princ1", catalog="hive_metastore", action_type="USE"),
        Grant(
            principal="princ2",
            catalog="hive_metastore",
            database="database_one",
            table="table_one",
            action_type="SELECT",
        ),
    }

    table = TablesCrawler(sql_backend, "schema", include_databases=["database_one"])
    udf = UdfsCrawler(sql_backend, "schema", include_databases=["database_one"])
    crawler = GrantsCrawler(table, udf, include_databases=["database_one"])
    grants = list(crawler.snapshot())

    assert "SHOW DATABASES" not in sql_backend.queries
    assert len(grants) == len(expected_grants) and set(grants) == expected_grants


def test_migrate_grants_logs_unmapped_acl(caplog) -> None:
    group_manager = create_autospec(GroupManager)
    table = Table("hive_metastore", "database", "table", "MANAGED", "DELTA")

    def grant_loader() -> list[Grant]:
        return [
            Grant(
                principal="user",
                action_type="READ_METADATA",
                catalog=table.catalog,
                database=table.database,
                table=table.name,
            ),
        ]

    migrate_grants = MigrateGrants(
        MockBackend(),
        group_manager,
        [grant_loader],
    )

    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.hive_metastore.grants"):
        migrate_grants.apply(table, f"uc.{table.database}.{table.name}")
    assert (
        "failed-to-migrate: Hive metastore grant 'READ_METADATA' cannot be mapped to UC grant for TABLE 'uc.database.table'"
        in caplog.text
    )
    group_manager.assert_not_called()


def test_grant_owner() -> None:
    """Verify that the owner of a crawled grant is an administrator."""
    admin_locator = create_autospec(AdministratorLocator)
    admin_locator.get_workspace_administrator.return_value = "an_admin"

    ownership = GrantOwnership(admin_locator)
    owner = ownership.owner_of(Grant(principal="someone", action_type="SELECT"))

    assert owner == "an_admin"
    admin_locator.get_workspace_administrator.assert_called_once()

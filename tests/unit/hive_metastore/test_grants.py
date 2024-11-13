import dataclasses
import io
import logging
import os
from unittest.mock import create_autospec

import pytest
import yaml
from databricks.labs.lsql.backends import MockBackend
from databricks.labs.lsql.core import Row
from databricks.sdk.service.iam import ComplexValue, Group

from databricks.labs.ucx.__about__ import __version__ as ucx_version
from databricks.labs.ucx.contexts.workspace_cli import WorkspaceContext
from databricks.labs.ucx.framework.owners import AdministratorLocator
from databricks.labs.ucx.hive_metastore.catalog_schema import Catalog, Schema
from databricks.labs.ucx.hive_metastore.grants import Grant, GrantsCrawler, MigrateGrants, GrantOwnership
from databricks.labs.ucx.hive_metastore.tables import Table, TablesCrawler
from databricks.labs.ucx.hive_metastore.udfs import UdfsCrawler
from databricks.labs.ucx.progress.history import ProgressEncoder
from databricks.labs.ucx.workspace_access.groups import GroupManager
from tests.integration.hive_metastore.test_ext_hms import sql_backend
from tests.unit import mock_workspace_client


def test_type_and_key_table() -> None:
    type_and_key = Grant.type_and_key(catalog="hive_metastore", database="mydb", table="mytable")
    assert type_and_key == ("TABLE", "hive_metastore.mydb.mytable")

    grant = Grant(principal="user", action_type="SELECT", catalog="hive_metastore", database="mydb", table="mytable")
    assert grant.this_type_and_key()[0] == "TABLE"
    assert grant.object_key == "hive_metastore.mydb.mytable"


def test_type_and_key_view() -> None:
    type_and_key = Grant.type_and_key(catalog="hive_metastore", database="mydb", view="myview")
    assert type_and_key == ("VIEW", "hive_metastore.mydb.myview")

    grant = Grant(principal="user", action_type="SELECT", catalog="hive_metastore", database="mydb", view="myview")
    assert grant.this_type_and_key()[0] == "VIEW"
    assert grant.object_key == "hive_metastore.mydb.myview"


def test_type_and_key_database() -> None:
    type_and_key = Grant.type_and_key(catalog="hive_metastore", database="mydb")
    assert type_and_key == ("DATABASE", "hive_metastore.mydb")

    grant = Grant(principal="user", action_type="SELECT", catalog="hive_metastore", database="mydb")
    assert grant.this_type_and_key()[0] == "DATABASE"
    assert grant.object_key == "hive_metastore.mydb"


def test_type_and_key_catalog() -> None:
    type_and_key = Grant.type_and_key(catalog="mycatalog")
    assert type_and_key == ("CATALOG", "mycatalog")

    grant = Grant(principal="user", action_type="SELECT", catalog="mycatalog")
    assert grant.this_type_and_key()[0] == "CATALOG"
    assert grant.object_key == "mycatalog"


def test_type_and_key_any_file() -> None:
    type_and_key = Grant.type_and_key(any_file=True)
    assert type_and_key == ("ANY FILE", "")

    grant = Grant(principal="user", action_type="SELECT", catalog="hive_metastore", any_file=True)
    assert grant.this_type_and_key()[0] == "ANY FILE"
    assert grant.object_key == ""


def test_type_and_key_anonymous_function() -> None:
    type_and_key = Grant.type_and_key(anonymous_function=True)
    assert type_and_key == ("ANONYMOUS FUNCTION", "")

    grant = Grant(principal="user", action_type="SELECT", catalog="hive_metastore", anonymous_function=True)
    assert grant.this_type_and_key()[0] == "ANONYMOUS FUNCTION"
    assert grant.object_key == ""


def test_type_and_key_udf() -> None:
    type_and_key = Grant.type_and_key(catalog="hive_metastore", database="mydb", udf="myfunction")
    assert type_and_key == ("FUNCTION", "hive_metastore.mydb.myfunction")

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


@pytest.mark.parametrize(
    "src, grant, dst, query",
    [
        (
            Catalog("hive_metastore"),
            Grant("user", "USAGE"),
            Catalog("catalog"),
            "GRANT USE CATALOG ON CATALOG `catalog` TO `user`",
        ),
        (
            Schema("hive_metastore", "schema"),
            Grant("user", "USAGE"),
            Schema("catalog", "schema"),
            "GRANT USE SCHEMA ON DATABASE `catalog`.`schema` TO `user`",
        ),
        (
            Table("hive_metastore", "database", "table", "MANAGED", "DELTA"),
            Grant("user", "SELECT"),
            Table("catalog", "database", "table", "MANAGED", "DELTA"),
            "GRANT SELECT ON TABLE `catalog`.`database`.`table` TO `user`",
        ),
    ],
)
def test_migrate_grants_applies_query(
    src: Catalog | Schema | Table,
    grant: Grant,
    dst: Catalog | Schema | Table,
    query: str,
) -> None:
    group_manager = create_autospec(GroupManager)
    backend = MockBackend()

    def grant_loader() -> list[Grant]:
        database = table = None
        if isinstance(src, Catalog):
            catalog = src.name
        elif isinstance(src, Schema):
            catalog = src.catalog
            database = src.name
        elif isinstance(src, Table):
            catalog = src.catalog
            database = src.database
            table = src.name
        else:
            raise TypeError(f"Unsupported source type: {type(src)}")
        return [
            Grant(
                'me',
                'OWN',
                catalog=catalog,
                database=database,
                table=table,
            ),
            dataclasses.replace(
                grant,
                catalog=catalog,
                database=database,
                table=table,
            ),
        ]

    migrate_grants = MigrateGrants(
        backend,
        group_manager,
        [grant_loader, grant_loader],
    )

    migrate_grants.apply(src, dst)

    assert query in backend.queries
    group_manager.assert_not_called()


@pytest.mark.parametrize(
    "src, src_grants, dst, query",
    [
        (
            Catalog("hive_metastore"),
            [Grant("user", "USAGE"), Grant("user", "OWN")],
            Catalog("catalog"),
            "ALTER CATALOG `catalog` OWNER TO `fake_owner`",
        ),
        (
            Schema("hive_metastore", "schema"),
            [Grant("user", "USAGE"), Grant("user", "OWN")],
            Schema("catalog", "schema"),
            "ALTER DATABASE `catalog`.`schema` OWNER TO `fake_owner`",
        ),
        (
            Table("hive_metastore", "database", "table", "MANAGED", "DELTA"),
            [Grant("user", "SELECT"), Grant("user", "OWN")],
            Table("catalog", "database", "table", "MANAGED", "DELTA"),
            "ALTER TABLE `catalog`.`database`.`table` OWNER TO `fake_owner`",
        ),
        (
            Catalog("hive_metastore"),
            [Grant("user", "OWN")],
            Catalog("catalog"),
            "ALTER CATALOG `catalog` OWNER TO `fake_owner`",
        ),
        (
            Schema("hive_metastore", "schema"),
            [Grant("user", "OWN")],
            Schema("catalog", "schema"),
            "ALTER DATABASE `catalog`.`schema` OWNER TO `fake_owner`",
        ),
        (
            Table("hive_metastore", "database", "table", "MANAGED", "DELTA"),
            [Grant("user", "OWN")],
            Table("catalog", "database", "table", "MANAGED", "DELTA"),
            "ALTER TABLE `catalog`.`database`.`table` OWNER TO `fake_owner`",
        ),
    ],
)
def test_migrate_grants_set_fixed_owner(
    src: Catalog | Schema | Table,
    src_grants: list[Grant],
    dst: Catalog | Schema | Table,
    query: str,
) -> None:
    group_manager = create_autospec(GroupManager)
    backend = MockBackend()

    def grant_loader() -> list[Grant]:
        grants = []
        for grant in src_grants:
            database = table = None
            if isinstance(src, Catalog):
                catalog = src.name
            elif isinstance(src, Schema):
                catalog = src.catalog
                database = src.name
            elif isinstance(src, Table):
                catalog = src.catalog
                database = src.database
                table = src.name
            else:
                raise TypeError(f"Unsupported source type: {type(src)}")
            grants.append(
                dataclasses.replace(
                    grant,
                    catalog=catalog,
                    database=database,
                    table=table,
                )
            )
            grants.append(
                Grant(
                    'fake_owner',
                    'OWN',
                    catalog=catalog,
                    database=database,
                    table=table,
                )
            )
        return grants

    migrate_grants = MigrateGrants(
        backend,
        group_manager,
        [grant_loader, grant_loader],
    )

    migrate_grants.apply(src, dst)

    # asserting the query was submitted
    assert query in backend.queries
    group_manager.assert_not_called()


def test_migrate_grants_alters_ownership_as_last() -> None:
    queries = [
        "ALTER DATABASE `catalog`.`schema` OWNER TO `fake_user`",
        "GRANT USE SCHEMA ON DATABASE `catalog`.`schema` TO `user`",
    ]
    group_manager = create_autospec(GroupManager)
    backend = MockBackend()

    def grant_loader() -> list[Grant]:
        return [
            Grant("user", "OWN", "hive_metastore", "schema"),
            Grant("user", "USAGE", "hive_metastore", "schema"),
        ]

    def one_owner() -> list[Grant]:
        return [
            Grant("fake_user", "OWN", "hive_metastore", "schema"),
        ]

    migrate_grants = MigrateGrants(
        backend,
        group_manager,
        [one_owner, grant_loader],
    )
    src = Schema("hive_metastore", "schema")
    dst = Schema("catalog", "schema")

    migrate_grants.apply(src, dst)

    assert backend.queries == queries
    group_manager.assert_not_called()


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

    def no_owner() -> list[Grant]:
        return []

    migrate_grants = MigrateGrants(
        MockBackend(),
        group_manager,
        [no_owner, grant_loader],
    )

    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.hive_metastore.grants"):
        migrate_grants.apply(table, dataclasses.replace(table, catalog="catalog"))
    assert (
        "failed-to-migrate: Hive metastore grant 'READ_METADATA' cannot be mapped to UC grant for TABLE 'catalog.database.table'"
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


@pytest.mark.parametrize(
    "grant_record,history_record",
    (
        (
            Grant(principal="user@domain", action_type="SELECT", catalog="main", database="foo", table="bar"),
            Row(
                workspace_id=2,
                job_run_id=1,
                object_type="Grant",
                object_id=["TABLE", "main.foo.bar", "SELECT", "user@domain"],
                data={
                    "principal": "user@domain",
                    "action_type": "SELECT",
                    "catalog": "main",
                    "database": "foo",
                    "table": "bar",
                    "any_file": "false",
                    "anonymous_function": "false",
                },
                failures=[],
                owner="the_admin",
                ucx_version=ucx_version,
            ),
        ),
        (
            Grant(principal="user@domain", action_type="SELECT", catalog="main", database="foo", view="bar"),
            Row(
                workspace_id=2,
                job_run_id=1,
                object_type="Grant",
                object_id=["VIEW", "main.foo.bar", "SELECT", "user@domain"],
                data={
                    "principal": "user@domain",
                    "action_type": "SELECT",
                    "catalog": "main",
                    "database": "foo",
                    "view": "bar",
                    "any_file": "false",
                    "anonymous_function": "false",
                },
                failures=[],
                owner="the_admin",
                ucx_version=ucx_version,
            ),
        ),
        (
            Grant(principal="user@domain", action_type="SELECT", catalog="main", database="foo", udf="bar"),
            Row(
                workspace_id=2,
                job_run_id=1,
                object_type="Grant",
                object_id=["FUNCTION", "main.foo.bar", "SELECT", "user@domain"],
                data={
                    "principal": "user@domain",
                    "action_type": "SELECT",
                    "catalog": "main",
                    "database": "foo",
                    "udf": "bar",
                    "any_file": "false",
                    "anonymous_function": "false",
                },
                failures=[],
                owner="the_admin",
                ucx_version=ucx_version,
            ),
        ),
        (
            Grant(principal="user@domain", action_type="SELECT", catalog="main", database="foo", udf="bar"),
            Row(
                workspace_id=2,
                job_run_id=1,
                object_type="Grant",
                object_id=["FUNCTION", "main.foo.bar", "SELECT", "user@domain"],
                data={
                    "principal": "user@domain",
                    "action_type": "SELECT",
                    "catalog": "main",
                    "database": "foo",
                    "udf": "bar",
                    "any_file": "false",
                    "anonymous_function": "false",
                },
                failures=[],
                owner="the_admin",
                ucx_version=ucx_version,
            ),
        ),
        (
            Grant(principal="user@domain", action_type="ALL_PRIVILEGES", catalog="main", database="foo"),
            Row(
                workspace_id=2,
                job_run_id=1,
                object_type="Grant",
                object_id=["DATABASE", "main.foo", "ALL_PRIVILEGES", "user@domain"],
                data={
                    "principal": "user@domain",
                    "action_type": "ALL_PRIVILEGES",
                    "catalog": "main",
                    "database": "foo",
                    "any_file": "false",
                    "anonymous_function": "false",
                },
                failures=[],
                owner="the_admin",
                ucx_version=ucx_version,
            ),
        ),
        (
            Grant(principal="user@domain", action_type="ALL_PRIVILEGES", catalog="main"),
            Row(
                workspace_id=2,
                job_run_id=1,
                object_type="Grant",
                object_id=["CATALOG", "main", "ALL_PRIVILEGES", "user@domain"],
                data={
                    "principal": "user@domain",
                    "action_type": "ALL_PRIVILEGES",
                    "catalog": "main",
                    "any_file": "false",
                    "anonymous_function": "false",
                },
                failures=[],
                owner="the_admin",
                ucx_version=ucx_version,
            ),
        ),
        (
            Grant(principal="user@domain", action_type="SELECT", any_file=True),
            Row(
                workspace_id=2,
                job_run_id=1,
                object_type="Grant",
                object_id=["ANY FILE", "", "SELECT", "user@domain"],
                data={
                    "principal": "user@domain",
                    "action_type": "SELECT",
                    "any_file": "true",
                    "anonymous_function": "false",
                },
                failures=[],
                owner="the_admin",
                ucx_version=ucx_version,
            ),
        ),
        (
            Grant(principal="user@domain", action_type="SELECT", anonymous_function=True),
            Row(
                workspace_id=2,
                job_run_id=1,
                object_type="Grant",
                object_id=["ANONYMOUS FUNCTION", "", "SELECT", "user@domain"],
                data={
                    "principal": "user@domain",
                    "action_type": "SELECT",
                    "any_file": "false",
                    "anonymous_function": "true",
                },
                failures=[],
                owner="the_admin",
                ucx_version=ucx_version,
            ),
        ),
        (
            Grant(principal="user@domain", action_type="ALL_PRIVILEGES", database="foo"),
            Row(
                workspace_id=2,
                job_run_id=1,
                object_type="Grant",
                object_id=["DATABASE", "hive_metastore.foo", "ALL_PRIVILEGES", "user@domain"],
                data={
                    "principal": "user@domain",
                    "action_type": "ALL_PRIVILEGES",
                    "database": "foo",
                    "any_file": "false",
                    "anonymous_function": "false",
                },
                failures=[],
                owner="the_admin",
                ucx_version=ucx_version,
            ),
        ),
    ),
)
def test_grant_supports_history(mock_backend, grant_record: Grant, history_record: Row) -> None:
    """Verify that Grant records are written to the history log as expected."""
    mock_ownership = create_autospec(GrantOwnership)
    mock_ownership.owner_of.return_value = "the_admin"
    history_log = ProgressEncoder[Grant](
        mock_backend, mock_ownership, Grant, run_id=1, workspace_id=2, catalog="a_catalog"
    )

    history_log.append_inventory_snapshot([grant_record])

    rows = mock_backend.rows_written_for("`a_catalog`.`multiworkspace`.`historical`", mode="append")

    assert rows == [history_record]


# Testing the validation in retrival of the default owner group. 666 is the current_user user_id.
@pytest.mark.parametrize("user_id, expected", [("666", True), ("777", False)])
def test_default_owner(user_id, expected) -> None:
    sql_backend = MockBackend()
    ws = mock_workspace_client()
    download_yaml = {
        'config.yml': yaml.dump(
            {
                'version': 2,
                'inventory_database': 'ucx',
                'default_owner_group': 'owners',
                'connect': {
                    'host': '...',
                    'token': '...',
                },
            }
        ),
        'workspaces.json': None,
    }

    ws.workspace.download.side_effect = lambda file_name: io.StringIO(download_yaml[os.path.basename(file_name)])
    account_admins_group = Group(
        id="1234", display_name="owners", members=[ComplexValue(display="User Name", value=user_id)]
    )
    ws.api_client.do.return_value = {
        "Resources": [account_admins_group.as_dict()],
    }

    group_manager = GroupManager(sql_backend, ws, "ucx")
    assert group_manager.validate_owner_group("owners") == expected

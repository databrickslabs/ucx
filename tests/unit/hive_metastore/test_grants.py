import pytest

from databricks.labs.ucx.hive_metastore.grants import Grant, GrantsCrawler
from databricks.labs.ucx.hive_metastore.tables import TablesCrawler
from databricks.labs.ucx.hive_metastore.udfs import UdfsCrawler
from databricks.labs.ucx.mixins.sql import Row

from ..framework.mocks import MockBackend


def test_type_and_key_table():
    grant = Grant.type_and_key(catalog="hive_metastore", database="mydb", table="mytable")
    assert grant == ("TABLE", "hive_metastore.mydb.mytable")

    grant = Grant(principal="user", action_type="SELECT", catalog="hive_metastore", database="mydb", table="mytable")
    assert grant.this_type_and_key()[0] == "TABLE"
    assert grant.object_key == "hive_metastore.mydb.mytable"


def test_type_and_key_view():
    grant = Grant.type_and_key(catalog="hive_metastore", database="mydb", view="myview")
    assert grant == ("VIEW", "hive_metastore.mydb.myview")

    grant = Grant(principal="user", action_type="SELECT", catalog="hive_metastore", database="mydb", view="myview")
    assert grant.this_type_and_key()[0] == "VIEW"
    assert grant.object_key == "hive_metastore.mydb.myview"


def test_type_and_key_database():
    grant = Grant.type_and_key(catalog="hive_metastore", database="mydb")
    assert grant == ("DATABASE", "hive_metastore.mydb")

    grant = Grant(principal="user", action_type="SELECT", catalog="hive_metastore", database="mydb")
    assert grant.this_type_and_key()[0] == "DATABASE"
    assert grant.object_key == "hive_metastore.mydb"


def test_type_and_key_catalog():
    grant = Grant.type_and_key(catalog="mycatalog")
    assert grant == ("CATALOG", "mycatalog")

    grant = Grant(principal="user", action_type="SELECT", catalog="mycatalog")
    assert grant.this_type_and_key()[0] == "CATALOG"
    assert grant.object_key == "mycatalog"


def test_type_and_key_any_file():
    grant = Grant.type_and_key(any_file=True)
    assert grant == ("ANY FILE", "")

    grant = Grant(principal="user", action_type="SELECT", catalog="hive_metastore", any_file=True)
    assert grant.this_type_and_key()[0] == "ANY FILE"
    assert grant.object_key == ""


def test_type_and_key_anonymous_function():
    grant = Grant.type_and_key(anonymous_function=True)
    assert grant == ("ANONYMOUS FUNCTION", "")

    grant = Grant(principal="user", action_type="SELECT", catalog="hive_metastore", anonymous_function=True)
    assert grant.this_type_and_key()[0] == "ANONYMOUS FUNCTION"
    assert grant.object_key == ""


def test_type_and_key_udf():
    grant = Grant.type_and_key(catalog="hive_metastore", database="mydb", udf="myfunction")
    assert grant == ("FUNCTION", "hive_metastore.mydb.myfunction")

    grant = Grant(principal="user", action_type="SELECT", catalog="hive_metastore", database="mydb", udf="myfunction")
    assert grant.this_type_and_key()[0] == "FUNCTION"
    assert grant.object_key == "hive_metastore.mydb.myfunction"


def test_type_and_key_invalid():
    with pytest.raises(ValueError):
        Grant.type_and_key()


def test_object_key():
    grant = Grant(principal="user", action_type="SELECT", catalog="hive_metastore", database="mydb", table="mytable")
    assert grant.object_key == "hive_metastore.mydb.mytable"


def test_hive_sql():
    grant = Grant(principal="user", action_type="SELECT", catalog="hive_metastore", database="mydb", table="mytable")
    assert grant.hive_grant_sql() == ["GRANT SELECT ON TABLE hive_metastore.mydb.mytable TO `user`"]
    assert grant.hive_revoke_sql() == "REVOKE SELECT ON TABLE hive_metastore.mydb.mytable FROM `user`"


def test_hive_table_own_sql():
    grant = Grant(principal="user", action_type="OWN", catalog="hive_metastore", database="mydb", table="mytable")
    assert grant.hive_grant_sql() == ["ALTER TABLE hive_metastore.mydb.mytable OWNER TO `user`"]


def test_hive_database_own_sql():
    grant = Grant(principal="user", action_type="OWN", catalog="hive_metastore", database="mydb")
    assert grant.hive_grant_sql() == ["ALTER DATABASE hive_metastore.mydb OWNER TO `user`"]


def test_hive_udf_own_sql():
    grant = Grant(principal="user", action_type="OWN", catalog="hive_metastore", database="mydb", udf="myfunction")
    assert grant.hive_grant_sql() == ["ALTER FUNCTION hive_metastore.mydb.myfunction OWNER TO `user`"]


def test_hive_revoke_sql():
    grant = Grant(principal="user", action_type="SELECT", catalog="hive_metastore", database="mydb", table="mytable")
    assert grant.hive_revoke_sql() == "REVOKE SELECT ON TABLE hive_metastore.mydb.mytable FROM `user`"


@pytest.mark.parametrize(
    "grant,query",
    [
        (
            Grant("user", "READ_METADATA", catalog="hive_metastore", database="mydb", table="mytable"),
            "GRANT BROWSE ON TABLE hive_metastore.mydb.mytable TO `user`",
        ),
        (
            Grant("me", "OWN", catalog="hive_metastore", database="mydb", table="mytable"),
            "ALTER TABLE hive_metastore.mydb.mytable OWNER TO `me`",
        ),
        (
            Grant("me", "USAGE", catalog="hive_metastore", database="mydb"),
            "GRANT USE SCHEMA ON DATABASE hive_metastore.mydb TO `me`",
        ),
        (
            Grant("me", "INVALID", catalog="hive_metastore", database="mydb"),
            None,
        ),
        (
            Grant("me", "SELECT", catalog="hive_metastore", database="mydb", udf="myfunction"),
            "GRANT EXECUTE ON FUNCTION hive_metastore.mydb.myfunction TO `me`",
        ),
    ],
)
def test_uc_sql(grant, query):
    assert grant.uc_grant_sql() == query


def make_row(data, columns):
    row = Row(data)
    row.__columns__ = columns
    return row


SELECT_COLS = ["catalog", "database", "table", "object_type", "table_format", "location", "view_text"]
SHOW_COLS = ["principal", "action_type", "object_type", "ignored"]
DESCRIBE_COLS = ["key", "value", "ignored"]
ROWS = {
    "SELECT.*": [
        make_row(("foo", "bar", "test_table", "type", "DELTA", "/foo/bar/test", None), SELECT_COLS),
        make_row(("foo", "bar", "test_view", "type", "VIEW", None, "SELECT * FROM table"), SELECT_COLS),
        make_row(("foo", None, None, "type", "CATALOG", None, None), SELECT_COLS),
    ],
    "SHOW.*": [
        make_row(("princ1", "SELECT", "TABLE", "ignored"), SHOW_COLS),
        make_row(("princ1", "SELECT", "VIEW", "ignored"), SHOW_COLS),
        make_row(("princ1", "USE", "CATALOG$", "ignored"), SHOW_COLS),
    ],
    "DESCRIBE.*": [
        make_row(("Catalog", "foo", "ignored"), DESCRIBE_COLS),
        make_row(("Type", "TABLE", "ignored"), DESCRIBE_COLS),
        make_row(("Provider", "", "ignored"), DESCRIBE_COLS),
        make_row(("Location", "/foo/bar/test", "ignored"), DESCRIBE_COLS),
        make_row(("View Text", "SELECT * FROM table", "ignored"), DESCRIBE_COLS),
    ],
}


def test_crawler_no_data():
    b = MockBackend()
    table = TablesCrawler(b, "schema")
    udf = UdfsCrawler(b, "schema")
    crawler = GrantsCrawler(table, udf)
    grants = crawler.snapshot()
    assert len(grants) == 0


def test_crawler_crawl():
    b = MockBackend(
        rows={
            "SHOW DATABASES": [
                make_row(("database_one",), ["databaseName"]),
                make_row(("database_two",), ["databaseName"]),
            ],
            "SHOW TABLES FROM hive_metastore.database_one": [
                ("database_one", "table_one", "true"),
                ("database_one", "table_two", "true"),
            ],
            "SELECT * FROM hive_metastore.schema.tables": [
                make_row(("foo", "bar", "test_table", "type", "DELTA", "/foo/bar/test", None), SELECT_COLS),
                make_row(("foo", "bar", "test_view", "type", "VIEW", None, "SELECT * FROM table"), SELECT_COLS),
                make_row(("foo", None, None, "type", "CATALOG", None, None), SELECT_COLS),
            ],
            "DESCRIBE TABLE EXTENDED hive_metastore.database_one.*": [
                make_row(("Catalog", "foo", "ignored"), DESCRIBE_COLS),
                make_row(("Type", "TABLE", "ignored"), DESCRIBE_COLS),
                make_row(("Provider", "", "ignored"), DESCRIBE_COLS),
                make_row(("Location", "/foo/bar/test", "ignored"), DESCRIBE_COLS),
                make_row(("View Text", "SELECT * FROM table", "ignored"), DESCRIBE_COLS),
            ],
            "SHOW GRANTS ON .*": [
                make_row(("princ1", "SELECT", "TABLE", "ignored"), SHOW_COLS),
                make_row(("princ1", "SELECT", "VIEW", "ignored"), SHOW_COLS),
                make_row(("princ1", "USE", "CATALOG$", "ignored"), SHOW_COLS),
            ],
        }
    )
    table = TablesCrawler(b, "schema")
    udf = UdfsCrawler(b, "schema")
    crawler = GrantsCrawler(table, udf)
    grants = crawler.snapshot()
    assert len(grants) == 3


def test_crawler_udf_crawl():
    b = MockBackend(
        rows={
            "SHOW DATABASES": [
                make_row(("database_one",), ["databaseName"]),
            ],
            "SHOW USER FUNCTIONS FROM hive_metastore.database_one": [
                ("hive_metastore.database_one.function_one"),
                ("hive_metastore.database_one.function_two"),
            ],
            "DESCRIBE FUNCTION EXTENDED hive_metastore.database_one.*": [
                ("Type: SCALAR"),
                ("Input: p INT"),
                ("Returns: FLOAT"),
                ("Deterministic: true"),
                ("Data Access: CONTAINS SQL"),
                ("Body: 1"),
                ("ignore"),
            ],
            "SHOW GRANTS ON .*": [
                make_row(("princ1", "SELECT", "FUNCTION", "ignored"), SHOW_COLS),
            ],
        }
    )

    table = TablesCrawler(b, "schema")
    udf = UdfsCrawler(b, "schema")
    crawler = GrantsCrawler(table, udf)
    grants = crawler.snapshot()

    assert len(grants) == 2
    assert Grant(
        principal="princ1",
        action_type="SELECT",
        catalog="hive_metastore",
        database="database_one",
        table=None,
        view=None,
        udf="function_one",
        any_file=False,
        anonymous_function=False,
    ) == next(g for g in grants if g.udf == "function_one")
    assert Grant(
        principal="princ1",
        action_type="SELECT",
        catalog="hive_metastore",
        database="database_one",
        table=None,
        view=None,
        udf="function_two",
        any_file=False,
        anonymous_function=False,
    ) == next(g for g in grants if g.udf == "function_two")


def test_crawler_snapshot():
    # Test with no data
    b = MockBackend()
    table = TablesCrawler(b, "schema")
    udf = UdfsCrawler(b, "schema")
    crawler = GrantsCrawler(table, udf)
    snapshot = crawler.snapshot()
    assert len(snapshot) == 0

    # Test with test data
    b = MockBackend(rows=ROWS)
    table = TablesCrawler(b, "schema")
    udf = UdfsCrawler(b, "schema")
    crawler = GrantsCrawler(table, udf)
    snapshot = crawler.snapshot()
    assert len(snapshot) == 3


def test_grants_returning_error_when_describing():
    errors = {"SHOW GRANTS ON TABLE hive_metastore.test_database.table1": "error"}
    rows = {
        "SHOW DATABASES": [
            make_row(("test_database",), ["databaseName"]),
            make_row(("other_database",), ["databaseName"]),
        ],
        "SHOW TABLES FROM hive_metastore.test_database": [
            ("test_database", "table1", False),
            ("test_database", "table2", False),
        ],
        "SHOW GRANTS ON TABLE hive_metastore.test_database.table2": [("principal1", "OWNER", "TABLE", "")],
        "DESCRIBE *": [
            ("Catalog", "catalog", ""),
            ("Type", "delta", ""),
        ],
    }

    backend = MockBackend(fails_on_first=errors, rows=rows)
    tc = TablesCrawler(backend, "default")
    udf = UdfsCrawler(backend, "default")
    crawler = GrantsCrawler(tc, udf)

    results = crawler._crawl()
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


def test_udf_grants_returning_error_when_describing():
    errors = {"SHOW GRANTS ON FUNCTION hive_metastore.test_database.function_bad": "error"}
    rows = {
        "SHOW DATABASES": [
            make_row(("test_database",), ["databaseName"]),
            make_row(("other_database",), ["databaseName"]),
        ],
        "SHOW USER FUNCTIONS FROM hive_metastore.test_database": [
            ("hive_metastore.test_database.function_bad"),
            ("hive_metastore.test_database.function_good"),
        ],
        "SHOW GRANTS ON FUNCTION hive_metastore.test_database.function_good": [("principal1", "OWN", "FUNCTION", "")],
        "DESCRIBE *": [
            ("Type: SCALAR"),
            ("Body: 1"),
        ],
    }

    backend = MockBackend(fails_on_first=errors, rows=rows)
    tc = TablesCrawler(backend, "default")
    udf = UdfsCrawler(backend, "default")
    crawler = GrantsCrawler(tc, udf)

    results = crawler._crawl()
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

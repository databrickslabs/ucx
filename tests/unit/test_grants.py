import pytest

from databricks.labs.ucx.providers.mixins.sql import Row
from databricks.labs.ucx.tacl.grants import Grant, GrantsCrawler
from databricks.labs.ucx.tacl.tables import TablesCrawler

from .mocks import MockBackend


def test_type_and_key_table():
    grant = Grant.type_and_key(catalog="hive_metastore", database="mydb", table="mytable")
    assert grant == ("TABLE", "hive_metastore.mydb.mytable")

    grant = Grant(principal="user", action_type="SELECT", catalog="hive_metastore", database="mydb", table="mytable")
    assert grant._this_type_and_key()[0] == "TABLE"
    assert grant.object_key == "hive_metastore.mydb.mytable"


def test_type_and_key_view():
    grant = Grant.type_and_key(catalog="hive_metastore", database="mydb", view="myview")
    assert grant == ("VIEW", "hive_metastore.mydb.myview")

    grant = Grant(principal="user", action_type="SELECT", catalog="hive_metastore", database="mydb", view="myview")
    assert grant._this_type_and_key()[0] == "VIEW"
    assert grant.object_key == "hive_metastore.mydb.myview"


def test_type_and_key_database():
    grant = Grant.type_and_key(catalog="hive_metastore", database="mydb")
    assert grant == ("DATABASE", "hive_metastore.mydb")

    grant = Grant(principal="user", action_type="SELECT", catalog="hive_metastore", database="mydb")
    assert grant._this_type_and_key()[0] == "DATABASE"
    assert grant.object_key == "hive_metastore.mydb"


def test_type_and_key_catalog():
    grant = Grant.type_and_key(catalog="mycatalog")
    assert grant == ("CATALOG", "mycatalog")

    grant = Grant(principal="user", action_type="SELECT", catalog="mycatalog")
    assert grant._this_type_and_key()[0] == "CATALOG"
    assert grant.object_key == "mycatalog"


def test_type_and_key_any_file():
    grant = Grant.type_and_key(any_file=True)
    assert grant == ("ANY FILE", "")

    grant = Grant(principal="user", action_type="SELECT", catalog="hive_metastore", any_file=True)
    assert grant._this_type_and_key()[0] == "ANY FILE"
    assert grant.object_key == ""


def test_type_and_key_anonymous_function():
    grant = Grant.type_and_key(anonymous_function=True)
    assert grant == ("ANONYMOUS FUNCTION", "")

    grant = Grant(principal="user", action_type="SELECT", catalog="hive_metastore", anonymous_function=True)
    assert grant._this_type_and_key()[0] == "ANONYMOUS FUNCTION"
    assert grant.object_key == ""


def test_type_and_key_invalid():
    with pytest.raises(ValueError):
        Grant.type_and_key()


def test_object_key():
    grant = Grant(principal="user", action_type="SELECT", catalog="hive_metastore", database="mydb", table="mytable")
    assert grant.object_key == "hive_metastore.mydb.mytable"


def test_hive_sql():
    grant = Grant(principal="user", action_type="SELECT", catalog="hive_metastore", database="mydb", table="mytable")
    assert grant.hive_grant_sql() == "GRANT SELECT ON TABLE hive_metastore.mydb.mytable TO user"
    assert grant.hive_revoke_sql() == "REVOKE SELECT ON TABLE hive_metastore.mydb.mytable FROM user"


def test_hive_revoke_sql():
    grant = Grant(principal="user", action_type="SELECT", catalog="hive_metastore", database="mydb", table="mytable")
    assert grant.hive_revoke_sql() == "REVOKE SELECT ON TABLE hive_metastore.mydb.mytable FROM user"


@pytest.mark.parametrize(
    "grant,query",
    [
        (
            Grant("user", "READ_METADATA", catalog="hive_metastore", database="mydb", table="mytable"),
            "GRANT BROWSE ON TABLE hive_metastore.mydb.mytable TO user",
        ),
        (
            Grant("me", "OWN", catalog="hive_metastore", database="mydb", table="mytable"),
            "ALTER TABLE hive_metastore.mydb.mytable OWNER TO me",
        ),
        (
            Grant("me", "USAGE", catalog="hive_metastore", database="mydb"),
            "GRANT USE SCHEMA ON DATABASE hive_metastore.mydb TO me",
        ),
        (
            Grant("me", "INVALID", catalog="hive_metastore", database="mydb"),
            None,
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


def test_crawler_crawl():
    # Test with no data
    b = MockBackend()
    table = TablesCrawler(b, "hive_metastore", "schema")
    crawler = GrantsCrawler(table)
    grants = crawler._crawl("hive_metastore", "schema")
    assert len(grants) == 0
    # Test with test data
    b = MockBackend(rows=ROWS)
    table = TablesCrawler(b, "hive_metastore", "schema")
    crawler = GrantsCrawler(table)
    grants = crawler._crawl("hive_metastore", "schema")
    assert len(grants) == 3


def test_crawler_snapshot():
    # Test with no data
    b = MockBackend()
    table = TablesCrawler(b, "hive_metastore", "schema")
    crawler = GrantsCrawler(table)
    snapshot = crawler.snapshot("hive_metastore", "schema")
    assert len(snapshot) == 0
    # Test with test data
    b = MockBackend(rows=ROWS)
    table = TablesCrawler(b, "hive_metastore", "schema")
    crawler = GrantsCrawler(table)
    snapshot = crawler.snapshot("hive_metastore", "schema")
    assert len(snapshot) == 3

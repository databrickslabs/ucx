import pytest
from databricks.labs.lsql.backends import MockBackend

from databricks.labs.ucx.hive_metastore.grants import Grant, GrantsCrawler
from databricks.labs.ucx.hive_metastore.tables import TablesCrawler
from databricks.labs.ucx.hive_metastore.udfs import UdfsCrawler


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


def test_type_and_key_external_location():
    grant = Grant.type_and_key(external_location="myexternallocation")
    assert grant == ("EXTERNAL LOCATION", "myexternallocation")

    grant = Grant(principal="user", action_type="SELECT", external_location="myexternallocation")
    assert grant.this_type_and_key()[0] == "EXTERNAL LOCATION"
    assert grant.object_key == "myexternallocation"


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


UCX_TABLES = MockBackend.rows("catalog", "database", "table", "object_type", "table_format", "location", "view_text")
DESCRIBE_TABLE = MockBackend.rows("key", "value", "ignored")
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


def test_crawler_no_data():
    sql_backend = MockBackend()
    table = TablesCrawler(sql_backend, "schema")
    udf = UdfsCrawler(sql_backend, "schema")
    crawler = GrantsCrawler(table, udf)
    grants = crawler.snapshot()
    assert len(grants) == 0


def test_crawler_crawl():
    sql_backend = MockBackend(
        rows={
            "SHOW DATABASES": SHOW_DATABASES[
                ("database_one",),
                ("database_two",),
            ],
            "SHOW TABLES FROM hive_metastore.database_one": SHOW_TABLES[
                ("database_one", "table_one", "true"),
                ("database_one", "table_two", "true"),
            ],
            "SELECT * FROM hive_metastore.schema.tables": UCX_TABLES[
                ("foo", "bar", "test_table", "type", "DELTA", "/foo/bar/test", None),
                ("foo", "bar", "test_view", "type", "VIEW", None, "SELECT * FROM table"),
                ("foo", None, None, "type", "CATALOG", None, None),
            ],
            "DESCRIBE TABLE EXTENDED hive_metastore.database_one.*": DESCRIBE_TABLE[
                ("Catalog", "foo", "ignored"),
                ("Type", "TABLE", "ignored"),
                ("Provider", "", "ignored"),
                ("Location", "/foo/bar/test", "ignored"),
                ("View Text", "SELECT * FROM table", "ignored"),
            ],
            "SHOW GRANTS ON .*": SHOW_GRANTS[
                ("princ1", "SELECT", "TABLE", "ignored"),
                ("princ1", "SELECT", "VIEW", "ignored"),
                ("princ1", "USE", "CATALOG$", "ignored"),
            ],
        }
    )
    table = TablesCrawler(sql_backend, "schema")
    udf = UdfsCrawler(sql_backend, "schema")
    crawler = GrantsCrawler(table, udf)
    grants = crawler.snapshot()
    assert len(grants) == 3


def test_crawler_udf_crawl():
    sql_backend = MockBackend(
        rows={
            "SHOW DATABASES": SHOW_DATABASES[("database_one",),],
            "SHOW USER FUNCTIONS FROM hive_metastore.database_one": SHOW_FUNCTIONS[
                ("hive_metastore.database_one.function_one",),
                ("hive_metastore.database_one.function_two",),
            ],
            "DESCRIBE FUNCTION EXTENDED hive_metastore.database_one.*": MockBackend.rows("something")[
                ("Type: SCALAR",),
                ("Input: p INT",),
                ("Returns: FLOAT",),
                ("Deterministic: true",),
                ("Data Access: CONTAINS SQL",),
                ("Body: 1",),
                ("ignore",),
            ],
            "SHOW GRANTS ON .*": SHOW_GRANTS[("princ1", "SELECT", "FUNCTION", "ignored"),],
        }
    )

    table = TablesCrawler(sql_backend, "schema")
    udf = UdfsCrawler(sql_backend, "schema")
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


def test_crawler_snapshot_when_no_data():
    sql_backend = MockBackend()
    table = TablesCrawler(sql_backend, "schema")
    udf = UdfsCrawler(sql_backend, "schema")
    crawler = GrantsCrawler(table, udf)
    snapshot = crawler.snapshot()
    assert len(snapshot) == 0


def test_crawler_snapshot_with_data():
    sql_backend = MockBackend(rows=ROWS)
    table = TablesCrawler(sql_backend, "schema")
    udf = UdfsCrawler(sql_backend, "schema")
    crawler = GrantsCrawler(table, udf)
    snapshot = crawler.snapshot()
    assert len(snapshot) == 3


def test_grants_returning_error_when_showing_grants():
    errors = {"SHOW GRANTS ON TABLE hive_metastore.test_database.table1": "error"}
    rows = {
        "SHOW DATABASES": SHOW_DATABASES[
            ("test_database",),
            ("other_database",),
        ],
        "SHOW TABLES FROM hive_metastore.test_database": SHOW_TABLES[
            ("test_database", "table1", False),
            ("test_database", "table2", False),
        ],
        "SHOW GRANTS ON TABLE hive_metastore.test_database.table2": SHOW_GRANTS[("principal1", "OWNER", "TABLE", ""),],
        "DESCRIBE *": DESCRIBE_TABLE[
            ("Catalog", "catalog", ""),
            ("Type", "delta", ""),
        ],
    }

    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = TablesCrawler(backend, "default")
    udf = UdfsCrawler(backend, "default")
    crawler = GrantsCrawler(table_crawler, udf)

    results = crawler.snapshot()
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


def test_grants_returning_error_when_describing():
    errors = {"DESCRIBE TABLE EXTENDED hive_metastore.test_database.table1": "error"}
    rows = {
        "SHOW DATABASES": SHOW_DATABASES[("test_database",),],
        "SHOW TABLES FROM hive_metastore.test_database": SHOW_TABLES[
            ("test_database", "table1", False),
            ("test_database", "table2", False),
        ],
        "SHOW GRANTS ON TABLE hive_metastore.test_database.table2": SHOW_GRANTS[("principal1", "OWNER", "TABLE", ""),],
        "DESCRIBE *": DESCRIBE_TABLE[
            ("Catalog", "catalog", ""),
            ("Type", "delta", ""),
        ],
    }

    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = TablesCrawler(backend, "default")
    udf = UdfsCrawler(backend, "default")
    crawler = GrantsCrawler(table_crawler, udf)

    results = crawler.snapshot()
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


def test_udf_grants_returning_error_when_showing_grants():
    errors = {"SHOW GRANTS ON FUNCTION hive_metastore.test_database.function_bad": "error"}
    rows = {
        "SHOW DATABASES": SHOW_DATABASES[
            ("test_database",),
            ("other_database",),
        ],
        "SHOW USER FUNCTIONS FROM hive_metastore.test_database": SHOW_FUNCTIONS[
            ("hive_metastore.test_database.function_bad",),
            ("hive_metastore.test_database.function_good",),
        ],
        "SHOW GRANTS ON FUNCTION hive_metastore.test_database.function_good": SHOW_GRANTS[
            ("principal1", "OWN", "FUNCTION", "")
        ],
        "DESCRIBE *": SHOW_FUNCTIONS[
            ("Type: SCALAR",),
            ("Body: 1",),
        ],
    }

    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = TablesCrawler(backend, "default")
    udf = UdfsCrawler(backend, "default")
    crawler = GrantsCrawler(table_crawler, udf)

    results = crawler.snapshot()
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


def test_udf_grants_returning_error_when_describing():
    errors = {"DESCRIBE FUNCTION EXTENDED hive_metastore.test_database.function_bad": "error"}
    rows = {
        "SHOW DATABASES": SHOW_DATABASES[("test_database",),],
        "SHOW USER FUNCTIONS FROM hive_metastore.test_database": SHOW_FUNCTIONS[
            ("hive_metastore.test_database.function_bad",),
            ("hive_metastore.test_database.function_good",),
        ],
        "SHOW GRANTS ON FUNCTION hive_metastore.test_database.function_good": SHOW_GRANTS[
            ("principal1", "OWN", "FUNCTION", ""),
        ],
        "DESCRIBE *": SHOW_FUNCTIONS[
            ("Type: SCALAR",),
            ("Body: 1",),
        ],
    }

    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = TablesCrawler(backend, "default")
    udf = UdfsCrawler(backend, "default")
    crawler = GrantsCrawler(table_crawler, udf)

    results = crawler.snapshot()
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


def test_crawler_should_filter_databases():
    sql_backend = MockBackend(
        rows={
            "SHOW TABLES FROM hive_metastore.database_one": SHOW_TABLES[
                ("database_one", "table_one", "true"),
                ("database_one", "table_two", "true"),
            ],
            "SELECT * FROM hive_metastore.schema.tables": UCX_TABLES[
                ("foo", "bar", "test_table", "type", "DELTA", "/foo/bar/test", None),
                ("foo", "bar", "test_view", "type", "VIEW", None, "SELECT * FROM table"),
                ("foo", None, None, "type", "CATALOG", None, None),
            ],
            "DESCRIBE TABLE EXTENDED hive_metastore.database_one.*": DESCRIBE_TABLE[
                ("Catalog", "foo", "ignored"),
                ("Type", "TABLE", "ignored"),
                ("Provider", "", "ignored"),
                ("Location", "/foo/bar/test", "ignored"),
                ("View Text", "SELECT * FROM table", "ignored"),
            ],
            "SHOW GRANTS ON .*": SHOW_GRANTS[
                ("princ1", "SELECT", "TABLE", "ignored"),
                ("princ1", "SELECT", "VIEW", "ignored"),
                ("princ1", "USE", "CATALOG$", "ignored"),
            ],
        }
    )
    table = TablesCrawler(sql_backend, "schema", include_databases=["database_one"])
    udf = UdfsCrawler(sql_backend, "schema", include_databases=["database_one"])
    crawler = GrantsCrawler(table, udf, include_databases=["database_one"])
    grants = crawler.snapshot()
    assert len(grants) == 3
    assert 'SHOW TABLES FROM hive_metastore.database_one' in sql_backend.queries

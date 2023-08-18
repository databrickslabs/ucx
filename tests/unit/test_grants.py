import pytest

from uc_migration_toolkit.tacl.grants import Grant


def test_type_and_key_table():
    grant = Grant.type_and_key(catalog="hive_metastore", database="mydb", table="mytable")
    assert grant == ("TABLE", "hive_metastore.mydb.mytable")


def test_type_and_key_view():
    grant = Grant.type_and_key(catalog="hive_metastore", database="mydb", view="myview")
    assert grant == ("VIEW", "hive_metastore.mydb.myview")


def test_type_and_key_database():
    grant = Grant.type_and_key(catalog="hive_metastore", database="mydb")
    assert grant == ("DATABASE", "hive_metastore.mydb")


def test_type_and_key_catalog():
    grant = Grant.type_and_key(catalog="mycatalog")
    assert grant == ("CATALOG", "mycatalog")


def test_type_and_key_any_file():
    grant = Grant.type_and_key(any_file=True)
    assert grant == ("ANY FILE", "")


def test_type_and_key_anonymous_function():
    grant = Grant.type_and_key(anonymous_function=True)
    assert grant == ("ANONYMOUS FUNCTION", "")


def test_type_and_key_invalid():
    with pytest.raises(ValueError):
        Grant.type_and_key()


def test_hive_sql():
    grant = Grant(principal="user", action_type="SELECT", catalog="hive_metastore",
                  database="mydb", table="mytable")
    assert grant.hive_grant_sql() == "GRANT SELECT ON TABLE hive_metastore.mydb.mytable TO user"


@pytest.mark.parametrize("grant,query", [
    (Grant("user", "READ_METADATA", catalog="hive_metastore", database="mydb", table="mytable"),
     "GRANT BROWSE ON TABLE hive_metastore.mydb.mytable TO user"),
    (Grant("me", "OWN", catalog="hive_metastore", database="mydb", table="mytable"),
     "ALTER TABLE hive_metastore.mydb.mytable OWNER TO me"),
    (Grant("me", "USAGE", catalog="hive_metastore", database="mydb"),
     "GRANT USE SCHEMA ON DATABASE hive_metastore.mydb TO me"),
])
def test_uc_sql(grant, query):
    assert grant.uc_sql() == query

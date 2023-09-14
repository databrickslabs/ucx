import pytest

from databricks.labs.ucx.data_storage._internal import SqlBackend
from databricks.labs.ucx.data_storage.tables import Table, TablesCrawler


def test_is_delta_true():
    delta_table = Table(catalog="catalog", database="db", name="table", object_type="type", table_format="DELTA")
    assert delta_table.is_delta


def test_is_delta_false():
    non_delta_table = Table(catalog="catalog", database="db", name="table", object_type="type", table_format="PARQUET")
    assert not non_delta_table.is_delta


def test_key():
    table = Table(catalog="CATALOG", database="DB", name="TABLE", object_type="type", table_format="DELTA")
    assert table.key == "catalog.db.table"


def test_kind_table():
    table = Table(catalog="catalog", database="db", name="table", object_type="type", table_format="DELTA")
    assert table.kind == "TABLE"


def test_kind_view():
    view_table = Table(
        catalog="catalog",
        database="db",
        name="table",
        object_type="type",
        table_format="DELTA",
        view_text="SELECT * FROM table",
    )
    assert view_table.kind == "VIEW"


def test_sql_managed_non_delta():
    with pytest.raises(ValueError):
        Table(catalog="catalog", database="db", name="table", object_type="type", table_format="PARQUET")._sql_managed(
            "catalog"
        )


@pytest.mark.parametrize(
    "table,query",
    [
        (
            Table(catalog="catalog", database="db", name="managed_table", object_type="..", table_format="DELTA"),
            "CREATE TABLE IF NOT EXISTS new_catalog.db.managed_table DEEP CLONE "
            "catalog.db.managed_table;ALTER TABLE catalog.db.managed_table SET "
            "TBLPROPERTIES ('upgraded_to' = 'new_catalog.db.managed_table');",
        ),
        (
            Table(
                catalog="catalog",
                database="db",
                name="view",
                object_type="..",
                table_format="DELTA",
                view_text="SELECT * FROM table",
            ),
            "CREATE VIEW IF NOT EXISTS new_catalog.db.view AS SELECT * FROM table;",
        ),
        (
            Table(
                catalog="catalog",
                database="db",
                name="external_table",
                object_type="..",
                table_format="DELTA",
                location="s3a://foo/bar",
            ),
            "CREATE TABLE IF NOT EXISTS new_catalog.db.external_table LIKE "
            "catalog.db.external_table COPY LOCATION;ALTER TABLE "
            "catalog.db.external_table SET TBLPROPERTIES ('upgraded_to' = "
            "'new_catalog.db.external_table');",
        ),
    ],
)
def test_uc_sql(table, query):
    assert table.uc_create_sql("new_catalog") == query


def test_tables_crawler_inventory_table():
    tc = TablesCrawler(SqlBackend, "main", "default")
    assert tc._table == "tables"

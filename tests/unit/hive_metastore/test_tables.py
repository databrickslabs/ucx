import pytest
from databricks.sdk.errors import NotFound

from databricks.labs.ucx.hive_metastore.tables import (
    Table,
    TablesAnnotator,
    TablesCrawler,
)

from ..framework.mocks import MockBackend


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
            "CREATE TABLE IF NOT EXISTS new_catalog.db.managed_table DEEP CLONE catalog.db.managed_table;",
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
                object_type="EXTERNAL",
                table_format="DELTA",
                location="s3a://foo/bar",
            ),
            "SYNC TABLE new_catalog.db.external_table FROM catalog.db.external_table;",
        ),
    ],
)
def test_uc_sql(table, query):
    assert table.uc_create_sql("new_catalog") == query


def test_tables_crawler_inventory_table():
    tc = TablesCrawler(MockBackend(), "default")
    assert tc._table == "tables"


def test_tables_crawler_parse_tp():
    tc = TablesCrawler(MockBackend(), "default")
    tp1 = tc._parse_table_props(
        "[delta.minReaderVersion=1,delta.minWriterVersion=2,upgraded_to=fake_cat.fake_ext.fake_delta]"
    )
    tp2 = tc._parse_table_props("[delta.minReaderVersion=1,delta.minWriterVersion=2]")
    assert len(tp1) == 3
    assert tp1.get("upgraded_to") == "fake_cat.fake_ext.fake_delta"
    assert len(tp2) == 2
    assert tp2.get("upgraded_to") is None


def test_tables_returning_error_when_describing():
    errors = {"DESCRIBE TABLE EXTENDED hive_metastore.database.table1": "error"}
    rows = {
        "SHOW DATABASES": [("database",)],
        "SHOW TABLES FROM hive_metastore.database": [("", "table1", ""), ("", "table2", "")],
        "DESCRIBE TABLE EXTENDED hive_metastore.database.table2": [("Catalog", "catalog", ""), ("Type", "delta", "")],
    }
    backend = MockBackend(fails_on_first=errors, rows=rows)
    tc = TablesCrawler(backend, "default")
    results = tc._crawl()
    assert len(results) == 1


def test_skip_happy_path(mocker, caplog):
    ws = mocker.patch("databricks.sdk.WorkspaceClient.__init__")
    sbe = mocker.patch("databricks.labs.ucx.framework.crawlers.StatementExecutionBackend.__init__")
    annotate = TablesAnnotator(ws, sbe)
    annotate.skip_table(schema="schema", table="table")
    assert len(caplog.records) == 0


def test_skip_missing_schema(mocker, caplog):
    ws = mocker.patch("databricks.sdk.WorkspaceClient.__init__")
    sbe = mocker.patch("databricks.labs.ucx.framework.crawlers.StatementExecutionBackend.__init__")
    sbe.execute.side_effect = NotFound("[SCHEMA_NOT_FOUND]")
    annotate = TablesAnnotator(ws, sbe)
    annotate.skip_schema(schema="schema")
    assert [rec.message for rec in caplog.records if "schema not found" in rec.message.lower()]


def test_skip_missing_table(mocker, caplog):
    ws = mocker.patch("databricks.sdk.WorkspaceClient.__init__")
    sbe = mocker.patch("databricks.labs.ucx.framework.crawlers.StatementExecutionBackend.__init__")
    sbe.execute.side_effect = NotFound("[TABLE_OR_VIEW_NOT_FOUND]")
    annotate = TablesAnnotator(ws, sbe)
    annotate.skip_table(schema="schema", table="table")
    assert [rec.message for rec in caplog.records if "table not found" in rec.message.lower()]

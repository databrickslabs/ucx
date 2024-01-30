import pytest

from databricks.labs.ucx.hive_metastore.tables import Table, TablesCrawler, What

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
        Table(
            catalog="catalog", database="db", name="table", object_type="type", table_format="PARQUET"
        ).sql_migrate_dbfs("catalog")


@pytest.mark.parametrize(
    "table,target,query",
    [
        (
            Table(
                catalog="catalog",
                database="db",
                name="managed_table",
                object_type="MANAGED",
                table_format="DELTA",
                location="dbfs:/location/table",
            ),
            "new_catalog.db.managed_table",
            "CREATE TABLE IF NOT EXISTS new_catalog.db.managed_table DEEP CLONE catalog.db.managed_table;",
        ),
        (
            Table(
                catalog="catalog",
                database="db",
                name="managed_table",
                object_type="MANAGED",
                table_format="DELTA",
                location="dbfs:/mnt/location/table",
            ),
            "new_catalog.db.managed_table",
            "SYNC TABLE new_catalog.db.managed_table FROM catalog.db.managed_table;",
        ),
        (
            Table(
                catalog="catalog",
                database="db",
                name="view",
                object_type="VIEW",
                table_format="DELTA",
                view_text="SELECT * FROM table",
            ),
            "new_catalog.db.view",
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
            "new_catalog.db.external_table",
            "SYNC TABLE new_catalog.db.external_table FROM catalog.db.external_table;",
        ),
    ],
)
def test_uc_sql(table, target, query):
    if table.kind == "VIEW":
        assert table.sql_migrate_view(target) == query
    if table.kind == "TABLE" and table.is_dbfs_root:
        assert table.sql_migrate_dbfs(target) == query
    if table.kind == "TABLE" and not table.is_dbfs_root:
        assert table.sql_migrate_external(target) == query


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


def test_is_dbfs_root():
    table_a = Table("a", "b", "c", "MANAGED", "DELTA", location="dbfs:/somelocation/tablename")
    assert table_a.is_dbfs_root
    assert table_a.what == What.DBFS_ROOT_DELTA
    table_b = Table("a", "b", "c", "MANAGED", "PARQUET", location="dbfs:/somelocation/tablename")
    assert table_b.is_dbfs_root
    assert table_b.what == What.DBFS_ROOT_NON_DELTA
    table_c = Table("a", "b", "c", "MANAGED", "DELTA", location="/dbfs/somelocation/tablename")
    assert table_c.is_dbfs_root
    assert table_c.what == What.DBFS_ROOT_DELTA
    table_d = Table("a", "b", "c", "MANAGED", "DELTA", location="dbfs:/mnt/somelocation/tablename")
    assert not table_d.is_dbfs_root
    assert table_d.what == What.EXTERNAL
    table_e = Table("a", "b", "c", "MANAGED", "DELTA", location="/dbfs/mnt/somelocation/tablename")
    assert not table_e.is_dbfs_root
    assert table_e.what == What.EXTERNAL
    table_f = Table("a", "b", "c", "MANAGED", "DELTA", location="dbfs:/databricks-datasets/somelocation/tablename")
    assert not table_f.is_dbfs_root
    assert table_f.what == What.DB_DATASET
    table_g = Table("a", "b", "c", "MANAGED", "DELTA", location="/dbfs/databricks-datasets/somelocation/tablename")
    assert not table_g.is_dbfs_root
    assert table_g.what == What.DB_DATASET
    table_h = Table("a", "b", "c", "MANAGED", "DELTA", location="s3:/somelocation/tablename")
    assert not table_h.is_dbfs_root
    assert table_h.what == What.EXTERNAL
    table_i = Table("a", "b", "c", "MANAGED", "DELTA", location="adls:/somelocation/tablename")
    assert not table_i.is_dbfs_root
    assert table_i.what == What.EXTERNAL


def test_is_db_dataset():
    table_a = Table("a", "b", "c", "MANAGED", "DELTA", location="dbfs:/somelocation/tablename")
    assert not table_a.is_databricks_dataset
    assert not table_a.what == What.DB_DATASET
    table_b = Table("a", "b", "c", "MANAGED", "DELTA", location="/dbfs/somelocation/tablename")
    assert not table_b.is_databricks_dataset
    assert not table_b.what == What.DB_DATASET
    table_c = Table("a", "b", "c", "MANAGED", "DELTA", location="dbfs:/mnt/somelocation/tablename")
    assert not table_c.is_databricks_dataset
    assert not table_c.what == What.DB_DATASET
    table_d = Table("a", "b", "c", "MANAGED", "DELTA", location="/dbfs/mnt/somelocation/tablename")
    assert not table_d.is_databricks_dataset
    assert not table_d.what == What.DB_DATASET
    table_e = Table("a", "b", "c", "MANAGED", "DELTA", location="dbfs:/databricks-datasets/somelocation/tablename")
    assert table_e.is_databricks_dataset
    assert table_e.what == What.DB_DATASET
    table_f = Table("a", "b", "c", "MANAGED", "DELTA", location="/dbfs/databricks-datasets/somelocation/tablename")
    assert table_f.is_databricks_dataset
    assert table_f.what == What.DB_DATASET
    table_g = Table("a", "b", "c", "MANAGED", "DELTA", location="s3:/somelocation/tablename")
    assert not table_g.is_databricks_dataset
    assert not table_g.what == What.DB_DATASET
    table_h = Table("a", "b", "c", "MANAGED", "DELTA", location="adls:/somelocation/tablename")
    assert not table_h.is_databricks_dataset
    assert not table_h.what == What.DB_DATASET


def test_is_supported_for_sync():
    assert Table(
        "a", "b", "c", "EXTERNAL", "DELTA", location="dbfs:/somelocation/tablename"
    ).is_format_supported_for_sync
    assert Table("a", "b", "c", "EXTERNAL", "CSV", location="dbfs:/somelocation/tablename").is_format_supported_for_sync
    assert Table(
        "a", "b", "c", "EXTERNAL", "TEXT", location="dbfs:/somelocation/tablename"
    ).is_format_supported_for_sync
    assert Table("a", "b", "c", "EXTERNAL", "ORC", location="dbfs:/somelocation/tablename").is_format_supported_for_sync
    assert Table(
        "a", "b", "c", "EXTERNAL", "JSON", location="dbfs:/somelocation/tablename"
    ).is_format_supported_for_sync
    assert not (
        Table("a", "b", "c", "EXTERNAL", "AVRO", location="dbfs:/somelocation/tablename").is_format_supported_for_sync
    )


def test_table_what():
    assert Table("a", "b", "c", "EXTERNAL", "DELTA", location="s3://external_location/table").what == What.EXTERNAL
    assert (
        Table("a", "b", "c", "EXTERNAL", "UNSUPPORTED_FORMAT", location="s3://external_location/table").what
        == What.UNKNOWN
    )
    assert (
        Table("a", "b", "c", "MANAGED", "DELTA", location="dbfs:/somelocation/tablename").what == What.DBFS_ROOT_DELTA
    )
    assert (
        Table("a", "b", "c", "MANAGED", "PARQUET", location="dbfs:/somelocation/tablename").what
        == What.DBFS_ROOT_NON_DELTA
    )
    assert Table("a", "b", "c", "VIEW", "VIEW", view_text="select * from some_table").what == What.VIEW
    assert (
        Table("a", "b", "c", "MANAGED", "DELTA", location="dbfs:/databricks-datasets/somelocation/tablename").what
        == What.DB_DATASET
    )

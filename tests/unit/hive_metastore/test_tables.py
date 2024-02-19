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


def test_tables_returning_error_when_describing():
    errors = {"DESCRIBE TABLE EXTENDED hive_metastore.database.table1": "error"}
    rows = {
        "SHOW DATABASES": [("database",)],
        "SHOW TABLES FROM hive_metastore.database": [("", "table1", ""), ("", "table2", "")],
        "DESCRIBE TABLE EXTENDED hive_metastore.database.table2": [
            ("Catalog", "catalog", ""),
            ("Type", "delta", ""),
            ("Table Properties",
             "[delta.minReaderVersion=1,delta.minWriterVersion=2,upgraded_to=fake_cat.fake_ext.fake_delta]",
             ""),
        ],
    }
    backend = MockBackend(fails_on_first=errors, rows=rows)
    tc = TablesCrawler(backend, "default")
    results = tc.snapshot()
    assert len(results) == 1
    first = results[0]
    assert first.upgraded_to == 'fake_cat.fake_ext.fake_delta'


@pytest.mark.parametrize(
    'table,dbfs_root,what',
    [
        (Table("a", "b", "c", "MANAGED", "DELTA", location="dbfs:/somelocation/tablename"), True, What.DBFS_ROOT_DELTA),
        (
            Table("a", "b", "c", "MANAGED", "PARQUET", location="dbfs:/somelocation/tablename"),
            True,
            What.DBFS_ROOT_NON_DELTA,
        ),
        (Table("a", "b", "c", "MANAGED", "DELTA", location="/dbfs/somelocation/tablename"), True, What.DBFS_ROOT_DELTA),
        (
            Table("a", "b", "c", "MANAGED", "DELTA", location="dbfs:/mnt/somelocation/tablename"),
            False,
            What.EXTERNAL_SYNC,
        ),
        (
            Table("a", "b", "c", "MANAGED", "DELTA", location="/dbfs/mnt/somelocation/tablename"),
            False,
            What.EXTERNAL_SYNC,
        ),
        (
            Table("a", "b", "c", "MANAGED", "DELTA", location="dbfs:/databricks-datasets/somelocation/tablename"),
            False,
            What.DB_DATASET,
        ),
        (
            Table("a", "b", "c", "MANAGED", "DELTA", location="/dbfs/databricks-datasets/somelocation/tablename"),
            False,
            What.DB_DATASET,
        ),
        (Table("a", "b", "c", "MANAGED", "DELTA", location="s3:/somelocation/tablename"), False, What.EXTERNAL_SYNC),
        (Table("a", "b", "c", "MANAGED", "DELTA", location="adls:/somelocation/tablename"), False, What.EXTERNAL_SYNC),
    ],
)
def test_is_dbfs_root(table, dbfs_root, what):
    assert table.is_dbfs_root == dbfs_root
    assert table.what == what


@pytest.mark.parametrize(
    'table,db_dataset',
    [
        (Table("a", "b", "c", "MANAGED", "DELTA", location="dbfs:/somelocation/tablename"), False),
        (Table("a", "b", "c", "MANAGED", "DELTA", location="/dbfs/somelocation/tablename"), False),
        (Table("a", "b", "c", "MANAGED", "DELTA", location="dbfs:/mnt/somelocation/tablename"), False),
        (Table("a", "b", "c", "MANAGED", "DELTA", location="/dbfs/mnt/somelocation/tablename"), False),
        (Table("a", "b", "c", "MANAGED", "DELTA", location="dbfs:/databricks-datasets/somelocation/tablename"), True),
        (Table("a", "b", "c", "MANAGED", "DELTA", location="/dbfs/databricks-datasets/somelocation/tablename"), True),
        (Table("a", "b", "c", "MANAGED", "DELTA", location="s3:/somelocation/tablename"), False),
        (Table("a", "b", "c", "MANAGED", "DELTA", location="adls:/somelocation/tablename"), False),
    ],
)
def test_is_db_dataset(table, db_dataset):
    assert table.is_databricks_dataset == db_dataset
    assert (table.what == What.DB_DATASET) == db_dataset


@pytest.mark.parametrize(
    'table,supported',
    [
        (Table("a", "b", "c", "EXTERNAL", "DELTA", location="dbfs:/somelocation/tablename"), True),
        (Table("a", "b", "c", "EXTERNAL", "CSV", location="dbfs:/somelocation/tablename"), True),
        (Table("a", "b", "c", "EXTERNAL", "TEXT", location="dbfs:/somelocation/tablename"), True),
        (Table("a", "b", "c", "EXTERNAL", "ORC", location="dbfs:/somelocation/tablename"), True),
        (Table("a", "b", "c", "EXTERNAL", "JSON", location="dbfs:/somelocation/tablename"), True),
        (Table("a", "b", "c", "EXTERNAL", "AVRO", location="dbfs:/somelocation/tablename"), False),
    ],
)
def test_is_supported_for_sync(table, supported):
    assert table.is_format_supported_for_sync == supported


@pytest.mark.parametrize(
    'table,what',
    [
        (Table("a", "b", "c", "EXTERNAL", "DELTA", location="s3://external_location/table"), What.EXTERNAL_SYNC),
        (
            Table("a", "b", "c", "EXTERNAL", "UNSUPPORTED_FORMAT", location="s3://external_location/table"),
            What.EXTERNAL_NO_SYNC,
        ),
        (Table("a", "b", "c", "MANAGED", "DELTA", location="dbfs:/somelocation/tablename"), What.DBFS_ROOT_DELTA),
        (Table("a", "b", "c", "MANAGED", "PARQUET", location="dbfs:/somelocation/tablename"), What.DBFS_ROOT_NON_DELTA),
        (Table("a", "b", "c", "VIEW", "VIEW", view_text="select * from some_table"), What.VIEW),
        (
            Table("a", "b", "c", "MANAGED", "DELTA", location="dbfs:/databricks-datasets/somelocation/tablename"),
            What.DB_DATASET,
        ),
    ],
)
def test_table_what(table, what):
    assert table.what == what

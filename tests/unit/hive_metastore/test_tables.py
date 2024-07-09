import pytest
from databricks.labs.lsql.backends import MockBackend

from databricks.labs.ucx.hive_metastore.locations import Mount, ExternalLocations
from databricks.labs.ucx.hive_metastore.tables import Table, TablesCrawler, What, HiveSerdeType


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
            (
                "Table Properties",
                "[delta.minReaderVersion=1,delta.minWriterVersion=2,upgraded_to=fake_cat.fake_ext.fake_delta]",
                "",
            ),
        ],
    }
    backend = MockBackend(fails_on_first=errors, rows=rows)
    tables_crawler = TablesCrawler(backend, "default")
    results = tables_crawler.snapshot()
    assert len(results) == 1
    first = results[0]
    assert first.upgraded_to == 'fake_cat.fake_ext.fake_delta'


def test_tables_returning_error_when_show_tables(caplog):
    errors = {"SHOW TABLES FROM hive_metastore.database": "SCHEMA_NOT_FOUND"}
    rows = {"SHOW DATABASES": [("database",)]}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    tables_crawler = TablesCrawler(backend, "default")
    results = tables_crawler.snapshot()
    assert len(results) == 0
    assert "Schema hive_metastore.database no longer exists" in caplog.text


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
        (Table("a", "b", "c", "EXTERNAL", "AVRO", location="dbfs:/somelocation/tablename"), True),
        (Table("a", "b", "c", "EXTERNAL", "BINARYFILE", location="dbfs:/somelocation/tablename"), False),
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


def test_tables_crawler_should_filter_by_database():
    rows = {
        "SHOW TABLES FROM hive_metastore.database": [("", "table1", ""), ("", "table2", "")],
        "SHOW TABLES FROM hive_metastore.database_2": [("", "table1", "")],
    }
    backend = MockBackend(rows=rows)
    tables_crawler = TablesCrawler(backend, "default", ["database"])
    results = tables_crawler.snapshot()
    assert len(results) == 2
    assert sorted(backend.queries) == sorted(
        [
            'SELECT * FROM hive_metastore.default.tables',
            'SHOW TABLES FROM hive_metastore.database',
            'DESCRIBE TABLE EXTENDED hive_metastore.database.table1',
            'DESCRIBE TABLE EXTENDED hive_metastore.database.table2',
        ]
    )


def test_is_partitioned_flag():
    rows = {
        "SHOW DATABASES": [("database",)],
        "SHOW TABLES FROM hive_metastore.database": [("", "table1", ""), ("", "table2", "")],
        'DESCRIBE TABLE EXTENDED hive_metastore.database.table1': [
            ("column1", "string", "null"),
            ("column2", "string", "null"),
            ("# Partition Information", "", ""),
            ("# col_name", "data_type", "comment"),
            ("column1", "string", "null"),
            ("Provider", "delta", ""),
            ("Type", "table", ""),
        ],
        'DESCRIBE TABLE EXTENDED hive_metastore.database.table2': [
            ("column1", "string", "null"),
            ("column2", "string", "null"),
            ("Provider", "delta", ""),
            ("Type", "table", ""),
        ],
    }
    backend = MockBackend(rows=rows)
    tables_crawler = TablesCrawler(
        backend,
        "default",
    )
    results = tables_crawler.snapshot()
    assert len(results) == 2
    assert (
        Table(
            catalog='hive_metastore',
            database='database',
            name='table2',
            object_type='TABLE',
            table_format='DELTA',
            location=None,
            view_text=None,
            upgraded_to=None,
            storage_properties={},
            is_partitioned=False,
        )
        in results
    )
    assert (
        Table(
            catalog='hive_metastore',
            database='database',
            name='table1',
            object_type='TABLE',
            table_format='DELTA',
            location=None,
            view_text=None,
            upgraded_to=None,
            storage_properties={},
            is_partitioned=True,
        )
        in results
    )


@pytest.mark.parametrize(
    'table, mounts, describe, ddl, expected_hiveserde_type, expected_new_ddl',
    [
        # valid parquet hiveserde test
        (
            Table("hive_metastore", "schema", "table", "EXTERNAL", "HIVE", location="dbfs:/mnt/test_parquet/table1"),
            [
                Mount("/mnt/test_parquet", "s3://databricks/test_parquet"),
                Mount("/mnt/test_orc", "s3://databricks/test_orc"),
            ],
            MockBackend.rows("col_name", "data_type", "comment")[
                ("Serde Library", "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe", None),
                ("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat", None),
                ("OutputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat", None),
            ],
            MockBackend.rows("createtab_stmt")[
                (
                    "CREATE TABLE hive_metastore.schema.table (id INT, name STRING, age INT) USING PARQUET PARTITIONED BY (age) LOCATION 'dbfs:/mnt/test_parquet/table1' TBLPROPERTIES ('transient_lastDdlTime'='1712729041')"
                ),
            ],
            HiveSerdeType.PARQUET,
            "CREATE TABLE uc_catalog.uc_schema.table (id INT, name STRING, age INT) USING PARQUET PARTITIONED BY (age) LOCATION 's3://databricks/test_parquet/table1' TBLPROPERTIES ('transient_lastDdlTime'='1712729041')",
        ),
        # valid avro hiveserde test
        (
            Table("hive_metastore", "schema", "table", "EXTERNAL", "HIVE", location="s3://databricks/test_avro"),
            [
                Mount("/mnt/test_parquet", "s3://databricks/test_parquet"),
                Mount("/mnt/test_orc", "s3://databricks/test_orc"),
            ],
            MockBackend.rows("col_name", "data_type", "comment")[
                ("Serde Library", "org.apache.hadoop.hive.serde2.avro.AvroSerDe", None),
                ("InputFormat", "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat", None),
                ("OutputFormat", "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat", None),
            ],
            MockBackend.rows("createtab_stmt")[
                (
                    'CREATE TABLE hive_metastore.schema.table (id INT, name STRING, age INT) USING AVRO LOCATION \'s3://databricks/test_avro\' TBLPROPERTIES (\'avro.schema.literal\'=\'{"namespace": "org.apache.hive", "name": "first_schema", "type": "record", "fields": [{"name":"id", "type":"int"}, {"name":"name", "type":"string"}, {"name":"age", "type":"int"}]}\', \'transient_lastDdlTime\'=\'1712728956\')'
                ),
            ],
            HiveSerdeType.AVRO,
            'CREATE TABLE uc_catalog.uc_schema.table (id INT, name STRING, age INT) USING AVRO LOCATION \'s3://databricks/test_avro\' TBLPROPERTIES (\'avro.schema.literal\'=\'{"namespace": "org.apache.hive", "name": "first_schema", "type": "record", "fields": [{"name":"id", "type":"int"}, {"name":"name", "type":"string"}, {"name":"age", "type":"int"}]}\', \'transient_lastDdlTime\'=\'1712728956\')',
        ),
        # valid orc hiveserde test
        (
            Table("hive_metastore", "schema", "table", "EXTERNAL", "HIVE", location="/dbfs/mnt/test_orc/table1"),
            [
                Mount("/mnt/test_parquet", "s3://databricks/test_parquet"),
                Mount("/mnt/test_orc", "s3://databricks/test_orc"),
            ],
            MockBackend.rows("col_name", "data_type", "comment")[
                ("Serde Library", "org.apache.hadoop.hive.ql.io.orc.OrcSerde", None),
                ("InputFormat", "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat", None),
                ("OutputFormat", "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat", None),
            ],
            MockBackend.rows("createtab_stmt")[
                (
                    "CREATE TABLE hive_metastore.schema.table (id INT, name STRING, age INT) USING ORC PARTITIONED BY (age) LOCATION '/dbfs/mnt/test_orc/table1' TBLPROPERTIES ('transient_lastDdlTime'='1712729616')"
                ),
            ],
            HiveSerdeType.ORC,
            "CREATE TABLE uc_catalog.uc_schema.table (id INT, name STRING, age INT) USING ORC PARTITIONED BY (age) LOCATION 's3://databricks/test_orc/table1' TBLPROPERTIES ('transient_lastDdlTime'='1712729616')",
        ),
        # un-supported hiveserde test, and no table location test
        (
            Table("hive_metastore", "schema", "table", "EXTERNAL", "HIVE"),
            [
                Mount("test", "test"),
            ],
            MockBackend.rows("col_name", "data_type", "comment")[
                ("Serde Library", "LazyBinaryColumnarSerDe", None),
                ("InputFormat", "RCFileInputFormat", None),
                ("OutputFormat", "RCFileOutputFormat", None),
            ],
            None,
            HiveSerdeType.OTHER_HIVESERDE,
            None,
        ),
        # invalid hiveserde info test
        (
            Table("hive_metastore", "schema", "table", "EXTERNAL", "HIVE", location="dummy"),
            None,
            MockBackend.rows("col_name", "data_type", "comment")[("dummy", "dummy", None),],
            None,
            HiveSerdeType.INVALID_HIVESERDE_INFO,
            None,
        ),
        # not hiveserde table test
        (
            Table("hive_metastore", "schema", "table", "EXTERNAL", "DELTA", location="dummy"),
            None,
            MockBackend.rows("col_name", "data_type", "comment")[("dummy", "dummy", None),],
            None,
            HiveSerdeType.NOT_HIVESERDE,
            None,
        ),
    ],
)
def test_in_place_migrate_hiveserde_sql(table, mounts, describe, ddl, expected_hiveserde_type, expected_new_ddl):
    sql_backend = MockBackend(
        rows={
            "DESCRIBE TABLE EXTENDED *": describe,
            "SHOW CREATE TABLE *": ddl,
        }
    )
    dst_table_location = None
    if mounts and table.is_dbfs_mnt:
        dst_table_location = ExternalLocations.resolve_mount(table.location, mounts)

    hiveserde_type = table.hiveserde_type(sql_backend)
    assert hiveserde_type == expected_hiveserde_type

    migrate_sql = table.sql_migrate_external_hiveserde_in_place(
        "uc_catalog", "uc_schema", "table", sql_backend, hiveserde_type, dst_table_location
    )
    assert migrate_sql == expected_new_ddl


@pytest.mark.parametrize(
    'ddl, expected_log',
    [
        # sqlglot raises sqlglot.errors.ParseError
        (
            MockBackend.rows("createtab_stmt")[("!@#"),],
            "Exception when parsing 'SHOW CREATE TABLE' DDL for hive_metastore.schema.test_parquet",
        ),
        # sqlglot parse no statement
        (
            MockBackend.rows("createtab_stmt")[(""),],
            "sqlglot parsed none statement from 'SHOW CREATE TABLE' DDL for hive_metastore.schema.test_parquet",
        ),
        # sqlglot parse no table
        (
            MockBackend.rows("createtab_stmt")[("invalid statement"),],
            "sqlglot failed to extract table object from parsed DDL for hive_metastore.schema.test_parquet",
        ),
        # sqlglot parse no location
        (
            MockBackend.rows("createtab_stmt")[("create table test (id int, name string) using parquet"),],
            "sqlglot failed to extract table location object from parsed DDL for hive_metastore.schema.test_parquet",
        ),
    ],
)
def test_in_place_migrate_hiveserde_sql_parsing_failure(caplog, ddl, expected_log):
    table = Table(
        "hive_metastore", "schema", "test_parquet", "EXTERNAL", "HIVE", location="dbfs:/mnt/test_parquet/table1"
    )
    sql_backend = MockBackend(
        rows={
            "DESCRIBE TABLE EXTENDED *": MockBackend.rows("col_name", "data_type", "comment")[
                ("Serde Library", "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe", None),
                ("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat", None),
                ("OutputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat", None),
            ],
            "SHOW CREATE TABLE *": ddl,
        }
    )

    migrate_sql = table.sql_migrate_external_hiveserde_in_place(
        "uc_catalog",
        "uc_schema",
        "test_parquet",
        sql_backend,
        HiveSerdeType.PARQUET,
        replace_table_location="test_location",
    )

    assert migrate_sql is None
    assert expected_log in caplog.text

import datetime
import logging
from itertools import cycle
from unittest.mock import create_autospec
import pytest
from databricks.labs.lsql.backends import MockBackend, SqlBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import CatalogInfo, SchemaInfo, TableInfo

from databricks.labs.ucx.hive_metastore import Mounts
from databricks.labs.ucx.hive_metastore.grants import MigrateGrants
from databricks.labs.ucx.hive_metastore.locations import Mount
from databricks.labs.ucx.hive_metastore.mapping import (
    Rule,
    TableMapping,
    TableToMigrate,
)
from databricks.labs.ucx.hive_metastore.table_migrate import (
    TablesMigrator,
)
from databricks.labs.ucx.hive_metastore.migration_status import (
    MigrationStatusRefresher,
    MigrationIndex,
    MigrationStatus,
    TableView,
)
from databricks.labs.ucx.hive_metastore.tables import (
    Table,
    TablesCrawler,
    What,
)
from databricks.labs.ucx.hive_metastore.view_migrate import ViewToMigrate

from .. import mock_table_mapping, mock_workspace_client


logger = logging.getLogger(__name__)


@pytest.fixture
def ws():
    client = create_autospec(WorkspaceClient)
    client.get_workspace_id.return_value = "12345"
    return client


def test_migrate_dbfs_root_tables_should_produce_proper_queries(ws):
    errors = {}
    rows = {r"SYNC .*": MockBackend.rows("status_code", "description")[("SUCCESS", "test")]}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = TablesCrawler(backend, "inventory_database")
    table_mapping = mock_table_mapping(["managed_dbfs", "managed_mnt", "managed_other"])
    migration_status_refresher = MigrationStatusRefresher(ws, backend, "inventory_database", table_crawler)
    migrate_grants = create_autospec(MigrateGrants)
    table_migrate = TablesMigrator(
        table_crawler,
        ws,
        backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
    )
    table_migrate.migrate_tables(what=What.DBFS_ROOT_DELTA)
    table_migrate.migrate_tables(what=What.EXTERNAL_SYNC)

    assert (
        "CREATE TABLE IF NOT EXISTS `ucx_default`.`db1_dst`.`managed_dbfs` DEEP CLONE `hive_metastore`.`db1_src`.`managed_dbfs`;"
        in backend.queries
    )
    assert "SYNC TABLE `ucx_default`.`db1_dst`.`managed_mnt` FROM `hive_metastore`.`db1_src`.`managed_mnt`;" in backend.queries
    assert (
        "ALTER TABLE `hive_metastore`.`db1_src`.`managed_dbfs` "
        "SET TBLPROPERTIES ('upgraded_to' = 'ucx_default.db1_dst.managed_dbfs');"
    ) in backend.queries
    assert (
        f"ALTER TABLE `ucx_default`.`db1_dst`.`managed_dbfs` "
        f"SET TBLPROPERTIES ('upgraded_from' = 'hive_metastore.db1_src.managed_dbfs' , "
        f"'{Table.UPGRADED_FROM_WS_PARAM}' = '12345');"
    ) in backend.queries
    assert "SYNC TABLE `ucx_default`.`db1_dst`.`managed_other` FROM `hive_metastore`.`db1_src`.`managed_other`;" in backend.queries
    migrate_grants.apply.assert_called()


def test_dbfs_non_delta_tables_should_produce_proper_queries(ws):
    errors = {}
    rows = {
        "SHOW CREATE TABLE": [
            {
                "createtab_stmt": "CREATE EXTERNAL TABLE `hive_metastore`.`db1_src`.`managed_dbfs` "
                "(foo STRING,bar STRING) USING PARQUET "
                "LOCATION 's3://some_location/table'"
            }
        ]
    }
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = TablesCrawler(backend, "inventory_database")
    table_mapping = mock_table_mapping(["dbfs_parquet"])
    migration_status_refresher = MigrationStatusRefresher(ws, backend, "inventory_database", table_crawler)
    migrate_grants = create_autospec(MigrateGrants)
    table_migrate = TablesMigrator(
        table_crawler,
        ws,
        backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
    )
    table_migrate.migrate_tables(what=What.DBFS_ROOT_NON_DELTA)

    migrate_grants.apply.assert_called()

    assert (
        "CREATE TABLE IF NOT EXISTS `ucx_default`.`db1_dst`.`managed_dbfs` "
        "AS SELECT * FROM `hive_metastore`.`db1_src`.`managed_dbfs`" in backend.queries
    )
    assert (
        "ALTER TABLE `hive_metastore`.`db1_src`.`managed_dbfs` "
        "SET TBLPROPERTIES ('upgraded_to' = 'ucx_default.db1_dst.managed_dbfs');"
    ) in backend.queries
    assert (
        f"ALTER TABLE `ucx_default`.`db1_dst`.`managed_dbfs` "
        f"SET TBLPROPERTIES ('upgraded_from' = 'hive_metastore.db1_src.managed_dbfs' , "
        f"'{Table.UPGRADED_FROM_WS_PARAM}' = '12345');"
    ) in backend.queries


def test_migrate_dbfs_root_tables_should_be_skipped_when_upgrading_external(ws):
    errors = {}
    rows = {}
    crawler_backend = MockBackend(fails_on_first=errors, rows=rows)
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = TablesCrawler(crawler_backend, "inventory_database")
    table_mapping = mock_table_mapping(["managed_dbfs"])
    migration_status_refresher = MigrationStatusRefresher(ws, backend, "inventory_database", table_crawler)
    migrate_grants = create_autospec(MigrateGrants)
    table_migrate = TablesMigrator(
        table_crawler,
        ws,
        backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
    )
    table_migrate.migrate_tables(what=What.EXTERNAL_SYNC)

    migrate_grants.apply.assert_not_called()


def test_migrate_external_tables_should_produce_proper_queries(ws):
    errors = {}
    rows = {r"SYNC .*": MockBackend.rows("status_code", "description")[("SUCCESS", "test")]}
    crawler_backend = MockBackend(fails_on_first=errors, rows=rows)
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = TablesCrawler(crawler_backend, "inventory_database")
    table_mapping = mock_table_mapping(["external_src"])
    migration_status_refresher = MigrationStatusRefresher(ws, backend, "inventory_database", table_crawler)
    migrate_grants = create_autospec(MigrateGrants)
    table_migrate = TablesMigrator(
        table_crawler,
        ws,
        backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
    )
    table_migrate.migrate_tables(what=What.EXTERNAL_SYNC)

    migrate_grants.apply.assert_called()

    assert backend.queries == [
        "SYNC TABLE `ucx_default`.`db1_dst`.`external_dst` FROM `hive_metastore`.`db1_src`.`external_src`;",
        (
            f"ALTER TABLE `ucx_default`.`db1_dst`.`external_dst` "
            f"SET TBLPROPERTIES ('upgraded_from' = 'hive_metastore.db1_src.external_src' , "
            f"'{Table.UPGRADED_FROM_WS_PARAM}' = '12345');"
        ),
    ]


def test_migrate_external_table_failed_sync(ws, caplog):
    errors = {}
    rows = {r"SYNC .*": MockBackend.rows("status_code", "description")[("LOCATION_OVERLAP", "test")]}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    crawler_backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = TablesCrawler(crawler_backend, "inventory_database")
    table_mapping = mock_table_mapping(["external_src"])
    migration_status_refresher = MigrationStatusRefresher(ws, backend, "inventory_database", table_crawler)
    migrate_grants = create_autospec(MigrateGrants)
    table_migrate = TablesMigrator(
        table_crawler,
        ws,
        backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
    )
    table_migrate.migrate_tables(what=What.EXTERNAL_SYNC)
    assert "SYNC command failed to migrate" in caplog.text
    migrate_grants.apply.assert_not_called()


@pytest.mark.parametrize(
    'hiveserde_in_place_migrate, describe, ddl, errors, migrated, expected_value',
    [
        # test migrate parquet hiveserde table in place
        (
            True,
            MockBackend.rows("col_name", "data_type", "comment")[
                ("Serde Library", "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe", None),
                ("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat", None),
                ("OutputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat", None),
            ],
            MockBackend.rows("createtab_stmt")[
                (
                    "CREATE TABLE hive_metastore.schema.test_parquet (id INT) USING PARQUET LOCATION 'dbfs:/mnt/test/table1'"
                ),
            ],
            {},
            True,
            "CREATE TABLE ucx_default.db1_dst.external_dst (id INT) USING PARQUET LOCATION 's3://test/folder/table1'",
        ),
        # test unsupported hiveserde type
        (
            True,
            MockBackend.rows("col_name", "data_type", "comment")[("dummy", "dummy", None)],
            MockBackend.rows("createtab_stmt")[("dummy"),],
            {},
            False,
            "hive_metastore.db1_src.external_src table can only be migrated using CTAS.",
        ),
        # test None table_migrate_sql returned
        (
            True,
            MockBackend.rows("col_name", "data_type", "comment")[
                ("Serde Library", "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe", None),
                ("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat", None),
                ("OutputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat", None),
            ],
            MockBackend.rows("createtab_stmt")[("dummy"),],
            {},
            False,
            "Failed to generate in-place migration DDL for hive_metastore.db1_src.external_src, skip the in-place migration. It can be migrated in CTAS workflow",
        ),
        # test not in place migration
        (
            False,
            None,
            None,
            {},
            True,
            "CREATE TABLE IF NOT EXISTS ucx_default.db1_dst.external_dst LOCATION 's3://test/folder/table1_ctas_migrated' AS SELECT * FROM hive_metastore.db1_src.external_src",
        ),
        # test failed migration
        (
            True,
            MockBackend.rows("col_name", "data_type", "comment")[
                ("Serde Library", "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe", None),
                ("InputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat", None),
                ("OutputFormat", "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat", None),
            ],
            MockBackend.rows("createtab_stmt")[
                (
                    "CREATE TABLE hive_metastore.schema.test_parquet (id INT) USING PARQUET LOCATION 'dbfs:/mnt/test/table1'"
                ),
            ],
            {"CREATE TABLE ucx": "error"},
            False,
            "Failed to migrate table hive_metastore.db1_src.external_src to ucx_default.db1_dst.external_dst: error",
        ),
    ],
)
def test_migrate_external_hiveserde_table_in_place(
    ws,
    caplog,
    hiveserde_in_place_migrate,
    describe,
    ddl,
    errors,
    migrated,
    expected_value,
):
    caplog.set_level(logging.INFO)
    backend = MockBackend(
        rows={
            "DESCRIBE TABLE EXTENDED *": describe,
            "SHOW CREATE TABLE *": ddl,
        },
        fails_on_first=errors,
    )
    table_crawler = TablesCrawler(backend, "inventory_database")
    table_mapping = mock_table_mapping(["external_hiveserde"])
    migration_status_refresher = MigrationStatusRefresher(ws, backend, "inventory_database", table_crawler)
    mount_crawler = create_autospec(Mounts)
    mount_crawler.snapshot.return_value = [Mount('/mnt/test', 's3://test/folder')]
    migrate_grants = create_autospec(MigrateGrants)

    table_migrate = TablesMigrator(
        table_crawler,
        ws,
        backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
    )

    table_migrate.migrate_tables(
        what=What.EXTERNAL_HIVESERDE,
        mounts_crawler=mount_crawler,
        hiveserde_in_place_migrate=hiveserde_in_place_migrate,
    )

    if migrated:
        assert expected_value in backend.queries
        migrate_grants.apply.assert_called()
        return
    migrate_grants.apply.assert_not_called()
    assert expected_value in caplog.text


@pytest.mark.parametrize(
    'what, test_table, expected_query',
    [
        (
            What.EXTERNAL_HIVESERDE,
            "external_hiveserde",
            "CREATE TABLE IF NOT EXISTS `ucx_default`.`db1_dst`.`external_dst` LOCATION 's3://test/folder/table1_ctas_migrated' AS SELECT * FROM `hive_metastore`.`db1_src`.`external_src`",
        ),
        (
            What.EXTERNAL_NO_SYNC,
            "external_no_sync",
            "CREATE TABLE IF NOT EXISTS `ucx_default`.`db1_dst`.`external_dst` LOCATION 's3:/bucket/test/table1_ctas_migrated' AS SELECT * FROM `hive_metastore`.`db1_src`.`external_src`",
        ),
        (
            What.EXTERNAL_NO_SYNC,
            "external_no_sync_missing_location",
            "CREATE TABLE IF NOT EXISTS `ucx_default`.`db1_dst`.`external_dst` AS SELECT * FROM `hive_metastore`.`db1_src`.`external_src`",
        ),
    ],
)
def test_migrate_external_tables_ctas_should_produce_proper_queries(ws, what, test_table, expected_query):
    backend = MockBackend()
    table_crawler = TablesCrawler(backend, "inventory_database")
    table_mapping = mock_table_mapping([test_table])
    migration_status_refresher = MigrationStatusRefresher(ws, backend, "inventory_database", table_crawler)
    mounts_crawler = create_autospec(Mounts)
    mounts_crawler.snapshot.return_value = [Mount('/mnt/test', 's3://test/folder')]
    migrate_grants = create_autospec(MigrateGrants)
    table_migrate = TablesMigrator(
        table_crawler,
        ws,
        backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
    )
    table_migrate.migrate_tables(what=what, mounts_crawler=mounts_crawler)

    assert expected_query in backend.queries
    migrate_grants.apply.assert_called()


def test_migrate_already_upgraded_table_should_produce_no_queries(ws):
    errors = {}
    rows = {}
    crawler_backend = MockBackend(fails_on_first=errors, rows=rows)
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = TablesCrawler(crawler_backend, "inventory_database")
    ws.catalogs.list.return_value = [CatalogInfo(name="cat1")]
    ws.schemas.list.return_value = [
        SchemaInfo(catalog_name="cat1", name="test_schema1"),
    ]
    ws.tables.list.return_value = [
        TableInfo(
            catalog_name="cat1",
            schema_name="schema1",
            name="dest1",
            full_name="cat1.schema1.dest1",
            properties={"upgraded_from": "hive_metastore.db1_src.external_src"},
        ),
    ]

    table_mapping = create_autospec(TableMapping)
    table_mapping.get_tables_to_migrate.return_value = [
        TableToMigrate(
            Table("hive_metastore", "db1_src", "external_src", "EXTERNAL", "DELTA"),
            Rule("workspace", "cat1", "db1_src", "schema1", "external_src", "dest1"),
        )
    ]
    migration_status_refresher = MigrationStatusRefresher(ws, backend, "inventory_database", table_crawler)
    migrate_grants = create_autospec(MigrateGrants)
    table_migrate = TablesMigrator(
        table_crawler,
        ws,
        backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
    )
    table_migrate.migrate_tables(what=What.EXTERNAL_SYNC)

    assert not backend.queries
    migrate_grants.apply.assert_not_called()


def test_migrate_unsupported_format_table_should_produce_no_queries(ws):
    errors = {}
    rows = {}
    crawler_backend = MockBackend(fails_on_first=errors, rows=rows)
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = TablesCrawler(crawler_backend, "inventory_database")
    table_mapping = mock_table_mapping(["external_src_unsupported"])
    migration_status_refresher = MigrationStatusRefresher(ws, backend, "inventory_database", table_crawler)
    migrate_grants = create_autospec(MigrateGrants)
    table_migrate = TablesMigrator(
        table_crawler,
        ws,
        backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
    )
    table_migrate.migrate_tables(what=What.UNKNOWN)

    assert len(backend.queries) == 0
    migrate_grants.apply.assert_not_called()


def test_migrate_view_should_produce_proper_queries(ws):
    errors = {}
    original_view = "CREATE OR REPLACE VIEW `hive_metastore`.`db1_src`.`view_src` AS SELECT * FROM `db1_src`.`managed_dbfs`"
    rows = {"SHOW CREATE TABLE": [{"createtab_stmt": original_view}]}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = TablesCrawler(backend, "inventory_database")
    table_mapping = mock_table_mapping(["managed_dbfs", "view"])
    migration_status_refresher = create_autospec(MigrationStatusRefresher)
    migration_status_refresher.get_seen_tables.return_value = {
        "ucx_default.db1_dst.managed_dbfs": "hive_metastore.db1_src.managed_dbfs"
    }
    migration_index = MigrationIndex(
        [
            MigrationStatus("db1_src", "managed_dbfs", "ucx_default", "db1_dst", "new_managed_dbfs"),
            MigrationStatus("db1_src", "view_src", "ucx_default", "db1_dst", "view_dst"),
        ]
    )
    migration_status_refresher.index.return_value = migration_index
    migrate_grants = create_autospec(MigrateGrants)
    table_migrate = TablesMigrator(
        table_crawler,
        ws,
        backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
    )
    table_migrate.migrate_tables(what=What.VIEW)

    create = "CREATE OR REPLACE VIEW IF NOT EXISTS `ucx_default`.`db1_dst`.`view_dst` AS SELECT * FROM `ucx_default`.`db1_dst`.`new_managed_dbfs`"
    assert create in backend.queries
    src = (
        "ALTER VIEW `hive_metastore`.`db1_src`.`view_src` SET TBLPROPERTIES ('upgraded_to' = 'ucx_default.db1_dst.view_dst');"
    )
    assert src in backend.queries
    dst = f"ALTER VIEW `ucx_default`.`db1_dst`.`view_dst` SET TBLPROPERTIES ('upgraded_from' = 'hive_metastore.db1_src.view_src' , '{Table.UPGRADED_FROM_WS_PARAM}' = '12345');"
    assert dst in backend.queries
    migrate_grants.apply.assert_called()


def test_migrate_view_with_local_dataset_should_be_skipped(ws):
    ddl = "CREATE VIEW v AS WITH t(a) AS (SELECT 1) SELECT * FROM t;"
    to_migrate = ViewToMigrate(
        Table("hive_metastore", "database", "", "EXTERNAL", "VIEW", None, ddl),
        Rule("workspace", "catalog", "", "", "", ""),
    )
    dependencies = to_migrate.dependencies
    assert dependencies == [TableView(catalog="hive_metastore", schema="database", name="v")]


def test_migrate_view_with_columns(ws):
    errors = {}
    create = "CREATE OR REPLACE VIEW hive_metastore.db1_src.view_src (a,b) AS SELECT * FROM db1_src.managed_dbfs"
    rows = {"SHOW CREATE TABLE": [{"createtab_stmt": create}]}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = TablesCrawler(backend, "inventory_database")
    table_mapping = mock_table_mapping(["managed_dbfs", "view"])
    migration_status_refresher = create_autospec(MigrationStatusRefresher)
    migration_status_refresher.get_seen_tables.return_value = {
        "ucx_default.db1_dst.managed_dbfs": "hive_metastore.db1_src.managed_dbfs"
    }
    migration_index = MigrationIndex(
        [
            MigrationStatus("db1_src", "managed_dbfs", "ucx_default", "db1_dst", "new_managed_dbfs"),
            MigrationStatus("db1_src", "view_src", "ucx_default", "db1_dst", "view_dst"),
        ]
    )
    migration_status_refresher.index.return_value = migration_index
    migrate_grants = create_autospec(MigrateGrants)
    table_migrate = TablesMigrator(
        table_crawler,
        ws,
        backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
    )
    table_migrate.migrate_tables(what=What.VIEW)

    create = "CREATE OR REPLACE VIEW IF NOT EXISTS `ucx_default`.`db1_dst`.`view_dst` (`a`, `b`) AS SELECT * FROM `ucx_default`.`db1_dst`.`new_managed_dbfs`"
    assert create in backend.queries
    migrate_grants.apply.assert_called()


def get_table_migrator(backend: SqlBackend) -> TablesMigrator:
    table_crawler = create_autospec(TablesCrawler)
    client = mock_workspace_client()
    client.catalogs.list.return_value = [CatalogInfo(name="cat1")]
    client.schemas.list.return_value = [
        SchemaInfo(catalog_name="cat1", name="test_schema1"),
        SchemaInfo(catalog_name="cat1", name="test_schema2"),
    ]
    client.tables.list.side_effect = cycle(
        [
            [
                TableInfo(
                    catalog_name="cat1",
                    schema_name="schema1",
                    name="dest1",
                    full_name="cat1.schema1.dest1",
                    properties={"upgraded_from": "hive_metastore.test_schema1.test_table1"},
                ),
                TableInfo(
                    catalog_name="cat1",
                    schema_name="schema1",
                    name="dest_view1",
                    full_name="cat1.schema1.dest_view1",
                    properties={"upgraded_from": "hive_metastore.test_schema1.test_view1"},
                ),
                TableInfo(
                    catalog_name="cat1",
                    schema_name="schema1",
                    name="dest2",
                    full_name="cat1.schema1.dest2",
                    properties={"upgraded_from": "hive_metastore.test_schema1.test_table2"},
                ),
            ],
            [
                TableInfo(
                    catalog_name="cat1",
                    schema_name="schema2",
                    name="dest3",
                    full_name="cat1.schema2.dest3",
                    properties={"upgraded_from": "hive_metastore.test_schema2.test_table3"},
                ),
            ],
        ]
    )

    simple_view = Table(
        object_type="VIEW",
        table_format="VIEW",
        catalog="hive_metastore",
        database="test_schema1",
        name="test_view1",
        view_text="SELECT * FROM SOMETHING ELSE",
        upgraded_to="cat1.schema1.dest_view1",
    )
    test_tables = [
        Table(
            object_type="EXTERNAL",
            table_format="DELTA",
            catalog="hive_metastore",
            database="test_schema1",
            name="test_table1",
            location="s3://some_location/table",
            upgraded_to="cat1.schema1.dest1",
        ),
        simple_view,
        Table(
            object_type="MANAGED",
            table_format="DELTA",
            catalog="hive_metastore",
            database="test_schema1",
            name="test_table2",
            location="dbfs:/dbfs_location/table",
            upgraded_to="cat1.schema1.dest2",
        ),
        Table(
            object_type="EXTERNAL",
            table_format="DELTA",
            catalog="hive_metastore",
            database="test_schema2",
            name="test_table3",
            location="s3://some_location/table",
            upgraded_to="cat1.schema2.dest3",
        ),
    ]
    table_crawler.snapshot.return_value = test_tables
    table_mapping = mock_table_mapping()
    migration_status_refresher = MigrationStatusRefresher(client, backend, "inventory_database", table_crawler)
    migrate_grants = create_autospec(MigrateGrants)  # pylint: disable=mock-no-usage
    table_migrate = TablesMigrator(
        table_crawler,
        client,
        backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
    )
    return table_migrate


def test_revert_migrated_tables_skip_managed():
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_migrate = get_table_migrator(backend)
    table_migrate.revert_migrated_tables(schema="test_schema1")
    revert_queries = backend.queries
    assert (
        "ALTER TABLE `hive_metastore`.`test_schema1`.`test_table1` UNSET TBLPROPERTIES IF EXISTS('upgraded_to');"
        in revert_queries
    )
    assert "DROP TABLE IF EXISTS `cat1`.`schema1`.`dest1`" in revert_queries
    assert (
        "ALTER VIEW `hive_metastore`.`test_schema1`.`test_view1` UNSET TBLPROPERTIES IF EXISTS('upgraded_to');"
        in revert_queries
    )
    assert "DROP VIEW IF EXISTS `cat1`.`schema1`.`dest_view1`" in revert_queries


def test_revert_migrated_tables_including_managed():
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_migrate = get_table_migrator(backend)
    # testing reverting managed tables
    table_migrate.revert_migrated_tables(schema="test_schema1", delete_managed=True)
    revert_with_managed_queries = backend.queries
    assert (
        "ALTER TABLE `hive_metastore`.`test_schema1`.`test_table1` UNSET TBLPROPERTIES IF EXISTS('upgraded_to');"
        in revert_with_managed_queries
    )
    assert "DROP TABLE IF EXISTS `cat1`.`schema1`.`dest1`" in revert_with_managed_queries
    assert (
        "ALTER VIEW `hive_metastore`.`test_schema1`.`test_view1` UNSET TBLPROPERTIES IF EXISTS('upgraded_to');"
        in revert_with_managed_queries
    )
    assert "DROP VIEW IF EXISTS `cat1`.`schema1`.`dest_view1`" in revert_with_managed_queries
    assert (
        "ALTER TABLE `hive_metastore`.`test_schema1`.`test_table2` UNSET TBLPROPERTIES IF EXISTS('upgraded_to');"
        in revert_with_managed_queries
    )
    assert "DROP TABLE IF EXISTS `cat1`.`schema1`.`dest2`" in revert_with_managed_queries


def test_no_migrated_tables(ws):
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = create_autospec(TablesCrawler)
    table_mapping = create_autospec(TableMapping)
    table_mapping.load.return_value = [
        Rule("workspace", "catalog_1", "db1", "db1", "managed", "managed"),
    ]
    migration_status_refresher = MigrationStatusRefresher(ws, backend, "inventory_database", table_crawler)
    migrate_grants = create_autospec(MigrateGrants)
    table_migrate = TablesMigrator(
        table_crawler,
        ws,
        backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
    )
    table_migrate.migrate_tables(what=What.DBFS_ROOT_DELTA)
    table_migrate.revert_migrated_tables("test_schema1", "test_table1")
    ws.catalogs.list.assert_called()
    table_crawler.snapshot.assert_called()
    table_mapping.get_tables_to_migrate.assert_called()
    migrate_grants.apply.assert_not_called()


def test_revert_report(ws, capsys):
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_migrate = get_table_migrator(backend)
    table_migrate.print_revert_report(delete_managed=True)
    captured = capsys.readouterr()
    assert "test_schema1|1|0|0|1|0|1|0|0|" in captured.out.replace(" ", "")
    assert "test_schema2|1|0|0|0|0|0|0|0|" in captured.out.replace(" ", "")
    assert "- Migrated DBFS Root Tables will be deleted" in captured.out

    table_migrate.print_revert_report(delete_managed=False)
    captured = capsys.readouterr()
    assert "- Migrated DBFS Root Tables will be left intact" in captured.out


def test_empty_revert_report(ws):
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = create_autospec(TablesCrawler)
    ws.tables.list.side_effect = []
    table_mapping = mock_table_mapping()
    migration_status_refresher = MigrationStatusRefresher(ws, backend, "inventory_database", table_crawler)
    migrate_grants = create_autospec(MigrateGrants)
    table_migrate = TablesMigrator(
        table_crawler,
        ws,
        backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
    )
    table_migrate.migrate_tables(what=What.DBFS_ROOT_DELTA)
    assert not table_migrate.print_revert_report(delete_managed=False)
    table_crawler.snapshot.assert_called()
    table_mapping.get_tables_to_migrate.assert_called()
    migrate_grants.apply.assert_not_called()


def test_is_upgraded(ws):
    errors = {}
    rows = {
        "SHOW TBLPROPERTIES `schema1`.`table1`": MockBackend.rows("key", "value")["upgrade_to", "fake_dest"],
        "SHOW TBLPROPERTIES `schema1`.`table2`": MockBackend.rows("key", "value")[
            "upgraded_to", "table table2 does not have property: upgraded_to"
        ],
    }
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = create_autospec(TablesCrawler)
    table_crawler.snapshot.return_value = [
        Table("hive_metastore", "schema1", "table1", "MANAGED", "DELTA"),
        Table("hive_metastore", "schema1", "table2", "MANAGED", "DELTA"),
    ]
    table_mapping = mock_table_mapping()
    migration_status_refresher = MigrationStatusRefresher(ws, backend, "inventory_database", table_crawler)
    migrate_grants = create_autospec(MigrateGrants)
    table_migrate = TablesMigrator(
        table_crawler,
        ws,
        backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
    )
    table_migrate.migrate_tables(what=What.DBFS_ROOT_DELTA)
    assert table_migrate.is_migrated("schema1", "table1")
    assert not table_migrate.is_migrated("schema1", "table2")
    table_mapping.get_tables_to_migrate.assert_called()
    migrate_grants.apply.assert_not_called()


def test_table_status():
    class FakeDate(datetime.datetime):

        def timestamp(self):
            return 0

    datetime.datetime = FakeDate
    errors = {}
    rows = {"SHOW TBLPROPERTIES `schema1`.`table1`": MockBackend.rows("key", "value")["upgrade_to", "cat1.schema1.dest1"]}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = create_autospec(TablesCrawler)
    table_crawler.snapshot.return_value = [
        Table(
            object_type="EXTERNAL",
            table_format="DELTA",
            catalog="hive_metastore",
            database="schema1",
            name="table1",
            location="s3://some_location/table1",
            upgraded_to="cat1.schema1.dest1",
        ),
        Table(
            object_type="EXTERNAL",
            table_format="DELTA",
            catalog="hive_metastore",
            database="schema1",
            name="table2",
            location="s3://some_location/table2",
            upgraded_to="foo.bar.err",
        ),
        Table(
            object_type="EXTERNAL",
            table_format="DELTA",
            catalog="hive_metastore",
            database="schema1",
            name="table3",
            location="s3://some_location/table2",
            upgraded_to="cat1.schema1.table3",
        ),
    ]
    client = mock_workspace_client()
    client.catalogs.list.return_value = [CatalogInfo(name="cat1")]
    client.schemas.list.return_value = [
        SchemaInfo(catalog_name="cat1", name="schema1"),
    ]
    client.tables.list.return_value = [
        TableInfo(
            catalog_name="cat1",
            schema_name="schema1",
            name="table1",
            full_name="cat1.schema1.table1",
            properties={"upgraded_from": "hive_metastore.schema1.table1"},
        ),
        TableInfo(
            catalog_name="cat1",
            schema_name="schema1",
            name="table2",
            full_name="cat1.schema1.table2",
            properties={"upgraded_from": "hive_metastore.schema1.table2"},
        ),
    ]
    table_status_crawler = MigrationStatusRefresher(client, backend, "ucx", table_crawler)
    snapshot = list(table_status_crawler.snapshot())
    assert snapshot == [
        MigrationStatus(
            src_schema='schema1',
            src_table='table1',
            dst_catalog='cat1',
            dst_schema='schema1',
            dst_table='table1',
            update_ts='0',
        ),
        MigrationStatus(
            src_schema='schema1',
            src_table='table2',
            dst_catalog=None,
            dst_schema=None,
            dst_table=None,
            update_ts='0',
        ),
        MigrationStatus(
            src_schema='schema1',
            src_table='table3',
            dst_catalog=None,
            dst_schema=None,
            dst_table=None,
            update_ts='0',
        ),
    ]


def test_table_status_reset():
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = create_autospec(TablesCrawler)
    client = create_autospec(WorkspaceClient)
    table_status_crawler = MigrationStatusRefresher(client, backend, "ucx", table_crawler)
    table_status_crawler.reset()
    assert backend.queries == [
        "TRUNCATE TABLE `hive_metastore`.`ucx`.`migration_status`",
    ]
    table_crawler.snapshot.assert_not_called()
    client.catalogs.list.assert_not_called()


def test_table_status_seen_tables(caplog):
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = create_autospec(TablesCrawler)
    client = create_autospec(WorkspaceClient)
    client.catalogs.list.return_value = [CatalogInfo(name="cat1"), CatalogInfo(name="deleted_cat")]
    client.schemas.list.side_effect = [
        [SchemaInfo(catalog_name="cat1", name="schema1"), SchemaInfo(catalog_name="cat1", name="deleted_schema")],
        NotFound(),
    ]
    client.tables.list.side_effect = [
        [
            TableInfo(
                catalog_name="cat1",
                schema_name="schema1",
                name="table1",
                full_name="cat1.schema1.table1",
                properties={"upgraded_from": "hive_metastore.schema1.table1"},
            ),
            TableInfo(
                catalog_name="cat1",
                schema_name="schema1",
                name="table2",
                full_name="cat1.schema1.table2",
                properties={"upgraded_from": "hive_metastore.schema1.table2"},
            ),
            TableInfo(
                catalog_name="cat1",
                schema_name="schema1",
                name="table3",
                full_name="cat1.schema1.table3",
                properties={"upgraded_from": "hive_metastore.schema1.table3"},
            ),
            TableInfo(
                catalog_name="cat1",
                schema_name="schema1",
                name="table4",
                full_name="cat1.schema1.table4",
            ),
            TableInfo(
                catalog_name="cat1",
                schema_name="schema1",
                name="table5",
                properties={"upgraded_from": "hive_metastore.schema1.table2"},
            ),
        ],
        NotFound(),
    ]
    table_status_crawler = MigrationStatusRefresher(client, backend, "ucx", table_crawler)
    seen_tables = table_status_crawler.get_seen_tables()
    assert seen_tables == {
        'cat1.schema1.table1': 'hive_metastore.schema1.table1',
        'cat1.schema1.table2': 'hive_metastore.schema1.table2',
        'cat1.schema1.table3': 'hive_metastore.schema1.table3',
    }
    assert "Catalog deleted_cat no longer exists. Skipping checking its migration status." in caplog.text
    assert "Schema cat1.deleted_schema no longer exists. Skipping checking its migration status." in caplog.text
    table_crawler.snapshot.assert_not_called()


GRANTS = MockBackend.rows("principal", "action_type", "catalog", "database", "table", "view")


def test_migrate_acls_should_produce_proper_queries(ws, caplog):
    # all grants succeed except for one
    table_crawler = create_autospec(TablesCrawler)
    src = Table('hive_metastore', 'db1_src', 'managed_dbfs', 'TABLE', 'DELTA', "/foo/bar/test")
    table_crawler.snapshot.return_value = [src]
    table_mapping = mock_table_mapping(["managed_dbfs"])
    migration_status_refresher = create_autospec(MigrationStatusRefresher)

    migrate_grants = create_autospec(MigrateGrants)
    migration_index = create_autospec(MigrationIndex)
    migration_index.is_migrated.return_value = False
    migration_status_refresher.index.return_value = migration_index
    sql_backend = MockBackend()

    table_migrate = TablesMigrator(
        table_crawler,
        ws,
        sql_backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
    )

    table_migrate.migrate_tables(what=What.DBFS_ROOT_DELTA)

    migrate_grants.apply.assert_called_with(src, 'ucx_default.db1_dst.managed_dbfs')
    assert sql_backend.queries == [
        'CREATE TABLE IF NOT EXISTS `ucx_default`.`db1_dst`.`managed_dbfs` DEEP CLONE `hive_metastore`.`db1_src`.`managed_dbfs`;',
        "ALTER TABLE `hive_metastore`.`db1_src`.`managed_dbfs` SET TBLPROPERTIES ('upgraded_to' = 'ucx_default.db1_dst.managed_dbfs');",
        "ALTER TABLE `ucx_default`.`db1_dst`.`managed_dbfs` SET TBLPROPERTIES ('upgraded_from' = 'hive_metastore.db1_src.managed_dbfs' , 'upgraded_from_workspace_id' = '12345');",
    ]


def test_migrate_views_should_be_properly_sequenced(ws):
    errors = {}
    rows = {
        "SHOW CREATE TABLE `hive_metastore`.`db1_src`.`v1_src`": [
            {"createtab_stmt": "CREATE OR REPLACE VIEW hive_metastore.db1_src.v1_src AS select * from db1_src.v3_src"},
        ],
        "SHOW CREATE TABLE `hive_metastore`.`db1_src`.`v2_src`": [
            {"createtab_stmt": "CREATE OR REPLACE VIEW hive_metastore.db1_src.v2_src AS select * from db1_src.t1_src"},
        ],
        "SHOW CREATE TABLE `hive_metastore`.`db1_src`.`v3_src`": [
            {"createtab_stmt": "CREATE OR REPLACE VIEW hive_metastore.db1_src.v3_src AS select * from db1_src.v2_src"},
        ],
    }
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = create_autospec(TablesCrawler)
    table_mapping = mock_table_mapping()
    table_mapping.get_tables_to_migrate.return_value = [
        TableToMigrate(
            Table("hive_metastore", "db1_src", "v1_src", "EXTERNAL", "VIEW", None, "select * from db1_src.v3_src"),
            Rule("workspace", "catalog", "db1_src", "db1_dst", "v1_src", "v1_dst"),
        ),
        TableToMigrate(
            Table("hive_metastore", "db1_src", "v2_src", "EXTERNAL", "VIEW", None, "select * from db1_src.t1_src"),
            Rule("workspace", "catalog", "db1_src", "db1_dst", "v2_src", "v2_dst"),
        ),
        TableToMigrate(
            Table("hive_metastore", "db1_src", "t1_src", "EXTERNAL", "TABLE"),
            Rule("workspace", "catalog", "db1_src", "db1_dst", "t1_src", "t1_dst"),
        ),
        TableToMigrate(
            Table("hive_metastore", "db1_src", "v3_src", "EXTERNAL", "VIEW", None, "select * from db1_src.v2_src"),
            Rule("workspace", "catalog", "db1_src", "db1_dst", "v3_src", "v3_dst"),
        ),
        TableToMigrate(
            Table("hive_metastore", "db1_src", "t2_src", "EXTERNAL", "TABLE"),
            Rule("workspace", "catalog", "db1_src", "db1_dst", "t2_src", "t2_dst"),
        ),
    ]
    migration_status_refresher = create_autospec(MigrationStatusRefresher)
    migration_status_refresher.get_seen_tables.return_value = {
        "ucx_default.db1_dst.managed_dbfs": "hive_metastore.db1_src.managed_dbfs"
    }
    migration_index = create_autospec(MigrationIndex)
    migration_index.get.return_value = MigrationStatus(
        src_schema="db1_src",
        src_table="t1_src",
        dst_catalog="catalog",
        dst_schema="db1_dst",
        dst_table="t1_dst",
    )
    migration_index.is_migrated.side_effect = lambda _, b: b in {"t1_src", "t2_src"}
    migration_status_refresher.index.return_value = migration_index
    migrate_grants = create_autospec(MigrateGrants)
    table_migrate = TablesMigrator(
        table_crawler,
        ws,
        backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
    )
    table_crawler.snapshot.assert_not_called()
    migrate_grants.apply.assert_not_called()
    tasks = table_migrate.migrate_tables(what=What.VIEW)
    table_keys = [task.args[0].src.key for task in tasks]
    assert table_keys.index("hive_metastore.db1_src.v1_src") > table_keys.index("hive_metastore.db1_src.v3_src")
    assert table_keys.index("hive_metastore.db1_src.v3_src") > table_keys.index("hive_metastore.db1_src.v2_src")
    assert next((key for key in table_keys if key == "hive_metastore.db1_src.t1_src"), None) is None


def test_table_in_mount_mapping_with_table_owner():
    client = create_autospec(WorkspaceClient)
    client.tables.get.side_effect = NotFound()
    backend = MockBackend(
        rows={
            'hive_metastore.test.tables': [
                ("hms", "mounted_datalake", "name", "object_type", "table_format", "abfss://bucket@msft/path/test")
            ],
            'DESCRIBE TABLE delta.`abfss://bucket@msft/path/test`;': [
                ("col1", "string", None),
                ("col2", "decimal", None),
            ],
        }
    )
    table_mapping = create_autospec(TableMapping)
    table_mapping.get_tables_to_migrate.return_value = [
        TableToMigrate(
            Table("hive_metastore", "mounted_datalake", "test", "EXTERNAL", "DELTA", "abfss://bucket@msft/path/test"),
            Rule("prod", "tgt_catalog", "mounted_datalake", "tgt_db", "abfss://bucket@msft/path/test", "test"),
        )
    ]
    table_crawler = TablesCrawler(backend, "inventory_database")
    migration_status_refresher = MigrationStatusRefresher(client, backend, "inventory_database", table_crawler)
    migrate_grants = create_autospec(MigrateGrants)
    table_migrate = TablesMigrator(
        table_crawler,
        client,
        backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
    )
    table_migrate.migrate_tables(what=What.TABLE_IN_MOUNT)
    assert (
        "CREATE TABLE IF NOT EXISTS `tgt_catalog`.`tgt_db`.`test` (col1 string, col2 decimal)  LOCATION 'abfss://bucket@msft/path/test';"
        in backend.queries
    )
    migrate_grants.apply.assert_called()


def test_table_in_mount_mapping_with_partition_information():
    client = create_autospec(WorkspaceClient)
    client.tables.get.side_effect = NotFound()
    backend = MockBackend(
        rows={
            'hive_metastore.test.tables': [
                ("hms", "mounted_datalake", "name", "object_type", "table_format", "abfss://bucket@msft/path/test")
            ],
            'DESCRIBE TABLE delta.`abfss://bucket@msft/path/test`;': [
                ("col1", "string", None),
                ("col2", "decimal", None),
                ("# Partition Information", None, None),
                ("# col_name", None, None),
                ("col1", None, None),
            ],
        }
    )
    table_mapping = create_autospec(TableMapping)
    table_mapping.get_tables_to_migrate.return_value = [
        TableToMigrate(
            Table("hive_metastore", "mounted_datalake", "test", "EXTERNAL", "DELTA", "abfss://bucket@msft/path/test"),
            Rule("prod", "tgt_catalog", "mounted_datalake", "tgt_db", "abfss://bucket@msft/path/test", "test"),
        )
    ]
    table_crawler = TablesCrawler(backend, "inventory_database")
    migration_status_refresher = MigrationStatusRefresher(client, backend, "inventory_database", table_crawler)
    migrate_grants = create_autospec(MigrateGrants)
    table_migrate = TablesMigrator(
        table_crawler,
        client,
        backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
    )
    table_migrate.migrate_tables(what=What.TABLE_IN_MOUNT)
    assert (
        "CREATE TABLE IF NOT EXISTS `tgt_catalog`.`tgt_db`.`test` (col1 string, col2 decimal) PARTITIONED BY (col1) LOCATION 'abfss://bucket@msft/path/test';"
        in backend.queries
    )
    migrate_grants.apply.assert_called()


def test_migrate_view_failed(ws, caplog):
    errors = {"CREATE OR REPLACE VIEW": "error"}
    create = "CREATE OR REPLACE VIEW hive_metastore.db1_src.view_src (a,b) AS SELECT * FROM db1_src.managed_dbfs"
    rows = {"SHOW CREATE TABLE": [{"createtab_stmt": create}]}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = TablesCrawler(backend, "inventory_database")
    table_mapping = mock_table_mapping(["managed_dbfs", "view"])
    migration_status_refresher = create_autospec(MigrationStatusRefresher)
    migration_status_refresher.get_seen_tables.return_value = {
        "ucx_default.db1_dst.managed_dbfs": "hive_metastore.db1_src.managed_dbfs"
    }
    migration_index = MigrationIndex(
        [
            MigrationStatus("db1_src", "managed_dbfs", "ucx_default", "db1_dst", "new_managed_dbfs"),
            MigrationStatus("db1_src", "view_src", "ucx_default", "db1_dst", "view_dst"),
        ]
    )
    migration_status_refresher.index.return_value = migration_index
    migrate_grants = create_autospec(MigrateGrants)
    table_migrate = TablesMigrator(
        table_crawler,
        ws,
        backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
    )
    table_migrate.migrate_tables(what=What.VIEW)

    assert (
        "Failed to migrate view hive_metastore.db1_src.view_src to ucx_default.db1_dst.view_dst: error" in caplog.text
    )
    migrate_grants.apply.assert_not_called()


def test_migrate_dbfs_root_tables_failed(ws, caplog):
    errors = {"CREATE TABLE IF NOT EXISTS": "error"}
    backend = MockBackend(fails_on_first=errors, rows={})
    table_crawler = TablesCrawler(backend, "inventory_database")
    table_mapping = mock_table_mapping(["managed_dbfs"])
    migration_status_refresher = MigrationStatusRefresher(ws, backend, "inventory_database", table_crawler)
    migrate_grants = create_autospec(MigrateGrants)
    table_migrate = TablesMigrator(
        table_crawler,
        ws,
        backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
    )
    table_migrate.migrate_tables(what=What.DBFS_ROOT_DELTA)

    migrate_grants.apply.assert_not_called()

    assert (
        "Failed to migrate table hive_metastore.db1_src.managed_dbfs to ucx_default.db1_dst.managed_dbfs: error"
        in caplog.text
    )


def test_revert_migrated_tables_failed(caplog):
    errors = {"ALTER TABLE": "error"}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_migrate = get_table_migrator(backend)
    table_migrate.revert_migrated_tables(schema="test_schema1")
    assert "Failed to revert table hive_metastore.test_schema1.test_table1: error" in caplog.text


def test_refresh_migration_status_published_remained_tables(caplog):
    backend = MockBackend()
    table_crawler = create_autospec(TablesCrawler)
    client = mock_workspace_client()
    table_crawler.snapshot.return_value = [
        Table(
            object_type="EXTERNAL",
            table_format="DELTA",
            catalog="hive_metastore",
            database="schema1",
            name="table1",
            location="s3://some_location/table1",
            upgraded_to="ucx_default.db1_dst.dst_table1",
        ),
        Table(
            object_type="EXTERNAL",
            table_format="DELTA",
            catalog="hive_metastore",
            database="schema1",
            name="table2",
            location="s3://some_location/table2",
            upgraded_to="ucx_default.db1_dst.dst_table2",
        ),
        Table(
            object_type="EXTERNAL",
            table_format="DELTA",
            catalog="hive_metastore",
            database="schema1",
            name="table3",
            location="s3://some_location/table3",
        ),
    ]
    table_mapping = mock_table_mapping()
    migration_status_refresher = create_autospec(MigrationStatusRefresher)
    migration_index = MigrationIndex(
        [
            MigrationStatus("schema1", "table1", "ucx_default", "db1_dst", "dst_table1"),
            MigrationStatus("schema1", "table2", "ucx_default", "db1_dst", "dst_table2"),
        ]
    )
    migration_status_refresher.index.return_value = migration_index
    migrate_grants = create_autospec(MigrateGrants)
    table_migrate = TablesMigrator(
        table_crawler,
        client,
        backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
    )
    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.hive_metastore"):
        tables = table_migrate.get_remaining_tables()
        assert 'remained-hive-metastore-table: hive_metastore.schema1.table3' in caplog.messages
        assert len(tables) == 1 and tables[0].key == "hive_metastore.schema1.table3"
    migrate_grants.assert_not_called()

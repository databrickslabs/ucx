import datetime
import logging
import sys
from collections.abc import Generator
from itertools import cycle
from unittest.mock import create_autospec

import pytest
from databricks.labs.lsql.backends import MockBackend, SqlBackend
from databricks.labs.lsql.core import Row
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import CatalogInfo, SchemaInfo, TableInfo

from databricks.labs.ucx.__about__ import __version__ as ucx_version
from databricks.labs.ucx.framework.owners import AdministratorLocator
from databricks.labs.ucx.framework.utils import escape_sql_identifier
from databricks.labs.ucx.hive_metastore.grants import MigrateGrants
from databricks.labs.ucx.hive_metastore.locations import ExternalLocations
from databricks.labs.ucx.hive_metastore.mapping import (
    Rule,
    TableMapping,
    TableToMigrate,
)
from databricks.labs.ucx.hive_metastore.table_migrate import (
    TablesMigrator,
)
from databricks.labs.ucx.hive_metastore.table_migration_status import (
    TableMigrationStatusRefresher,
    TableMigrationIndex,
    TableMigrationOwnership,
    TableMigrationStatus,
    TableView,
)
from databricks.labs.ucx.hive_metastore.tables import (
    Table,
    TablesCrawler,
    What,
)
from databricks.labs.ucx.hive_metastore.ownerhsip import TableOwnership
from databricks.labs.ucx.hive_metastore.view_migrate import ViewToMigrate
from databricks.labs.ucx.progress.history import ProgressEncoder

from .. import mock_table_mapping, mock_workspace_client


logger = logging.getLogger(__name__)


@pytest.fixture
def mock_pyspark(mocker):
    pyspark_sql_session = mocker.Mock()
    sys.modules["pyspark.sql.session"] = pyspark_sql_session


def test_migrate_dbfs_root_tables_should_produce_proper_queries(ws, mock_pyspark):

    errors = {}
    rows = {r"SYNC .*": MockBackend.rows("status_code", "description")[("SUCCESS", "test")]}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = TablesCrawler(backend, "inventory_database")
    table_mapping = mock_table_mapping(["managed_dbfs", "managed_mnt", "managed_other"])
    migration_status_refresher = TableMigrationStatusRefresher(ws, backend, "inventory_database", table_crawler)
    migrate_grants = create_autospec(MigrateGrants)
    external_locations = create_autospec(ExternalLocations)
    table_migrate = TablesMigrator(
        table_crawler,
        ws,
        backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
        external_locations,
    )
    table_migrate.migrate_tables(what=What.DBFS_ROOT_DELTA)
    table_migrate.migrate_tables(what=What.EXTERNAL_SYNC)

    assert (
        "CREATE TABLE IF NOT EXISTS `ucx_default`.`db1_dst`.`managed_dbfs` DEEP CLONE `hive_metastore`.`db1_src`.`managed_dbfs`;"
        in backend.queries
    )
    assert (
        "SYNC TABLE `ucx_default`.`db1_dst`.`managed_mnt` FROM `hive_metastore`.`db1_src`.`managed_mnt`;"
        not in backend.queries
    )
    assert (
        "ALTER TABLE `hive_metastore`.`db1_src`.`managed_dbfs` "
        "SET TBLPROPERTIES ('upgraded_to' = 'ucx_default.db1_dst.managed_dbfs');"
    ) in backend.queries
    assert (
        f"ALTER TABLE `ucx_default`.`db1_dst`.`managed_dbfs` "
        f"SET TBLPROPERTIES ('upgraded_from' = 'hive_metastore.db1_src.managed_dbfs' , "
        f"'{Table.UPGRADED_FROM_WS_PARAM}' = '123');"
    ) in backend.queries
    assert (
        "SYNC TABLE `ucx_default`.`db1_dst`.`managed_other` FROM `hive_metastore`.`db1_src`.`managed_other`;"
        not in backend.queries
    )
    migrate_grants.apply.assert_called()
    external_locations.resolve_mount.assert_not_called()


def test_dbfs_non_delta_tables_should_produce_proper_queries(ws, mock_pyspark):
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
    migration_status_refresher = TableMigrationStatusRefresher(ws, backend, "inventory_database", table_crawler)
    migrate_grants = create_autospec(MigrateGrants)
    external_locations = create_autospec(ExternalLocations)
    table_migrate = TablesMigrator(
        table_crawler,
        ws,
        backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
        external_locations,
    )
    table_migrate.migrate_tables(what=What.DBFS_ROOT_NON_DELTA)

    migrate_grants.apply.assert_called()
    external_locations.resolve_mount.assert_not_called()

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
        f"'{Table.UPGRADED_FROM_WS_PARAM}' = '123');"
    ) in backend.queries


def test_migrate_dbfs_root_tables_should_be_skipped_when_upgrading_external(ws, mock_pyspark):
    errors = {}
    rows = {}
    crawler_backend = MockBackend(fails_on_first=errors, rows=rows)
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = TablesCrawler(crawler_backend, "inventory_database")
    table_mapping = mock_table_mapping(["managed_dbfs"])
    migration_status_refresher = TableMigrationStatusRefresher(ws, backend, "inventory_database", table_crawler)
    migrate_grants = create_autospec(MigrateGrants)
    external_locations = create_autospec(ExternalLocations)
    table_migrate = TablesMigrator(
        table_crawler,
        ws,
        backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
        external_locations,
    )
    table_migrate.migrate_tables(what=What.EXTERNAL_SYNC)

    migrate_grants.apply.assert_not_called()
    external_locations.resolve_mount.assert_not_called()


def test_migrate_external_tables_should_produce_proper_queries(ws, mock_pyspark):
    errors = {}
    rows = {r"SYNC .*": MockBackend.rows("status_code", "description")[("SUCCESS", "test")]}
    crawler_backend = MockBackend(fails_on_first=errors, rows=rows)
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = TablesCrawler(crawler_backend, "inventory_database")
    table_mapping = mock_table_mapping(["external_src"])
    migration_status_refresher = TableMigrationStatusRefresher(ws, backend, "inventory_database", table_crawler)
    migrate_grants = create_autospec(MigrateGrants)
    external_locations = create_autospec(ExternalLocations)
    table_migrate = TablesMigrator(
        table_crawler,
        ws,
        backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
        external_locations,
    )
    table_migrate.migrate_tables(what=What.EXTERNAL_SYNC)

    migrate_grants.apply.assert_called()
    external_locations.resolve_mount.assert_not_called()

    assert backend.queries == [
        "SYNC TABLE `ucx_default`.`db1_dst`.`external_dst` FROM `hive_metastore`.`db1_src`.`external_src`;",
        (
            f"ALTER TABLE `ucx_default`.`db1_dst`.`external_dst` "
            f"SET TBLPROPERTIES ('upgraded_from' = 'hive_metastore.db1_src.external_src' , "
            f"'{Table.UPGRADED_FROM_WS_PARAM}' = '123');"
        ),
    ]


def test_migrate_managed_table_as_external_tables_with_conversion(ws, mock_pyspark):
    errors = {}
    rows = {r"SYNC .*": MockBackend.rows("status_code", "description")[("SUCCESS", "test")]}
    crawler_backend = MockBackend(fails_on_first=errors, rows=rows)
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = TablesCrawler(crawler_backend, "inventory_database")
    table_mapping = mock_table_mapping(["managed_other"])
    migration_status_refresher = TableMigrationStatusRefresher(ws, backend, "inventory_database", table_crawler)
    migrate_grants = create_autospec(MigrateGrants)
    external_locations = create_autospec(ExternalLocations)
    table_migrate = TablesMigrator(
        table_crawler, ws, backend, table_mapping, migration_status_refresher, migrate_grants, external_locations
    )
    table_migrate.migrate_tables(what=What.EXTERNAL_SYNC, managed_table_external_storage="CONVERT_TO_EXTERNAL")

    migrate_grants.apply.assert_called()
    external_locations.resolve_mount.assert_not_called()

    assert backend.queries == [
        "SYNC TABLE `ucx_default`.`db1_dst`.`managed_other` FROM `hive_metastore`.`db1_src`.`managed_other`;",
        (
            f"ALTER TABLE `ucx_default`.`db1_dst`.`managed_other` "
            f"SET TBLPROPERTIES ('upgraded_from' = 'hive_metastore.db1_src.managed_other' , "
            f"'{Table.UPGRADED_FROM_WS_PARAM}' = '123');"
        ),
    ]


def test_migrate_managed_table_as_external_tables_without_conversion(ws, mock_pyspark):
    errors = {}
    rows = {r"SYNC .*": MockBackend.rows("status_code", "description")[("SUCCESS", "test")]}
    crawler_backend = MockBackend(fails_on_first=errors, rows=rows)
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = TablesCrawler(crawler_backend, "inventory_database")
    table_mapping = mock_table_mapping(["managed_other"])
    migration_status_refresher = TableMigrationStatusRefresher(ws, backend, "inventory_database", table_crawler)
    migrate_grants = create_autospec(MigrateGrants)
    external_locations = create_autospec(ExternalLocations)
    table_migrate = TablesMigrator(
        table_crawler,
        ws,
        backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
        external_locations,
    )
    table_migrate.migrate_tables(what=What.EXTERNAL_SYNC, managed_table_external_storage="SYNC_AS_EXTERNAL")

    migrate_grants.apply.assert_called()
    external_locations.resolve_mount.assert_not_called()

    assert backend.queries == [
        "SYNC TABLE `ucx_default`.`db1_dst`.`managed_other` AS EXTERNAL FROM `hive_metastore`.`db1_src`.`managed_other`;",
        (
            f"ALTER TABLE `ucx_default`.`db1_dst`.`managed_other` "
            f"SET TBLPROPERTIES ('upgraded_from' = 'hive_metastore.db1_src.managed_other' , "
            f"'{Table.UPGRADED_FROM_WS_PARAM}' = '123');"
        ),
    ]


def test_migrate_managed_table_as_managed_tables_should_produce_proper_queries(ws, mock_pyspark):
    errors = {}
    rows = {r"SYNC .*": MockBackend.rows("status_code", "description")[("SUCCESS", "test")]}
    crawler_backend = MockBackend(fails_on_first=errors, rows=rows)
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = TablesCrawler(crawler_backend, "inventory_database")
    table_mapping = mock_table_mapping(["managed_other"])
    migration_status_refresher = TableMigrationStatusRefresher(ws, backend, "inventory_database", table_crawler)
    migrate_grants = create_autospec(MigrateGrants)
    external_locations = create_autospec(ExternalLocations)
    table_migrate = TablesMigrator(
        table_crawler,
        ws,
        backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
        external_locations,
    )
    table_migrate.migrate_tables(what=What.EXTERNAL_SYNC, managed_table_external_storage="CLONE")

    migrate_grants.apply.assert_called()
    external_locations.resolve_mount.assert_not_called()

    assert backend.queries == [
        "CREATE TABLE IF NOT EXISTS `ucx_default`.`db1_dst`.`managed_other` AS SELECT * FROM `hive_metastore`.`db1_src`.`managed_other`",
        "ALTER TABLE `hive_metastore`.`db1_src`.`managed_other` SET TBLPROPERTIES "
        "('upgraded_to' = 'ucx_default.db1_dst.managed_other');",
        "COMMENT ON TABLE `hive_metastore`.`db1_src`.`managed_other` IS 'This table "
        'is deprecated. Please use `ucx_default.db1_dst.managed_other` instead of '
        "`hive_metastore.db1_src.managed_other`.';",
        (
            f"ALTER TABLE `ucx_default`.`db1_dst`.`managed_other` "
            f"SET TBLPROPERTIES ('upgraded_from' = 'hive_metastore.db1_src.managed_other' , "
            f"'{Table.UPGRADED_FROM_WS_PARAM}' = '123');"
        ),
    ]


def test_migrate_external_table_failed_sync(ws, caplog, mock_pyspark):
    errors = {}
    rows = {r"SYNC .*": MockBackend.rows("status_code", "description")[("LOCATION_OVERLAP", "test")]}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    crawler_backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = TablesCrawler(crawler_backend, "inventory_database")
    table_mapping = mock_table_mapping(["external_src"])
    migration_status_refresher = TableMigrationStatusRefresher(ws, backend, "inventory_database", table_crawler)
    migrate_grants = create_autospec(MigrateGrants)
    external_locations = create_autospec(ExternalLocations)
    table_migrate = TablesMigrator(
        table_crawler,
        ws,
        backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
        external_locations,
    )
    table_migrate.migrate_tables(what=What.EXTERNAL_SYNC)
    assert "SYNC command failed to migrate" in caplog.text
    migrate_grants.apply.assert_not_called()
    external_locations.resolve_mount.assert_not_called()


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
            "CREATE TABLE IF NOT EXISTS `ucx_default`.`db1_dst`.`external_dst` LOCATION 's3://test/folder/table1_ctas_migrated' AS SELECT * FROM `hive_metastore`.`db1_src`.`external_src`",
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
    ws, caplog, hiveserde_in_place_migrate, describe, ddl, errors, migrated, expected_value, mock_pyspark
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
    migration_status_refresher = TableMigrationStatusRefresher(ws, backend, "inventory_database", table_crawler)
    migrate_grants = create_autospec(MigrateGrants)
    external_locations = create_autospec(ExternalLocations)

    def resolve_mount(location: str) -> str:
        if location.startswith("dbfs:/mnt/test"):
            return location.replace("dbfs:/mnt/test", "s3://test/folder")
        return location

    external_locations.resolve_mount.side_effect = resolve_mount

    table_migrate = TablesMigrator(
        table_crawler,
        ws,
        backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
        external_locations,
    )

    table_migrate.migrate_tables(
        what=What.EXTERNAL_HIVESERDE,
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
def test_migrate_external_tables_ctas_should_produce_proper_queries(ws, what, test_table, expected_query, mock_pyspark):
    backend = MockBackend()
    table_crawler = TablesCrawler(backend, "inventory_database")
    table_mapping = mock_table_mapping([test_table])
    migration_status_refresher = TableMigrationStatusRefresher(ws, backend, "inventory_database", table_crawler)
    migrate_grants = create_autospec(MigrateGrants)
    external_locations = create_autospec(ExternalLocations)

    def resolve_mount(location: str) -> str:
        if location.startswith("dbfs:/mnt/test"):
            return location.replace("dbfs:/mnt/test", "s3://test/folder")
        return location

    external_locations.resolve_mount.side_effect = resolve_mount
    table_migrate = TablesMigrator(
        table_crawler,
        ws,
        backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
        external_locations,
    )
    table_migrate.migrate_tables(what=what)

    assert expected_query in backend.queries
    migrate_grants.apply.assert_called()


def test_migrate_already_upgraded_table_should_produce_no_queries(ws, mock_pyspark):
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
    migration_status_refresher = TableMigrationStatusRefresher(ws, backend, "inventory_database", table_crawler)
    migrate_grants = create_autospec(MigrateGrants)
    external_locations = create_autospec(ExternalLocations)

    table_migrate = TablesMigrator(
        table_crawler,
        ws,
        backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
        external_locations,
    )
    table_migrate.migrate_tables(what=What.EXTERNAL_SYNC)

    assert not backend.queries
    migrate_grants.apply.assert_not_called()
    external_locations.resolve_mount.assert_not_called()


def test_migrate_unsupported_format_table_should_produce_no_queries(ws, mock_pyspark):
    errors = {}
    rows = {}
    crawler_backend = MockBackend(fails_on_first=errors, rows=rows)
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = TablesCrawler(crawler_backend, "inventory_database")
    table_mapping = mock_table_mapping(["external_src_unsupported"])
    migration_status_refresher = TableMigrationStatusRefresher(ws, backend, "inventory_database", table_crawler)
    migrate_grants = create_autospec(MigrateGrants)
    external_locations = create_autospec(ExternalLocations)
    table_migrate = TablesMigrator(
        table_crawler,
        ws,
        backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
        external_locations,
    )
    table_migrate.migrate_tables(what=What.UNKNOWN)

    assert len(backend.queries) == 0
    migrate_grants.apply.assert_not_called()
    external_locations.resolve_mount.assert_not_called()


def test_migrate_view_should_produce_proper_queries(ws, mock_pyspark):
    errors = {}
    original_view = (
        "CREATE OR REPLACE VIEW `hive_metastore`.`db1_src`.`view_src` AS SELECT * FROM `db1_src`.`managed_dbfs`"
    )
    rows = {"SHOW CREATE TABLE": [{"createtab_stmt": original_view}]}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = TablesCrawler(backend, "inventory_database")
    table_mapping = mock_table_mapping(["managed_dbfs", "view"])
    migration_status_refresher = create_autospec(TableMigrationStatusRefresher)
    migration_status_refresher.get_seen_tables.return_value = {
        "ucx_default.db1_dst.managed_dbfs": "hive_metastore.db1_src.managed_dbfs"
    }
    migration_index = TableMigrationIndex(
        [
            TableMigrationStatus("db1_src", "managed_dbfs", "ucx_default", "db1_dst", "new_managed_dbfs"),
            TableMigrationStatus("db1_src", "view_src", "ucx_default", "db1_dst", "view_dst"),
        ]
    )
    migration_status_refresher.index.return_value = migration_index
    migrate_grants = create_autospec(MigrateGrants)
    external_locations = create_autospec(ExternalLocations)
    table_migrate = TablesMigrator(
        table_crawler,
        ws,
        backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
        external_locations,
    )
    table_migrate.migrate_tables(what=What.VIEW)

    create = "CREATE OR REPLACE VIEW IF NOT EXISTS `ucx_default`.`db1_dst`.`view_dst` AS SELECT * FROM `ucx_default`.`db1_dst`.`new_managed_dbfs`"
    assert create in backend.queries
    src = "ALTER VIEW `hive_metastore`.`db1_src`.`view_src` SET TBLPROPERTIES ('upgraded_to' = 'ucx_default.db1_dst.view_dst');"
    assert src in backend.queries
    dst = f"ALTER VIEW `ucx_default`.`db1_dst`.`view_dst` SET TBLPROPERTIES ('upgraded_from' = 'hive_metastore.db1_src.view_src' , '{Table.UPGRADED_FROM_WS_PARAM}' = '123');"
    assert dst in backend.queries
    migrate_grants.apply.assert_called()
    external_locations.resolve_mount.assert_not_called()


def test_migrate_view_with_local_dataset_should_be_skipped(ws):
    ddl = "CREATE VIEW v AS WITH t(a) AS (SELECT 1) SELECT * FROM t;"
    to_migrate = ViewToMigrate(
        Table("hive_metastore", "database", "", "EXTERNAL", "VIEW", None, ddl),
        Rule("workspace", "catalog", "", "", "", ""),
    )
    dependencies = to_migrate.dependencies
    assert dependencies == [TableView(catalog="hive_metastore", schema="database", name="v")]


def test_migrate_view_with_columns(ws, mock_pyspark):
    errors = {}
    create = "CREATE OR REPLACE VIEW hive_metastore.db1_src.view_src (a,b) AS SELECT * FROM db1_src.managed_dbfs"
    rows = {"SHOW CREATE TABLE": [{"createtab_stmt": create}]}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = TablesCrawler(backend, "inventory_database")
    table_mapping = mock_table_mapping(["managed_dbfs", "view"])
    migration_status_refresher = create_autospec(TableMigrationStatusRefresher)
    migration_status_refresher.get_seen_tables.return_value = {
        "ucx_default.db1_dst.managed_dbfs": "hive_metastore.db1_src.managed_dbfs"
    }
    migration_index = TableMigrationIndex(
        [
            TableMigrationStatus("db1_src", "managed_dbfs", "ucx_default", "db1_dst", "new_managed_dbfs"),
            TableMigrationStatus("db1_src", "view_src", "ucx_default", "db1_dst", "view_dst"),
        ]
    )
    migration_status_refresher.index.return_value = migration_index
    migrate_grants = create_autospec(MigrateGrants)
    external_locations = create_autospec(ExternalLocations)
    table_migrate = TablesMigrator(
        table_crawler,
        ws,
        backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
        external_locations,
    )
    table_migrate.migrate_tables(what=What.VIEW)

    create = "CREATE OR REPLACE VIEW IF NOT EXISTS `ucx_default`.`db1_dst`.`view_dst` (`a`, `b`) AS SELECT * FROM `ucx_default`.`db1_dst`.`new_managed_dbfs`"
    assert create in backend.queries
    migrate_grants.apply.assert_called()
    external_locations.resolve_mount.assert_not_called()


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
    migration_status_refresher = TableMigrationStatusRefresher(client, backend, "inventory_database", table_crawler)
    migrate_grants = create_autospec(MigrateGrants)  # pylint: disable=mock-no-usage
    external_locations = create_autospec(ExternalLocations)  # pylint: disable=mock-no-usage
    table_migrate = TablesMigrator(
        table_crawler,
        client,
        backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
        external_locations,
    )
    return table_migrate


def test_revert_migrated_tables_skip_managed(mock_pyspark):
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


def test_revert_migrated_tables_including_managed(mock_pyspark):
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


def test_no_migrated_tables(ws, mock_pyspark):
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = create_autospec(TablesCrawler)
    table_mapping = create_autospec(TableMapping)
    table_mapping.load.return_value = [
        Rule("workspace", "catalog_1", "db1", "db1", "managed", "managed"),
    ]
    migration_status_refresher = TableMigrationStatusRefresher(ws, backend, "inventory_database", table_crawler)
    migrate_grants = create_autospec(MigrateGrants)
    external_locations = create_autospec(ExternalLocations)
    table_migrate = TablesMigrator(
        table_crawler,
        ws,
        backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
        external_locations,
    )
    table_migrate.migrate_tables(what=What.DBFS_ROOT_DELTA)
    table_migrate.revert_migrated_tables("test_schema1", "test_table1")
    ws.catalogs.list.assert_called()
    table_crawler.snapshot.assert_called()
    table_mapping.get_tables_to_migrate.assert_called()
    migrate_grants.apply.assert_not_called()
    external_locations.resolve_mount.assert_not_called()


def test_revert_report(ws, capsys, mock_pyspark):
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


def test_empty_revert_report(ws, mock_pyspark):
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = create_autospec(TablesCrawler)
    ws.tables.list.side_effect = []
    table_mapping = mock_table_mapping()
    migration_status_refresher = TableMigrationStatusRefresher(ws, backend, "inventory_database", table_crawler)
    migrate_grants = create_autospec(MigrateGrants)
    external_locations = create_autospec(ExternalLocations)
    table_migrate = TablesMigrator(
        table_crawler,
        ws,
        backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
        external_locations,
    )
    table_migrate.migrate_tables(what=What.DBFS_ROOT_DELTA)
    assert not table_migrate.print_revert_report(delete_managed=False)
    table_crawler.snapshot.assert_called()
    table_mapping.get_tables_to_migrate.assert_called()
    migrate_grants.apply.assert_not_called()
    external_locations.resolve_mount.assert_not_called()


def test_is_upgraded(ws, mock_pyspark):
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
    migration_status_refresher = TableMigrationStatusRefresher(ws, backend, "inventory_database", table_crawler)
    migrate_grants = create_autospec(MigrateGrants)
    external_locations = create_autospec(ExternalLocations)
    table_migrate = TablesMigrator(
        table_crawler,
        ws,
        backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
        external_locations,
    )
    table_migrate.migrate_tables(what=What.DBFS_ROOT_DELTA)
    assert table_migrate.is_migrated("schema1", "table1")
    assert not table_migrate.is_migrated("schema1", "table2")
    table_mapping.get_tables_to_migrate.assert_called()
    migrate_grants.apply.assert_not_called()
    external_locations.resolve_mount.assert_not_called()


@pytest.fixture
def datetime_with_epoch_timestamp(monkeypatch) -> Generator[None, None, None]:
    # The timestamp() method on datetime() is immutable, so we can't just patch/mock it. Rather we need to substitute
    # the entire class, which the normal mocking/patching routines don't seem to support.
    # Note: this will not affect any modules that have already initialized and imported the class directly. Similarly,
    # the effect cannot be unwound if this test triggers initializing of modules that import the class directly instead
    # of the module.
    # If you find yourself here debugging an issue, please be aware that the following can be problematic:
    # >>> from datetime import datetime
    # (Why? It's reference to the class directly, and having the name of the module makes inspection tedious.)
    # Prefer instead:
    # >>> import datetime
    # Or even better:
    # >>> import datetime as dt

    class FakeDateTime(datetime.datetime):

        def timestamp(self):
            return 0

    _original_datetime = datetime.datetime
    try:
        datetime.datetime = FakeDateTime  # type: ignore[misc]
        yield
    finally:
        datetime.datetime = _original_datetime  # type: ignore[misc]


def test_table_status(datetime_with_epoch_timestamp) -> None:
    errors: dict[str, str] = {}
    rows = {
        "SHOW TBLPROPERTIES `schema1`.`table1`": MockBackend.rows("key", "value")["upgrade_to", "cat1.schema1.dest1"]
    }
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
    table_status_crawler = TableMigrationStatusRefresher(client, backend, "ucx", table_crawler)
    snapshot = list(table_status_crawler.snapshot())
    assert snapshot == [
        TableMigrationStatus(
            src_schema='schema1',
            src_table='table1',
            dst_catalog='cat1',
            dst_schema='schema1',
            dst_table='table1',
            update_ts='0',
        ),
        TableMigrationStatus(
            src_schema='schema1',
            src_table='table2',
            dst_catalog=None,
            dst_schema=None,
            dst_table=None,
            update_ts='0',
        ),
        TableMigrationStatus(
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
    table_status_crawler = TableMigrationStatusRefresher(client, backend, "ucx", table_crawler)
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
    table_status_crawler = TableMigrationStatusRefresher(client, backend, "ucx", table_crawler)
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


def test_migrate_acls_should_produce_proper_queries(ws, caplog, mock_pyspark) -> None:
    # all grants succeed except for one
    table_crawler = create_autospec(TablesCrawler)
    src = Table('hive_metastore', 'db1_src', 'managed_dbfs', 'TABLE', 'DELTA', "/foo/bar/test")
    dst = Table('ucx_default', 'db1_dst', 'managed_dbfs', 'MANAGED', 'DELTA')
    table_crawler.snapshot.return_value = [src]
    table_mapping = mock_table_mapping(["managed_dbfs"])
    migration_status_refresher = create_autospec(TableMigrationStatusRefresher)

    migrate_grants = create_autospec(MigrateGrants)
    migration_index = create_autospec(TableMigrationIndex)
    migration_index.is_migrated.return_value = False
    migration_status_refresher.index.return_value = migration_index
    sql_backend = MockBackend()
    external_locations = create_autospec(ExternalLocations)

    table_migrate = TablesMigrator(
        table_crawler,
        ws,
        sql_backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
        external_locations,
    )

    table_migrate.migrate_tables(what=What.DBFS_ROOT_DELTA)

    migrate_grants.apply.assert_called_with(src, dst)
    external_locations.resolve_mount.assert_not_called()
    assert sql_backend.queries == [
        'CREATE TABLE IF NOT EXISTS `ucx_default`.`db1_dst`.`managed_dbfs` DEEP CLONE `hive_metastore`.`db1_src`.`managed_dbfs`;',
        "ALTER TABLE `hive_metastore`.`db1_src`.`managed_dbfs` SET TBLPROPERTIES ('upgraded_to' = 'ucx_default.db1_dst.managed_dbfs');",
        "COMMENT ON TABLE `hive_metastore`.`db1_src`.`managed_dbfs` IS 'This table is deprecated. Please use `ucx_default.db1_dst.managed_dbfs` instead of `hive_metastore.db1_src.managed_dbfs`.';",
        "ALTER TABLE `ucx_default`.`db1_dst`.`managed_dbfs` SET TBLPROPERTIES ('upgraded_from' = 'hive_metastore.db1_src.managed_dbfs' , 'upgraded_from_workspace_id' = '123');",
    ]


def test_migrate_views_should_be_properly_sequenced(ws, mock_pyspark):
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
    migration_status_refresher = create_autospec(TableMigrationStatusRefresher)
    migration_status_refresher.get_seen_tables.return_value = {
        "ucx_default.db1_dst.managed_dbfs": "hive_metastore.db1_src.managed_dbfs"
    }
    migration_index = create_autospec(TableMigrationIndex)
    migration_index.get.return_value = TableMigrationStatus(
        src_schema="db1_src",
        src_table="t1_src",
        dst_catalog="catalog",
        dst_schema="db1_dst",
        dst_table="t1_dst",
    )
    migration_index.is_migrated.side_effect = lambda _, b: b in {"t1_src", "t2_src"}
    migration_status_refresher.index.return_value = migration_index
    migrate_grants = create_autospec(MigrateGrants)
    external_locations = create_autospec(ExternalLocations)
    table_migrate = TablesMigrator(
        table_crawler,
        ws,
        backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
        external_locations,
    )
    table_crawler.snapshot.assert_not_called()
    migrate_grants.apply.assert_not_called()
    tasks = table_migrate.migrate_tables(what=What.VIEW)
    table_keys = [task.args[0].src.key for task in tasks]
    assert table_keys.index("hive_metastore.db1_src.v1_src") > table_keys.index("hive_metastore.db1_src.v3_src")
    assert table_keys.index("hive_metastore.db1_src.v3_src") > table_keys.index("hive_metastore.db1_src.v2_src")
    assert not any(key for key in table_keys if key == "hive_metastore.db1_src.t1_src")
    external_locations.resolve_mount.assert_not_called()


def test_table_in_mount_mapping_with_table_owner(mock_pyspark):
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
    migration_status_refresher = TableMigrationStatusRefresher(client, backend, "inventory_database", table_crawler)
    migrate_grants = create_autospec(MigrateGrants)
    external_locations = create_autospec(ExternalLocations)
    table_migrate = TablesMigrator(
        table_crawler,
        client,
        backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
        external_locations,
    )
    table_migrate.migrate_tables(what=What.TABLE_IN_MOUNT)
    assert (
        "CREATE TABLE IF NOT EXISTS `tgt_catalog`.`tgt_db`.`test` (`col1` string, `col2` decimal)  LOCATION 'abfss://bucket@msft/path/test';"
        in backend.queries
    )
    migrate_grants.apply.assert_called()
    external_locations.resolve_mount.assert_not_called()


def test_table_in_mount_mapping_with_partition_information(mock_pyspark):
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
    migration_status_refresher = TableMigrationStatusRefresher(client, backend, "inventory_database", table_crawler)
    migrate_grants = create_autospec(MigrateGrants)
    external_locations = create_autospec(ExternalLocations)
    table_migrate = TablesMigrator(
        table_crawler,
        client,
        backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
        external_locations,
    )
    table_migrate.migrate_tables(what=What.TABLE_IN_MOUNT)
    assert (
        "CREATE TABLE IF NOT EXISTS `tgt_catalog`.`tgt_db`.`test` (`col1` string, `col2` decimal) PARTITIONED BY (`col1`) LOCATION 'abfss://bucket@msft/path/test';"
        in backend.queries
    )
    migrate_grants.apply.assert_called()
    external_locations.resolve_mount.assert_not_called()


def test_migrate_view_failed(ws, caplog, mock_pyspark):
    errors = {"CREATE OR REPLACE VIEW": "error"}
    create = "CREATE OR REPLACE VIEW hive_metastore.db1_src.view_src (a,b) AS SELECT * FROM db1_src.managed_dbfs"
    rows = {"SHOW CREATE TABLE": [{"createtab_stmt": create}]}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = TablesCrawler(backend, "inventory_database")
    table_mapping = mock_table_mapping(["managed_dbfs", "view"])
    migration_status_refresher = create_autospec(TableMigrationStatusRefresher)
    migration_status_refresher.get_seen_tables.return_value = {
        "ucx_default.db1_dst.managed_dbfs": "hive_metastore.db1_src.managed_dbfs"
    }
    migration_index = TableMigrationIndex(
        [
            TableMigrationStatus("db1_src", "managed_dbfs", "ucx_default", "db1_dst", "new_managed_dbfs"),
            TableMigrationStatus("db1_src", "view_src", "ucx_default", "db1_dst", "view_dst"),
        ]
    )
    migration_status_refresher.index.return_value = migration_index
    migrate_grants = create_autospec(MigrateGrants)
    external_locations = create_autospec(ExternalLocations)
    table_migrate = TablesMigrator(
        table_crawler,
        ws,
        backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
        external_locations,
    )
    table_migrate.migrate_tables(what=What.VIEW)

    assert (
        "Failed to migrate view hive_metastore.db1_src.view_src to ucx_default.db1_dst.view_dst: error" in caplog.text
    )
    migrate_grants.apply.assert_not_called()
    external_locations.resolve_mount.assert_not_called()


def test_migrate_dbfs_root_tables_failed(ws, caplog, mock_pyspark):
    errors = {"CREATE TABLE IF NOT EXISTS": "error"}
    backend = MockBackend(fails_on_first=errors, rows={})
    table_crawler = TablesCrawler(backend, "inventory_database")
    table_mapping = mock_table_mapping(["managed_dbfs"])
    migration_status_refresher = TableMigrationStatusRefresher(ws, backend, "inventory_database", table_crawler)
    migrate_grants = create_autospec(MigrateGrants)
    external_locations = create_autospec(ExternalLocations)
    table_migrate = TablesMigrator(
        table_crawler,
        ws,
        backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
        external_locations,
    )
    table_migrate.migrate_tables(what=What.DBFS_ROOT_DELTA)

    migrate_grants.apply.assert_not_called()
    external_locations.resolve_mount.assert_not_called()

    assert (
        "Failed to migrate table hive_metastore.db1_src.managed_dbfs to ucx_default.db1_dst.managed_dbfs: error"
        in caplog.text
    )


def test_revert_migrated_tables_failed(caplog, mock_pyspark):
    errors = {"ALTER TABLE": "error"}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_migrate = get_table_migrator(backend)
    table_migrate.revert_migrated_tables(schema="test_schema1")
    assert "Failed to revert table hive_metastore.test_schema1.test_table1: error" in caplog.text


def test_refresh_migration_status_published_remained_tables(caplog, mock_pyspark):
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
    migration_status_refresher = create_autospec(TableMigrationStatusRefresher)
    migration_index = TableMigrationIndex(
        [
            TableMigrationStatus("schema1", "table1", "ucx_default", "db1_dst", "dst_table1"),
            TableMigrationStatus("schema1", "table2", "ucx_default", "db1_dst", "dst_table2"),
        ]
    )
    migration_status_refresher.index.return_value = migration_index
    migrate_grants = create_autospec(MigrateGrants)
    external_locations = create_autospec(ExternalLocations)
    table_migrate = TablesMigrator(
        table_crawler,
        client,
        backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
        external_locations,
    )
    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.hive_metastore"):
        tables = table_migrate.get_remaining_tables()
        assert 'remained-hive-metastore-table: hive_metastore.schema1.table3' in caplog.messages
        assert len(tables) == 1 and tables[0].key == "hive_metastore.schema1.table3"
    migrate_grants.assert_not_called()
    external_locations.resolve_mount.assert_not_called()


def test_table_migration_status_owner() -> None:
    admin_locator = create_autospec(AdministratorLocator)

    tables_crawler = create_autospec(TablesCrawler)
    the_table = Table(
        catalog="hive_metastore",
        database="foo",
        name="bar",
        object_type="TABLE",
        table_format="DELTA",
        location="/some/path",
    )
    tables_crawler.snapshot.return_value = [the_table]
    table_ownership = create_autospec(TableOwnership)
    table_ownership._administrator_locator = admin_locator  # pylint: disable=protected-access
    table_ownership.owner_of.return_value = "bob"

    ownership = TableMigrationOwnership(tables_crawler, table_ownership)
    owner = ownership.owner_of(
        TableMigrationStatus(
            src_schema="foo",
            src_table="bar",
            dst_catalog="main",
            dst_schema="foo",
            dst_table="bar",
        )
    )

    assert owner == "bob"
    tables_crawler.snapshot.assert_called_once()
    table_ownership.owner_of.assert_called_once_with(the_table)
    admin_locator.get_workspace_administrator.assert_not_called()


def test_table_migration_status_owner_caches_tables_snapshot() -> None:
    """Verify that the tables inventory isn't loaded until needed, and after that isn't loaded repeatedly."""
    admin_locator = create_autospec(AdministratorLocator)  # pylint: disable=mock-no-usage

    tables_crawler = create_autospec(TablesCrawler)
    a_table = Table(
        catalog="hive_metastore",
        database="foo",
        name="bar",
        object_type="TABLE",
        table_format="DELTA",
        location="/some/path",
    )
    b_table = Table(
        catalog="hive_metastore",
        database="baz",
        name="daz",
        object_type="TABLE",
        table_format="DELTA",
        location="/some/path",
    )
    tables_crawler.snapshot.return_value = [a_table, b_table]
    table_ownership = create_autospec(TableOwnership)
    table_ownership._administrator_locator = admin_locator  # pylint: disable=protected-access
    table_ownership.owner_of.return_value = "bob"

    ownership = TableMigrationOwnership(tables_crawler, table_ownership)

    # Verify the snapshot() hasn't been loaded yet: it isn't needed.
    tables_crawler.snapshot.assert_not_called()

    _ = ownership.owner_of(
        TableMigrationStatus(src_schema="foo", src_table="bar", dst_catalog="main", dst_schema="foo", dst_table="bar"),
    )
    _ = ownership.owner_of(
        TableMigrationStatus(src_schema="baz", src_table="daz", dst_catalog="main", dst_schema="foo", dst_table="bar"),
    )

    # Verify the snapshot() wasn't reloaded for the second .owner_of() call.
    tables_crawler.snapshot.assert_called_once()


def test_table_migration_status_source_table_unknown() -> None:
    admin_locator = create_autospec(AdministratorLocator)
    admin_locator.get_workspace_administrator.return_value = "an_admin"

    tables_crawler = create_autospec(TablesCrawler)
    tables_crawler.snapshot.return_value = []
    table_ownership = create_autospec(TableOwnership)
    table_ownership._administrator_locator = admin_locator  # pylint: disable=protected-access

    ownership = TableMigrationOwnership(tables_crawler, table_ownership)

    unknown_table = TableMigrationStatus(
        src_schema="foo",
        src_table="bar",
        dst_catalog="main",
        dst_schema="foo",
        dst_table="bar",
    )
    owner = ownership.owner_of(unknown_table)

    assert owner == "an_admin"
    table_ownership.owner_of.assert_not_called()


@pytest.mark.parametrize(
    "table_migration_status_record,history_record",
    (
        (
            TableMigrationStatus(
                src_schema="foo",
                src_table="bar",
                dst_catalog="main",
                dst_schema="fu",
                dst_table="baz",
                update_ts="2024-10-18T16:34:00Z",
            ),
            Row(
                workspace_id=2,
                job_run_id=1,
                object_type="TableMigrationStatus",
                object_id=["foo", "bar"],
                data={
                    "src_schema": "foo",
                    "src_table": "bar",
                    "dst_catalog": "main",
                    "dst_schema": "fu",
                    "dst_table": "baz",
                    "update_ts": "2024-10-18T16:34:00Z",
                },
                failures=[],
                owner="the_admin",
                ucx_version=ucx_version,
            ),
        ),
        (
            TableMigrationStatus(
                src_schema="foo",
                src_table="bar",
            ),
            Row(
                workspace_id=2,
                job_run_id=1,
                object_type="TableMigrationStatus",
                object_id=["foo", "bar"],
                data={
                    "src_schema": "foo",
                    "src_table": "bar",
                },
                failures=[],
                owner="the_admin",
                ucx_version=ucx_version,
            ),
        ),
    ),
)
def test_table_migration_status_supports_history(
    mock_backend,
    table_migration_status_record: TableMigrationStatus,
    history_record: Row,
) -> None:
    """Verify that TableMigrationStatus records are written as expected to the history log."""
    table_migration_ownership = create_autospec(TableMigrationOwnership)
    table_migration_ownership.owner_of.return_value = "the_admin"
    history_log = ProgressEncoder[TableMigrationStatus](
        mock_backend,
        table_migration_ownership,
        TableMigrationStatus,
        run_id=1,
        workspace_id=2,
        catalog="a_catalog",
    )

    history_log.append_inventory_snapshot([table_migration_status_record])

    rows = mock_backend.rows_written_for("`a_catalog`.`multiworkspace`.`historical`", mode="append")

    assert rows == [history_record]


class MockBackendWithGeneralException(MockBackend):
    """Mock backend that allows raising a general exception.

    Note: we want to raise a Spark AnalysisException, for which we do not have the dependency to raise explicitly.
    """

    @staticmethod
    def _api_error_from_message(error_message: str):  # No return type to avoid mypy complains on different return type
        return Exception(error_message)


def test_migrate_tables_handles_table_with_empty_column(caplog) -> None:
    table_crawler = create_autospec(TablesCrawler)
    table = Table("hive_metastore", "schema", "table", "MANAGED", "DELTA")

    error_message = (
        "INVALID_PARAMETER_VALUE: Invalid input: RPC CreateTable Field managedcatalog.ColumnInfo.name: "
        'At columns.21: name "" is not a valid name`'
    )
    query = f"ALTER TABLE {escape_sql_identifier(table.full_name)} SET TBLPROPERTIES ('upgraded_to' = 'catalog.schema.table');"
    backend = MockBackendWithGeneralException(fails_on_first={query: error_message})

    ws = create_autospec(WorkspaceClient)
    ws.get_workspace_id.return_value = 123456789

    table_mapping = create_autospec(TableMapping)
    rule = Rule("workspace", "catalog", "schema", "schema", "table", "table")
    table_to_migrate = TableToMigrate(table, rule)
    table_mapping.get_tables_to_migrate.return_value = [table_to_migrate]

    migration_status_refresher = create_autospec(TableMigrationStatusRefresher)
    migration_status_refresher.get_seen_tables.return_value = {}
    migration_status_refresher.index.return_value = []

    migrate_grants = create_autospec(MigrateGrants)
    external_locations = create_autospec(ExternalLocations)
    table_migrator = TablesMigrator(
        table_crawler,
        ws,
        backend,
        table_mapping,
        migration_status_refresher,
        migrate_grants,
        external_locations,
    )

    with caplog.at_level(logging.WARN, logger="databricks.labs.ucx.hive_metastore"):
        table_migrator.migrate_tables(table.what)
    assert "failed-to-migrate: Table with empty column name 'hive_metastore.schema.table'" in caplog.messages

    table_crawler.snapshot.assert_not_called()  # Mocking table mapping instead
    ws.get_workspace_id.assert_not_called()  # Errors before getting here
    migration_status_refresher.index.assert_not_called()  # Only called when migrating view
    migrate_grants.apply.assert_not_called()  # Errors before getting here
    external_locations.resolve_mount.assert_not_called()  # Only called when migrating external table

import logging
from unittest.mock import MagicMock, create_autospec

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import CatalogInfo, SchemaInfo, TableInfo

from databricks.labs.ucx.hive_metastore.tables import (
    MigrationCount,
    Table,
    TablesCrawler,
    TablesMigrate,
)

from ..framework.mocks import MockBackend

logger = logging.getLogger(__name__)


def test_migrate_managed_tables_should_produce_proper_queries():
    errors = {}
    rows = {
        "SELECT": [
            (
                "hive_metastore",
                "db1",
                "managed",
                "MANAGED",
                "DELTA",
                None,
                None,
            ),
        ]
    }
    backend = MockBackend(fails_on_first=errors, rows=rows)
    tc = TablesCrawler(backend, "inventory_database")
    client = MagicMock()
    tm = TablesMigrate(tc, client, backend)
    tm.migrate_tables()

    assert (list(backend.queries)) == [
        "SELECT * FROM hive_metastore.inventory_database.tables",
        "CREATE TABLE IF NOT EXISTS ucx_default.db1.managed DEEP CLONE hive_metastore.db1.managed;",
        "ALTER TABLE hive_metastore.db1.managed SET TBLPROPERTIES ('upgraded_to' = 'ucx_default.db1.managed');",
        "ALTER TABLE ucx_default.db1.managed SET TBLPROPERTIES ('upgraded_from' = 'hive_metastore.db1.managed');",
    ]


def test_migrate_managed_tables_should_do_nothing_if_upgrade_tag_is_present():
    errors = {}
    rows = {
        "SELECT": [
            ("hive_metastore", "db1", "managed", "MANAGED", "DELTA", None, None),
        ]
    }
    backend = MockBackend(fails_on_first=errors, rows=rows)
    tc = TablesCrawler(backend, "inventory_database")
    client = MagicMock()
    client.catalogs.list.return_value = [CatalogInfo(name="catalog_1")]
    client.schemas.list.return_value = [SchemaInfo(name="db1")]
    client.tables.list.return_value = [
        TableInfo(full_name="catalog_1.db1.managed", properties={"upgraded_from": "hive_metastore.db1.managed"})
    ]
    tm = TablesMigrate(tc, client, backend, default_catalog="catalog_1")
    tm.migrate_tables()

    assert (list(backend.queries)) == ["SELECT * FROM hive_metastore.inventory_database.tables"]


def test_migrate_tables_should_migrate_tables_to_default_catalog_if_not_found_in_mapping():
    errors = {}
    rows = {
        "SELECT": [
            ("hive_metastore", "db1", "managed", "MANAGED", "DELTA", None, None),
        ]
    }
    backend = MockBackend(fails_on_first=errors, rows=rows)
    tc = TablesCrawler(backend, "inventory_database")
    client = MagicMock()
    database_to_catalog_mapping = {"db1": "catalog_1", "db2": "catalog_2"}
    tm = TablesMigrate(tc, client, backend, database_to_catalog_mapping=database_to_catalog_mapping)
    tm.migrate_tables()

    assert (list(backend.queries)) == [
        "SELECT * FROM hive_metastore.inventory_database.tables",
        "CREATE TABLE IF NOT EXISTS catalog_1.db1.managed DEEP CLONE hive_metastore.db1.managed;",
        "ALTER TABLE hive_metastore.db1.managed SET TBLPROPERTIES ('upgraded_to' = 'catalog_1.db1.managed');",
        "ALTER TABLE catalog_1.db1.managed SET TBLPROPERTIES ('upgraded_from' = 'hive_metastore.db1.managed');",
    ]


def test_migrate_tables_should_migrate_tables_to_default_catalog_if_specified():
    errors = {}
    rows = {
        "SELECT": [
            ("hive_metastore", "db1", "managed", "MANAGED", "DELTA", None, None),
        ]
    }
    backend = MockBackend(fails_on_first=errors, rows=rows)
    tc = TablesCrawler(backend, "inventory_database")
    client = MagicMock()
    tm = TablesMigrate(tc, client, backend, default_catalog="test_catalog")
    tm.migrate_tables()

    assert (list(backend.queries)) == [
        "SELECT * FROM hive_metastore.inventory_database.tables",
        "CREATE TABLE IF NOT EXISTS test_catalog.db1.managed DEEP CLONE hive_metastore.db1.managed;",
        "ALTER TABLE hive_metastore.db1.managed SET TBLPROPERTIES ('upgraded_to' = 'test_catalog.db1.managed');",
        "ALTER TABLE test_catalog.db1.managed SET TBLPROPERTIES ('upgraded_from' = 'hive_metastore.db1.managed');",
    ]


def test_migrate_tables_should_add_table_to_cache_when_migrated():
    errors = {}
    rows = {
        "SELECT": [
            ("hive_metastore", "db1", "managed", "MANAGED", "DELTA", None, None),
        ]
    }
    backend = MockBackend(fails_on_first=errors, rows=rows)
    tc = TablesCrawler(backend, "inventory_database")
    client = MagicMock()
    tm = TablesMigrate(tc, client, backend, default_catalog="test_catalog")
    tm.migrate_tables()

    assert tm._seen_tables == {"test_catalog.db1.managed": "hive_metastore.db1.managed"}


def test_revert_migrated_tables():
    errors = {}
    rows = {
        "SELECT": [
            ("hive_metastore", "db1", "managed", "MANAGED", "DELTA", None, None),
        ]
    }
    backend = MockBackend(fails_on_first=errors, rows=rows)
    tc = create_autospec(TablesCrawler)
    client = create_autospec(WorkspaceClient)
    tm = TablesMigrate(tc, client, backend, default_catalog="test_catalog")
    test_tables = [
        Table(
            object_type="EXTERNAL",
            table_format="DELTA",
            catalog="hive_metastore",
            database="test_schema1",
            name="test_table1",
            upgraded_to="cat1.schema1.dest1",
        ),
        Table(
            object_type="VIEW",
            table_format="VIEW",
            catalog="hive_metastore",
            database="test_schema1",
            name="test_view1",
            view_text="SELECT * FROM SOMETHING",
            upgraded_to="cat1.schema1.dest_view1",
        ),
        Table(
            object_type="MANAGED",
            table_format="DELTA",
            catalog="hive_metastore",
            database="test_schema1",
            name="test_table2",
            upgraded_to="cat1.schema1.dest2",
        ),
        Table(
            object_type="EXTERNAL",
            table_format="DELTA",
            catalog="hive_metastore",
            database="test_schema2",
            name="test_table3",
            upgraded_to="cat1.schema2.dest3",
        ),
    ]

    tc.snapshot.return_value = test_tables
    tm._seen_tables = {
        "cat1.schema1.dest1": "hive_metastore.test_schema1.test_table1",
        "cat1.schema1.dest_view1": "hive_metastore.test_schema1.test_view1",
        "cat1.schema1.dest2": "hive_metastore.test_schema1.test_table2",
        "cat1.schema2.dest3": "hive_metastore.test_schema2.test_table2",
    }
    tm.revert_migrated_tables(schema="test_schema1")
    revert_queries = list(backend.queries)
    assert (
        "ALTER TABLE `hive_metastore`.`test_schema1`.`test_table1` UNSET TBLPROPERTIES IF EXISTS('upgraded_to');"
        in revert_queries
    )
    assert "DROP TABLE IF EXISTS cat1.schema1.dest1" in revert_queries
    assert (
        "ALTER VIEW `hive_metastore`.`test_schema1`.`test_view1` UNSET TBLPROPERTIES IF EXISTS('upgraded_to');"
        in revert_queries
    )
    assert "DROP VIEW IF EXISTS cat1.schema1.dest_view1" in revert_queries

    # testing reverting managed tables
    tm.revert_migrated_tables(schema="test_schema1", delete_managed=True)
    revert_with_managed_queries = list(backend.queries)[-6:]
    assert (
        "ALTER TABLE `hive_metastore`.`test_schema1`.`test_table1` UNSET TBLPROPERTIES IF EXISTS('upgraded_to');"
        in revert_with_managed_queries
    )
    assert "DROP TABLE IF EXISTS cat1.schema1.dest1" in revert_with_managed_queries
    assert (
        "ALTER VIEW `hive_metastore`.`test_schema1`.`test_view1` UNSET TBLPROPERTIES IF EXISTS('upgraded_to');"
        in revert_with_managed_queries
    )
    assert "DROP VIEW IF EXISTS cat1.schema1.dest_view1" in revert_with_managed_queries
    assert (
        "ALTER TABLE `hive_metastore`.`test_schema1`.`test_table2` UNSET TBLPROPERTIES IF EXISTS('upgraded_to');"
        in revert_with_managed_queries
    )
    assert "DROP TABLE IF EXISTS cat1.schema1.dest2" in revert_with_managed_queries


def test_get_migrated_count():
    errors = {}
    rows = {
        "SELECT": [
            ("hive_metastore", "db1", "managed", "MANAGED", "DELTA", None, None),
        ]
    }
    backend = MockBackend(fails_on_first=errors, rows=rows)
    tc = create_autospec(TablesCrawler)
    client = create_autospec(WorkspaceClient)
    tm = TablesMigrate(tc, client, backend, default_catalog="test_catalog")
    test_tables = [
        Table(
            object_type="EXTERNAL",
            table_format="DELTA",
            catalog="hive_metastore",
            database="test_schema1",
            name="test_table1",
            upgraded_to="cat1.schema1.dest1",
        ),
        Table(
            object_type="MANAGED",
            table_format="DELTA",
            catalog="hive_metastore",
            database="test_schema1",
            name="test_table2",
            upgraded_to="dest1",
        ),
        Table(
            object_type="EXTERNAL",
            table_format="DELTA",
            catalog="hive_metastore",
            database="test_schema2",
            name="test_table3",
            upgraded_to="cat1.schema2.dest3",
        ),
        Table(
            object_type="VIEW",
            table_format="VIEW",
            view_text="SELECT * FROM SOMETHING",
            catalog="hive_metastore",
            database="test_schema1",
            name="test_view1",
            upgraded_to="cat1.schema1.dest_view1",
        ),
    ]

    tc.snapshot.return_value = test_tables
    tm._seen_tables = {
        "cat1.schema1.dest1": "hive_metastore.test_schema1.test_table1",
        "cat1.schema1.dest2": "hive_metastore.test_schema1.test_table2",
        "cat1.schema1.dest_view1": "hive_metastore.test_schema1.test_view1",
        "cat1.schema2.dest3": "hive_metastore.test_schema2.test_table3",
    }
    migrated_count = tm._get_revert_count()
    assert MigrationCount("test_schema1", 1, 1, 1) in migrated_count
    assert MigrationCount("test_schema2", 0, 1, 0) in migrated_count


def test_revert_report(capsys):
    errors = {}
    rows = {
        "SELECT": [
            ("hive_metastore", "db1", "managed", "MANAGED", "DELTA", None, None),
        ]
    }
    backend = MockBackend(fails_on_first=errors, rows=rows)
    tc = create_autospec(TablesCrawler)
    client = create_autospec(WorkspaceClient)
    tm = TablesMigrate(tc, client, backend, default_catalog="test_catalog")
    tm._get_revert_count = MagicMock()
    tm._get_revert_count.return_value = [
        MigrationCount("test_schema1", 1, 1, 1),
        MigrationCount("test_schema2", 0, 1, 0),
    ]

    tm.print_revert_report(delete_managed=True)
    captured = capsys.readouterr()
    assert "test_schema1|1|1|1" in captured.out.replace(" ", "")
    assert "test_schema2|1|0|0" in captured.out.replace(" ", "")

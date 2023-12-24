import logging
from unittest.mock import MagicMock, create_autospec

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import CatalogInfo, SchemaInfo, TableInfo

from databricks.labs.ucx.hive_metastore.tables import (
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


def test_tables_sql_unset_to():
    table = Table(
        object_type="TABLE",
        table_format="DELTA",
        catalog="hive_metastore",
        database="test_schema1",
        name="test_table1",
        upgraded_to="dest1",
    )
    assert table.sql_unset_to("hive_metastore") == (
        "ALTER TABLE `hive_metastore`.`test_schema1`.`test_table1` UNSET TBLPROPERTIES IF EXISTS('upgraded_to');"
    )


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
            object_type="TABLE",
            table_format="DELTA",
            catalog="hive_metastore",
            database="test_schema1",
            name="test_table1",
            upgraded_to="dest1",
        ),
        Table(
            object_type="TABLE",
            table_format="DELTA",
            catalog="hive_metastore",
            database="test_schema1",
            name="test_table2",
            upgraded_to="dest1",
        ),
        Table(
            object_type="TABLE",
            table_format="DELTA",
            catalog="hive_metastore",
            database="test_schema2",
            name="test_table3",
            upgraded_to="dest1",
        ),
    ]

    tc.snapshot.return_value = test_tables
    tm.revert_migrated_tables(schema="test_schema1")
    assert (list(backend.queries)) == [
        "ALTER TABLE `hive_metastore`.`test_schema1`.`test_table1` UNSET TBLPROPERTIES IF EXISTS('upgraded_to');",
        "ALTER TABLE `hive_metastore`.`test_schema1`.`test_table2` UNSET TBLPROPERTIES IF EXISTS('upgraded_to');",
    ]
    tc.unset_upgraded_to.assert_called_with(database="test_schema1", name=None)

    tc_test = TablesCrawler(backend, "inventory_database")
    tc_test.unset_upgraded_to(database="test_schema1", name=None)
    assert (
        backend.queries[-1]
        == "UPDATE hive_metastore.inventory_database.tables SET upgraded_to=NULL WHERE database='test_schema1'"
    )

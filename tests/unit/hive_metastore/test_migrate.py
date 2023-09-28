import logging
import pytest
from unittest.mock import MagicMock

from databricks.labs.ucx.hive_metastore.tables import TablesCrawler, TablesMigrate

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
    tm = TablesMigrate(tc, client, backend, "")
    tm.migrate_tables()

    assert (list(backend.queries)) == [
        "SELECT * FROM hive_metastore.inventory_database.tables",
        "CREATE TABLE IF NOT EXISTS ucx_default.db1.managed DEEP CLONE hive_metastore.db1.managed;",
        "ALTER TABLE hive_metastore.db1.managed SET TBLPROPERTIES ('upgraded_to' = 'ucx_default.db1.managed');",
        "ALTER TABLE ucx_default.db1.managed SET TBLPROPERTIES ('upgraded_from' = 'hive_metastore.db1.managed');",
    ]

@pytest.mark.skip(reason="Not implemented yet")
def test_migrate_managed_tables_should_do_nothing_if_upgrade_tag_is_present():
    errors = {}
    rows = {
        "SELECT": [
            ("hive_metastore", "db1", "managed", "MANAGED", "DELTA", None, None, "[upgraded_to=target]"),
        ]
    }
    backend = MockBackend(fails_on_first=errors, rows=rows)
    tc = TablesCrawler(backend, "inventory_database")
    client = MagicMock()
    tm = TablesMigrate(tc, client, backend, "")
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
    tm = TablesMigrate(tc, client, backend, "", database_to_catalog_mapping=database_to_catalog_mapping)
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
    tm = TablesMigrate(tc, client, backend, "", default_catalog="test_catalog")
    tm.migrate_tables()

    assert (list(backend.queries)) == [
        "SELECT * FROM hive_metastore.inventory_database.tables",
        "CREATE TABLE IF NOT EXISTS test_catalog.db1.managed DEEP CLONE hive_metastore.db1.managed;",
        "ALTER TABLE hive_metastore.db1.managed SET TBLPROPERTIES ('upgraded_to' = 'test_catalog.db1.managed');",
        "ALTER TABLE test_catalog.db1.managed SET TBLPROPERTIES ('upgraded_from' = 'hive_metastore.db1.managed');",
    ]

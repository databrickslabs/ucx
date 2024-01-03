import logging
from unittest.mock import MagicMock, create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import CatalogInfo, SchemaInfo, TableInfo

from databricks.labs.ucx.framework.crawlers import SqlBackend
from databricks.labs.ucx.hive_metastore.mapping import TableMapping, Rule
from databricks.labs.ucx.hive_metastore.tables import Table, TablesCrawler, MigrationCount, TablesMigrate

from ..framework.mocks import MockBackend

logger = logging.getLogger(__name__)


def test_migrate_dbfs_root_tables_should_produce_proper_queries():
    errors = {}
    rows = {
        "SELECT": [
            (
                "hive_metastore",
                "db1_src",
                "managed_src",
                "MANAGED",
                "DELTA",
                "dbfs:/table_location/table_name",
                None,
            ),
        ],
        "SHOW TBLPROPERTIES ":[
            {"key": "fake_key", "value": "fake_value"}
        ],
        "CREATE TABLE IF NOT EXISTS":[]

    }
    backend = MockBackend(fails_on_first=errors, rows=rows)
    tc = TablesCrawler(backend, "inventory_database")
    client = MagicMock()
    tmp = create_autospec(TableMapping)
    tmp.load.return_value=[
        Rule("workspace","ucx_default","db1_src","db1_dst","managed_src","managed_dst")
        ]
    tm = TablesMigrate(tc, client, backend, tmp)

    tm.migrate_tables()

    assert (list(backend.queries)) == [
        "SELECT * FROM hive_metastore.inventory_database.tables",
        "SHOW TBLPROPERTIES hive_metastore.db1_src.managed_src",
        "CREATE TABLE IF NOT EXISTS ucx_default.db1_dst.managed_dst DEEP CLONE hive_metastore.db1_src.managed_src;",
        "ALTER TABLE hive_metastore.db1_src.managed_src SET TBLPROPERTIES ('upgraded_to' = 'ucx_default.db1_dst.managed_dst');",
        "ALTER TABLE ucx_default.db1_dst.managed_dst SET TBLPROPERTIES ('upgraded_from' = 'hive_metastore.db1_src.managed_src');",
    ]


def test_migrate_managed_tables_should_do_nothing_if_upgrade_tag_is_present():
    errors = {}
    rows = {
        "SELECT": [
            ("hive_metastore", "db1", "managed", "MANAGED", "DELTA", "dbfs:/location/table", None),
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
    tmp = create_autospec(TableMapping)
    tmp.load.return_value=[
        Rule("workspace","catalog_1","db1","db1","managed","managed")
        ]
    tm = TablesMigrate(tc, client, backend, tmp)
    tm.migrate_tables()

    assert (list(backend.queries)) == ["SELECT * FROM hive_metastore.inventory_database.tables"]


def test_migrate_tables_should_add_table_to_cache_when_migrated():
    errors = {}
    rows = {
        "SELECT": [
            ("hive_metastore", "db1", "managed", "MANAGED", "DELTA", "dbfs:/location/table", None),
        ]
    }
    backend = MockBackend(fails_on_first=errors, rows=rows)
    tc = TablesCrawler(backend, "inventory_database")
    client = MagicMock()
    tmp = create_autospec(TableMapping)
    tmp.load.return_value=[
        Rule("workspace","test_catalog","db1","db1","managed","managed")
        ]
    tm = TablesMigrate(tc, client, backend, tmp)
    tm.migrate_tables()

    assert tm._seen_tables == {"test_catalog.db1.managed": "hive_metastore.db1.managed"}


def get_table_migrate(backend: SqlBackend) -> TablesMigrate:
    tc = create_autospec(TablesCrawler)
    client = create_autospec(WorkspaceClient)
    client.catalogs.list.return_value = [CatalogInfo(name="cat1")]
    client.schemas.list.return_value = [
        SchemaInfo(catalog_name="cat1", name="test_schema1"),
        SchemaInfo(catalog_name="cat1", name="test_schema2"),
    ]
    client.tables.list.side_effect = [
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
    tmp = create_autospec(TableMapping)
    tmp.load.return_value = [
        Rule("workspace", "catalog_1", "db1", "db1", "managed", "managed")
    ]
    tm = TablesMigrate(tc, client, backend, tmp)
    return tm


def test_revert_migrated_tables_skip_managed():
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    tm = get_table_migrate(backend)
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


def test_revert_migrated_tables_including_managed():
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    tm = get_table_migrate(backend)
    # testing reverting managed tables
    tm.revert_migrated_tables(schema="test_schema1", delete_managed=True)
    revert_with_managed_queries = list(backend.queries)
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


def test_get_table_list():
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    tm = get_table_migrate(backend)
    assert len(tm._get_tables_to_revert("test_schema1", "test_table1")) == 1
    assert len(tm._get_tables_to_revert("test_schema1")) == 3


def test_no_migrated_tables():
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    tc = create_autospec(TablesCrawler)
    client = create_autospec(WorkspaceClient)
    client.tables.list.side_effect = []
    tmp = create_autospec(TableMapping)
    tmp.load.return_value = [
        Rule("workspace", "catalog_1", "db1", "db1", "managed", "managed")
    ]
    tm = TablesMigrate(tc, client, backend, tmp)
    tm._init_seen_tables = MagicMock()
    assert len(tm._get_tables_to_revert("test_schema1", "test_table1")) == 0
    tm._init_seen_tables.assert_called()


def test_get_migrated_count():
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    tm = get_table_migrate(backend)
    migrated_count = tm._get_revert_count()
    assert MigrationCount("test_schema1", 1, 1, 1) in migrated_count
    assert MigrationCount("test_schema2", 0, 1, 0) in migrated_count


def test_revert_report(capsys):
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    tm = get_table_migrate(backend)
    tm.print_revert_report(delete_managed=True)
    captured = capsys.readouterr()
    assert "test_schema1|1|1|1" in captured.out.replace(" ", "")
    assert "test_schema2|1|0|0" in captured.out.replace(" ", "")
    assert "Migrated Manged Tables (targets) will be deleted" in captured.out

    tm.print_revert_report(delete_managed=False)
    captured = capsys.readouterr()
    assert "Migrated Manged Tables (targets) will be left intact" in captured.out


def test_empty_revert_report(capsys):
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    tc = create_autospec(TablesCrawler)
    client = create_autospec(WorkspaceClient)
    client.tables.list.side_effect = []
    tmp = create_autospec(TableMapping)
    tmp.load.return_value = [
        Rule("workspace", "catalog_1", "db1", "db1", "managed", "managed")
    ]
    tm = TablesMigrate(tc, client, backend, tmp)
    assert not tm.print_revert_report(delete_managed=False)


def test_is_upgraded():
    errors = {}
    rows = {
        "SHOW TBLPROPERTIES `schema1`.`table1`": [
            {"key": "upgraded_to", "value": "fake_dest"},
        ],
        "SHOW TBLPROPERTIES `schema1`.`table2`": [
            {"key": "another_key", "value": "fake_value"},
        ],
    }
    backend = MockBackend(fails_on_first=errors, rows=rows)
    tc = create_autospec(TablesCrawler)
    client = create_autospec(WorkspaceClient)
    tmp = create_autospec(TableMapping)
    tmp.load.return_value = [
        Rule("workspace", "catalog_1", "db1", "db1", "managed", "managed")
    ]
    tm = TablesMigrate(tc, client, backend, tmp)
    assert tm._is_upgraded("schema1", "table1")
    assert not tm._is_upgraded("schema1", "table2")


@pytest.mark.parametrize(
    "src_table,target,query",
    [
        (
                Table(catalog="hive_metastore", database="db", name="managed_table",location="dbfs:/location/table", object_type="..", table_format="DELTA"),
                "new_catalog.db.managed_table",
                "CREATE TABLE IF NOT EXISTS new_catalog.db.managed_table DEEP CLONE hive_metastore.db.managed_table;",
        ),
        (
                Table(
                    catalog="hive_metastore",
                    database="db",
                    name="view",
                    object_type="..",
                    table_format="DELTA",
                    view_text="SELECT * FROM table",
                ),
                "new_catalog.db.view"
                ,
                "CREATE VIEW IF NOT EXISTS new_catalog.db.view AS SELECT * FROM table;",
        ),
        (
                Table(
                    catalog="hive_metastore",
                    database="db",
                    name="external_table",
                    object_type="EXTERNAL",
                    table_format="DELTA",
                    location="s3a://foo/bar",
                ),
                "new_catalog.db.external_table",
                "SYNC TABLE new_catalog.db.external_table FROM hive_metastore.db.external_table;",
        ),
    ],
)
def test_migrate_query(src_table: Table, target: str, query: str):
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    tc = create_autospec(TablesCrawler)
    client = create_autospec(WorkspaceClient)
    tmp = create_autospec(TableMapping)
    tmp.load.return_value = [
    ]
    tm = TablesMigrate(tc, client, backend, tmp)
    tm._migrate_table(src_table,target)
    assert query in backend.queries

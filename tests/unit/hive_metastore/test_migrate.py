import logging
from unittest.mock import MagicMock, create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import (
    CatalogInfo,
    PermissionsList,
    Privilege,
    PrivilegeAssignment,
    SchemaInfo,
    TableInfo,
    TableType,
)

from databricks.labs.ucx.framework.crawlers import SqlBackend
from databricks.labs.ucx.framework.parallel import ManyError
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
    tm = TablesMigrate(tc, client, backend, default_catalog="test_catalog")
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
    tm = TablesMigrate(tc, client, backend, default_catalog="test_catalog")
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
    tm = TablesMigrate(tc, client, backend, default_catalog="test_catalog")
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
    tm = TablesMigrate(tc, client, backend, default_catalog="test_catalog")
    assert tm.is_upgraded("schema1", "table1")
    assert not tm.is_upgraded("schema1", "table2")


def test_migrate_uc_tables_invalid_from_schema(caplog):
    tc = create_autospec(TablesCrawler)
    client = create_autospec(WorkspaceClient)
    errors = {}
    rows = {
        "SYSTEM.INFORMATION_SCHEMA.SCHEMATA": [
            ("0"),
        ]
    }
    backend = MockBackend(fails_on_first=errors, rows=rows)
    tm = TablesMigrate(tc, client, backend)
    with pytest.raises(ManyError):
        tm.migrate_uc_tables(
            from_catalog="SrcC", from_schema="SrcS", from_table=["*"], to_catalog="TgtC", to_schema="TgtS"
        )


def test_migrate_uc_tables_invalid_to_schema(caplog):
    tc = create_autospec(TablesCrawler)
    client = create_autospec(WorkspaceClient)
    errors = {}
    rows = {
        "SYSTEM.INFORMATION_SCHEMA.SCHEMATA WHERE CATALOG_NAME = 'SrcC' AND SCHEMA_NAME = 'SrcS'": [
            ("1"),
        ],
        "SYSTEM.INFORMATION_SCHEMA.SCHEMATA WHERE CATALOG_NAME = 'TgtC' AND SCHEMA_NAME = 'TgtS'": [
            ("0"),
        ],
    }
    backend = MockBackend(fails_on_first=errors, rows=rows)
    tm = TablesMigrate(tc, client, backend)
    tm.migrate_uc_tables(from_catalog="SrcC", from_schema="SrcS", from_table=["*"], to_catalog="TgtC", to_schema="TgtS")
    assert len([rec.message for rec in caplog.records if "schema TgtS not found in TgtC" in rec.message]) == 1


def test_migrate_uc_tables(caplog):
    caplog.set_level(logging.INFO)
    tc = create_autospec(TablesCrawler)
    client = create_autospec(WorkspaceClient)
    errors = {}
    rows = {
        "SYSTEM.INFORMATION_SCHEMA.SCHEMATA WHERE CATALOG_NAME = 'SrcC' AND SCHEMA_NAME = 'SrcS'": [
            ("1"),
        ],
        "SYSTEM.INFORMATION_SCHEMA.SCHEMATA WHERE CATALOG_NAME = 'TgtC' AND SCHEMA_NAME = 'TgtS'": [
            ("1"),
        ],
        "SYSTEM.INFORMATION_SCHEMA.TABLES WHERE CATALOG_NAME = 'TgtC' AND SCHEMA_NAME = 'TgtS' AND "
        "TABLE_NAME = 'table1'": [
            ("0"),
        ],
        "SYSTEM.INFORMATION_SCHEMA.TABLES WHERE CATALOG_NAME = 'TgtC' AND SCHEMA_NAME = 'TgtS' AND "
        "TABLE_NAME = 'table2'": [
            ("1"),
        ],
        "SYSTEM.INFORMATION_SCHEMA.TABLES WHERE CATALOG_NAME = 'TgtC' AND SCHEMA_NAME = 'TgtS' AND "
        "TABLE_NAME = 'view1'": [
            ("0"),
        ],
        "SHOW CREATE TABLE SrcC.SrcS.table1": [
            ("CREATE TABLE SrcC.SrcS.table1 (name string)"),
        ],
    }
    client.tables.list.return_value = [
        TableInfo(
            catalog_name="SrcC",
            schema_name="SrcS",
            name="table1",
            full_name="SrcC.SrcS.table1",
            table_type=TableType.EXTERNAL,
        ),
        TableInfo(
            catalog_name="SrcC",
            schema_name="SrcS",
            name="table2",
            full_name="SrcC.SrcS.table2",
            table_type=TableType.EXTERNAL,
        ),
        TableInfo(
            catalog_name="SrcC",
            schema_name="SrcS",
            name="view1",
            full_name="SrcC.SrcS.view1",
            table_type=TableType.VIEW,
            view_definition="SELECT * FROM SrcC.SrcS.table1",
        ),
    ]
    client.grants.get.return_value = PermissionsList([PrivilegeAssignment("foo", [Privilege.SELECT])])
    backend = MockBackend(fails_on_first=errors, rows=rows)
    tm = TablesMigrate(tc, client, backend)
    tm.migrate_uc_tables(from_catalog="SrcC", from_schema="SrcS", from_table=["*"], to_catalog="TgtC", to_schema="TgtS")
    log_cnt = 0
    for rec in caplog.records:
        if rec.message in ["migrated 1 tables to the new schema TgtS.", "migrated 1 views to the new schema TgtS."]:
            log_cnt += 1

    assert log_cnt == 2

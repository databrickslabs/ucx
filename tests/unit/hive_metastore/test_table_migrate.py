import logging
from itertools import cycle
from unittest.mock import MagicMock, create_autospec

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import CatalogInfo, SchemaInfo, TableInfo

from databricks.labs.ucx.framework.crawlers import SqlBackend
from databricks.labs.ucx.hive_metastore.mapping import (
    Rule,
    TableMapping,
    TableToMigrate,
)
from databricks.labs.ucx.hive_metastore.table_migrate import TablesMigrate
from databricks.labs.ucx.hive_metastore.tables import (
    MigrationCount,
    Table,
    TablesCrawler,
    What,
)

from ..framework.mocks import MockBackend

logger = logging.getLogger(__name__)


def test_migrate_dbfs_root_tables_should_produce_proper_queries():
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = TablesCrawler(backend, "inventory_database")
    client = MagicMock()
    table_mapping = create_autospec(TableMapping)
    table_mapping.get_tables_to_migrate.return_value = [
        TableToMigrate(
            Table("hive_metastore", "db1_src", "managed_dbfs", "MANAGED", "DELTA", "dbfs:/some_location"),
            Rule("workspace", "ucx_default", "db1_src", "db1_dst", "managed_dbfs", "managed_dbfs"),
        ),
        TableToMigrate(
            Table("hive_metastore", "db1_src", "managed_mnt", "MANAGED", "DELTA", "s3:/mnt/location"),
            Rule("workspace", "ucx_default", "db1_src", "db1_dst", "managed_mnt", "managed_mnt"),
        ),
        TableToMigrate(
            Table("hive_metastore", "db1_src", "managed_other", "MANAGED", "DELTA", "s3:/location"),
            Rule("workspace", "ucx_default", "db1_src", "db1_dst", "managed_other", "managed_other"),
        ),
    ]
    table_migrate = TablesMigrate(table_crawler, client, backend, table_mapping)
    table_migrate.migrate_tables()

    assert (
        "CREATE TABLE IF NOT EXISTS ucx_default.db1_dst.managed_dbfs DEEP CLONE hive_metastore.db1_src.managed_dbfs;"
    ) in list(backend.queries)
    assert "SYNC TABLE ucx_default.db1_dst.managed_mnt FROM hive_metastore.db1_src.managed_mnt;" in list(
        backend.queries
    )
    assert (
        "ALTER TABLE hive_metastore.db1_src.managed_dbfs "
        "SET TBLPROPERTIES ('upgraded_to' = 'ucx_default.db1_dst.managed_dbfs');"
    ) in list(backend.queries)
    assert (
        "ALTER TABLE ucx_default.db1_dst.managed_dbfs "
        "SET TBLPROPERTIES ('upgraded_from' = 'hive_metastore.db1_src.managed_dbfs');"
    ) in list(backend.queries)
    assert "SYNC TABLE ucx_default.db1_dst.managed_other FROM hive_metastore.db1_src.managed_other;" in list(
        backend.queries
    )


def test_migrate_dbfs_root_tables_should_be_skipped_when_upgrading_external():
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = TablesCrawler(backend, "inventory_database")
    client = MagicMock()
    table_mapping = create_autospec(TableMapping)
    table_mapping.get_tables_to_migrate.return_value = [
        TableToMigrate(
            Table("hive_metastore", "db1_src", "managed_dbfs", "MANAGED", "DELTA", "dbfs:/some_location"),
            Rule("workspace", "ucx_default", "db1_src", "db1_dst", "managed_dbfs", "managed_dbfs"),
        ),
    ]
    table_migrate = TablesMigrate(table_crawler, client, backend, table_mapping)
    table_migrate.migrate_tables(what=What.EXTERNAL)

    assert len(backend.queries) == 0


def test_migrate_external_tables_should_produce_proper_queries():
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = TablesCrawler(backend, "inventory_database")
    client = MagicMock()
    table_mapping = create_autospec(TableMapping)
    table_mapping.get_tables_to_migrate.return_value = [
        TableToMigrate(
            Table("hive_metastore", "db1_src", "external_src", "EXTERNAL", "DELTA"),
            Rule("workspace", "ucx_default", "db1_src", "db1_dst", "external_src", "external_dst"),
        )
    ]
    table_migrate = TablesMigrate(table_crawler, client, backend, table_mapping)
    table_migrate.migrate_tables()

    assert (list(backend.queries)) == [
        "SYNC TABLE ucx_default.db1_dst.external_dst FROM hive_metastore.db1_src.external_src;"
    ]


def test_migrate_already_upgraded_table_should_produce_no_queries():
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = TablesCrawler(backend, "inventory_database")
    client = create_autospec(WorkspaceClient)
    client.catalogs.list.return_value = [CatalogInfo(name="cat1")]
    client.schemas.list.return_value = [
        SchemaInfo(catalog_name="cat1", name="test_schema1"),
    ]
    client.tables.list.return_value = [
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
    table_migrate = TablesMigrate(table_crawler, client, backend, table_mapping)
    table_migrate.migrate_tables()

    assert len(backend.queries) == 0


def test_migrate_unsupported_format_table_should_produce_no_queries():
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = TablesCrawler(backend, "inventory_database")
    client = create_autospec(WorkspaceClient)
    table_mapping = create_autospec(TableMapping)
    table_mapping.get_tables_to_migrate.return_value = [
        TableToMigrate(
            Table("hive_metastore", "db1_src", "external_src", "EXTERNAL", "UNSUPPORTED_FORMAT"),
            Rule("workspace", "cat1", "db1_src", "schema1", "external_src", "dest1"),
        )
    ]
    table_migrate = TablesMigrate(table_crawler, client, backend, table_mapping)
    table_migrate.migrate_tables()

    assert len(backend.queries) == 0


def test_migrate_view_should_produce_proper_queries():
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = TablesCrawler(backend, "inventory_database")
    client = MagicMock()
    table_mapping = create_autospec(TableMapping)
    table_mapping.get_tables_to_migrate.return_value = [
        TableToMigrate(
            Table("hive_metastore", "db1_src", "view_src", "VIEW", "VIEW", view_text="SELECT * FROM table"),
            Rule("workspace", "ucx_default", "db1_src", "db1_dst", "view_src", "view_dst"),
        )
    ]
    table_migrate = TablesMigrate(table_crawler, client, backend, table_mapping)
    table_migrate.migrate_tables()

    assert "CREATE VIEW IF NOT EXISTS ucx_default.db1_dst.view_dst AS SELECT * FROM table;" in list(backend.queries)
    assert (
        "ALTER VIEW hive_metastore.db1_src.view_src "
        "SET TBLPROPERTIES ('upgraded_to' = 'ucx_default.db1_dst.view_dst');"
    ) in list(backend.queries)
    assert (
        "ALTER VIEW ucx_default.db1_dst.view_dst "
        "SET TBLPROPERTIES ('upgraded_from' = 'hive_metastore.db1_src.view_src');"
    ) in list(backend.queries)


def get_table_migrate(backend: SqlBackend) -> TablesMigrate:
    table_crawler = create_autospec(TablesCrawler)
    client = create_autospec(WorkspaceClient)
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
    table_crawler.snapshot.return_value = test_tables
    table_mapping = create_autospec(TableMapping)
    table_mapping.load.return_value = []
    table_migrate = TablesMigrate(table_crawler, client, backend, table_mapping)
    return table_migrate


def test_revert_migrated_tables_skip_managed():
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_migrate = get_table_migrate(backend)
    table_migrate.revert_migrated_tables(schema="test_schema1")
    revert_queries = list(backend.queries)
    assert (
        "ALTER TABLE hive_metastore.test_schema1.test_table1 UNSET TBLPROPERTIES IF EXISTS('upgraded_to');"
        in revert_queries
    )
    assert "DROP TABLE IF EXISTS cat1.schema1.dest1" in revert_queries
    assert (
        "ALTER VIEW hive_metastore.test_schema1.test_view1 UNSET TBLPROPERTIES IF EXISTS('upgraded_to');"
        in revert_queries
    )
    assert "DROP VIEW IF EXISTS cat1.schema1.dest_view1" in revert_queries


def test_revert_migrated_tables_including_managed():
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_migrate = get_table_migrate(backend)
    # testing reverting managed tables
    table_migrate.revert_migrated_tables(schema="test_schema1", delete_managed=True)
    revert_with_managed_queries = list(backend.queries)
    assert (
        "ALTER TABLE hive_metastore.test_schema1.test_table1 UNSET TBLPROPERTIES IF EXISTS('upgraded_to');"
        in revert_with_managed_queries
    )
    assert "DROP TABLE IF EXISTS cat1.schema1.dest1" in revert_with_managed_queries
    assert (
        "ALTER VIEW hive_metastore.test_schema1.test_view1 UNSET TBLPROPERTIES IF EXISTS('upgraded_to');"
        in revert_with_managed_queries
    )
    assert "DROP VIEW IF EXISTS cat1.schema1.dest_view1" in revert_with_managed_queries
    assert (
        "ALTER TABLE hive_metastore.test_schema1.test_table2 UNSET TBLPROPERTIES IF EXISTS('upgraded_to');"
        in revert_with_managed_queries
    )
    assert "DROP TABLE IF EXISTS cat1.schema1.dest2" in revert_with_managed_queries


def test_get_table_list():
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_migrate = get_table_migrate(backend)
    table_migrate._init_seen_tables()
    assert len(table_migrate._get_tables_to_revert("test_schema1", "test_table1")) == 1
    assert len(table_migrate._get_tables_to_revert("test_schema1")) == 3


def test_no_migrated_tables():
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = create_autospec(TablesCrawler)
    client = create_autospec(WorkspaceClient)
    client.tables.list.side_effect = []
    table_mapping = create_autospec(TableMapping)
    table_mapping.load.return_value = [
        Rule("workspace", "catalog_1", "db1", "db1", "managed", "managed"),
    ]
    table_migrate = TablesMigrate(table_crawler, client, backend, table_mapping)
    table_migrate.migrate_tables()
    table_migrate._init_seen_tables = MagicMock()
    assert len(table_migrate._get_tables_to_revert("test_schema1", "test_table1")) == 0
    table_migrate.revert_migrated_tables("test_schema1", "test_table1")
    table_migrate._init_seen_tables.assert_called()


def test_get_migrated_count():
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_migrate = get_table_migrate(backend)
    migrated_count = table_migrate._get_revert_count()
    assert MigrationCount("test_schema1", 1, 1, 1) in migrated_count
    assert MigrationCount("test_schema2", 0, 1, 0) in migrated_count


def test_revert_report(capsys):
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_migrate = get_table_migrate(backend)
    table_migrate.print_revert_report(delete_managed=True)
    captured = capsys.readouterr()
    assert "test_schema1|1|1|1" in captured.out.replace(" ", "")
    assert "test_schema2|1|0|0" in captured.out.replace(" ", "")
    assert "Migrated Manged Tables (targets) will be deleted" in captured.out

    table_migrate.print_revert_report(delete_managed=False)
    captured = capsys.readouterr()
    assert "Migrated Manged Tables (targets) will be left intact" in captured.out


def test_empty_revert_report(capsys):
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = create_autospec(TablesCrawler)
    client = create_autospec(WorkspaceClient)
    client.tables.list.side_effect = []
    table_mapping = create_autospec(TableMapping)
    table_mapping.load.return_value = []
    table_migrate = TablesMigrate(table_crawler, client, backend, table_mapping)
    table_migrate.migrate_tables()
    assert not table_migrate.print_revert_report(delete_managed=False)


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
    table_crawler = create_autospec(TablesCrawler)
    client = create_autospec(WorkspaceClient)
    table_mapping = create_autospec(TableMapping)
    table_mapping.load.return_value = []
    table_migrate = TablesMigrate(table_crawler, client, backend, table_mapping)
    table_migrate.migrate_tables()
    assert table_migrate.is_upgraded("schema1", "table1")
    assert not table_migrate.is_upgraded("schema1", "table2")

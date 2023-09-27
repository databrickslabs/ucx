import pytest

from databricks.labs.ucx.config import GroupsConfig, WorkspaceConfig
from databricks.labs.ucx.hive_metastore.tables import Table, TablesCrawler

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
        Table(catalog="catalog", database="db", name="table", object_type="type", table_format="PARQUET")._sql_managed(
            "catalog"
        )


@pytest.mark.parametrize(
    "table,query",
    [
        (
            Table(catalog="catalog", database="db", name="managed_table", object_type="..", table_format="DELTA"),
            "CREATE TABLE IF NOT EXISTS new_catalog.db.managed_table DEEP CLONE catalog.db.managed_table;",
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
            "SYNC TABLE new_catalog.db.external_table FROM catalog.db.external_table;",
        ),
    ],
)
def test_uc_sql(table, query):
    assert table.uc_create_sql("new_catalog") == query


def test_tables_crawler_inventory_table():
    workspace_cfg = WorkspaceConfig(groups=GroupsConfig(auto=True), inventory_database="default")
    tc = TablesCrawler(MockBackend(), workspace_cfg)
    assert tc._table == "tables"


def test_tables_returning_error_when_describing():
    errors = {"DESCRIBE TABLE EXTENDED hive_metastore.database.table1": "error"}
    rows = {
        "SHOW DATABASES": [("database",)],
        "SHOW TABLES FROM hive_metastore.database": [("", "table1", ""), ("", "table2", "")],
        "DESCRIBE TABLE EXTENDED hive_metastore.database.table2": [("Catalog", "catalog", ""), ("Type", "delta", "")],
    }
    backend = MockBackend(fails_on_first=errors, rows=rows)
    workspace_cfg = WorkspaceConfig(groups=GroupsConfig(auto=True), inventory_database="default")
    tc = TablesCrawler(backend, workspace_cfg)
    results = tc._crawl()
    assert len(results) == 1


def test_migrate_tables_should_migrate_tables_to_default_catalog():
    errors = {}
    rows = {
        "SELECT": [
            ("catalog", "database", "managed", "MANAGED", "DELTA"),
        ]
    }
    backend = MockBackend(fails_on_first=errors, rows=rows)
    workspace_cfg = WorkspaceConfig(
        default_catalog="target_catalog", groups=GroupsConfig(auto=True), inventory_database="inventory_database"
    )
    tc = TablesCrawler(backend, workspace_cfg)
    tc.migrate_tables()

    assert (list(backend.queries)) == [
        "SELECT * FROM hive_metastore.inventory_database.tables",
        "CREATE TABLE IF NOT EXISTS target_catalog.database.managed DEEP CLONE catalog.database.managed;",
        "ALTER TABLE catalog.database.managed SET TBLPROPERTIES ('upgraded_to' = 'target_catalog.database.managed');",
    ]


def test_migrate_tables_should_migrate_tables_to_appropriate_catalog_if_config():
    errors = {}
    rows = {
        "SELECT": [
            ("hive_metastore", "db1", "managed_1", "MANAGED", "DELTA"),
            ("hive_metastore", "db2", "managed_2", "MANAGED", "DELTA"),
            ("hive_metastore", "db3", "managed_3", "MANAGED", "DELTA"),
        ]
    }
    backend = MockBackend(fails_on_first=errors, rows=rows)
    database_to_catalog_mapping = {"db1": "catalog_1", "db2": "catalog_2"}
    workspace_cfg = WorkspaceConfig(
        database_to_catalog_mapping=database_to_catalog_mapping,
        groups=GroupsConfig(auto=True),
        inventory_database="inventory_database",
    )

    tc = TablesCrawler(backend, workspace_cfg)
    tc.migrate_tables()

    assert (list(backend.queries)) == [
        "SELECT * FROM hive_metastore.inventory_database.tables",
        "CREATE TABLE IF NOT EXISTS catalog_1.db1.managed_1 DEEP CLONE hive_metastore.db1.managed_1;",
        "ALTER TABLE hive_metastore.db1.managed_1 SET TBLPROPERTIES ('upgraded_to' = 'catalog_1.db1.managed_1');",
        "CREATE TABLE IF NOT EXISTS catalog_2.db2.managed_2 DEEP CLONE hive_metastore.db2.managed_2;",
        "ALTER TABLE hive_metastore.db2.managed_2 SET TBLPROPERTIES ('upgraded_to' = 'catalog_2.db2.managed_2');",
        "CREATE TABLE IF NOT EXISTS ucx_default.db3.managed_3 DEEP CLONE hive_metastore.db3.managed_3;",
        "ALTER TABLE hive_metastore.db3.managed_3 SET TBLPROPERTIES ('upgraded_to' = 'ucx_default.db3.managed_3');",
    ]


def test_migrate_tables_should_migrate_tables_to_default_catalog_if_not_found_in_mapping():
    errors = {}
    rows = {
        "SELECT": [
            ("hive_metastore", "db3", "managed_3", "MANAGED", "DELTA"),
        ]
    }
    backend = MockBackend(fails_on_first=errors, rows=rows)
    database_to_catalog_mapping = {"db1": "catalog_1", "db2": "catalog_2"}
    workspace_cfg = WorkspaceConfig(
        database_to_catalog_mapping=database_to_catalog_mapping,
        groups=GroupsConfig(auto=True),
        inventory_database="inventory_database",
    )

    tc = TablesCrawler(backend, workspace_cfg)
    tc.migrate_tables()

    assert (list(backend.queries)) == [
        "SELECT * FROM hive_metastore.inventory_database.tables",
        "CREATE TABLE IF NOT EXISTS ucx_default.db3.managed_3 DEEP CLONE hive_metastore.db3.managed_3;",
        "ALTER TABLE hive_metastore.db3.managed_3 SET TBLPROPERTIES ('upgraded_to' = 'ucx_default.db3.managed_3');",
    ]


def test_migrate_external_tables_should_have_appropriate_sql():
    errors = {}
    rows = {
        "SELECT": [
            ("hive_metastore", "db1", "extt", "EXTERNAL", "DELTA"),
        ],
        "SYNC": [
            ("ucx_default", "db1", "extt", "hive_metastore", "db1", "extt", "SUCCESS", ""),
        ],
    }
    backend = MockBackend(fails_on_first=errors, rows=rows)
    workspace_cfg = WorkspaceConfig(
        default_catalog="target_catalog", groups=GroupsConfig(auto=True), inventory_database="inventory_database"
    )
    tc = TablesCrawler(backend, workspace_cfg)
    tc.migrate_tables()

    assert (list(backend.queries)) == [
        "SELECT * FROM hive_metastore.inventory_database.tables",
        "SYNC TABLE target_catalog.db1.extt FROM hive_metastore.db1.extt;",
        "ALTER TABLE hive_metastore.db1.extt SET TBLPROPERTIES ('upgraded_to' = 'target_catalog.db1.extt');",
    ]


def test_migrate_views_should_have_appropriate_sql():
    errors = {}
    rows = {
        "SELECT": [
            ("hive_metastore", "db1", "view_1", "VIEW", "DELTA", "", "SELECT 1+1"),
        ],
    }
    backend = MockBackend(fails_on_first=errors, rows=rows)
    workspace_cfg = WorkspaceConfig(
        default_catalog="target_catalog", groups=GroupsConfig(auto=True), inventory_database="inventory_database"
    )
    tc = TablesCrawler(backend, workspace_cfg)
    tc.migrate_tables()

    assert (list(backend.queries)) == [
        "SELECT * FROM hive_metastore.inventory_database.tables",
        "CREATE VIEW IF NOT EXISTS target_catalog.db1.view_1 AS SELECT 1+1;",
        "ALTER VIEW hive_metastore.db1.view_1 SET TBLPROPERTIES ('upgraded_to' = 'target_catalog.db1.view_1');",
    ]


def test_external_tables_returning_error_should_be_catched_and_logged():
    errors = {}
    rows = {
        "SELECT": [
            ("catalog", "database", "external_table", "EXTERNAL", "DELTA"),
        ],
        "SYNC": [
            ("catalog", "database", "external_table", "target_catalog", "target_schema", "target_table", "ERROR", ""),
        ],
    }
    backend = MockBackend(fails_on_first=errors, rows=rows)
    workspace_cfg = WorkspaceConfig(
        default_catalog="target_catalog", groups=GroupsConfig(auto=True), inventory_database="inventory_database"
    )
    tc = TablesCrawler(backend, workspace_cfg)
    tc.migrate_tables()

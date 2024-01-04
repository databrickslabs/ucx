import io
from unittest.mock import MagicMock, create_autospec

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import CatalogInfo, SchemaInfo, TableInfo

from databricks.labs.ucx.account import WorkspaceInfo
from databricks.labs.ucx.hive_metastore.mapping import Rule, TableMapping
from databricks.labs.ucx.hive_metastore.tables import Table, TablesCrawler
from tests.unit.framework.mocks import MockBackend


def test_current_tables_empty_fails():
    ws = MagicMock()
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_mapping = TableMapping(ws, backend, "~/.ucx")

    tables_crawler = create_autospec(TablesCrawler)
    tables_crawler.snapshot.return_value = []

    with pytest.raises(ValueError):
        list(table_mapping.current_tables(tables_crawler, "a", "b"))


def test_current_tables_some_rules():
    ws = MagicMock()
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_mapping = TableMapping(ws, backend, "~/.ucx")

    tables_crawler = create_autospec(TablesCrawler)
    tables_crawler.snapshot.return_value = [
        Table(
            catalog="hive_metastore",
            object_type="TABLE",
            table_format="DELTA",
            database="foo",
            name="bar",
        )
    ]

    rule = next(table_mapping.current_tables(tables_crawler, "a", "b"))

    assert rule == Rule(
        workspace_name="a", catalog_name="b", src_schema="foo", dst_schema="foo", src_table="bar", dst_table="bar"
    )
    assert rule.as_uc_table_key == "b.foo.bar"
    assert rule.as_hms_table_key == "hive_metastore.foo.bar"


def test_save_mapping():
    ws = MagicMock()
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_mapping = TableMapping(ws, backend, "~/.ucx")

    tables_crawler = create_autospec(TablesCrawler)
    tables_crawler.snapshot.return_value = [
        Table(
            catalog="hive_metastore",
            object_type="TABLE",
            table_format="DELTA",
            database="foo",
            name="bar",
        )
    ]

    workspace_info = create_autospec(WorkspaceInfo)
    workspace_info.current.return_value = "foo-bar"

    table_mapping.save(tables_crawler, workspace_info)

    (path, content), _ = ws.workspace.upload.call_args
    assert "~/.ucx/mapping.csv" == path
    assert (
        "workspace_name,catalog_name,src_schema,dst_schema,src_table,dst_table\r\n"
        "foo-bar,foo_bar,foo,foo,bar,bar\r\n"
    ) == content.read()


def test_load_mapping_not_found():
    ws = MagicMock()
    ws.workspace.download.side_effect = NotFound(...)
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_mapping = TableMapping(ws, backend, "~/.ucx")

    with pytest.raises(ValueError):
        table_mapping.load()


def test_load_mapping():
    ws = MagicMock()
    ws.workspace.download.return_value = io.StringIO(
        "workspace_name,catalog_name,src_schema,dst_schema,src_table,dst_table\r\n"
        "foo-bar,foo_bar,foo,foo,bar,bar\r\n"
    )
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_mapping = TableMapping(ws, backend, "~/.ucx")

    rules = table_mapping.load()

    assert [
        Rule(
            workspace_name="foo-bar",
            catalog_name="foo_bar",
            src_schema="foo",
            dst_schema="foo",
            src_table="bar",
            dst_table="bar",
        )
    ] == rules


def test_skip_happy_path(mocker, caplog):
    ws = mocker.patch("databricks.sdk.WorkspaceClient.__init__")
    sbe = mocker.patch("databricks.labs.ucx.framework.crawlers.StatementExecutionBackend.__init__")
    mapping = TableMapping(ws, sbe)
    mapping.skip_table(schema="schema", table="table")
    sbe.execute.assert_called_with(
        f"ALTER TABLE `schema`.`table` SET TBLPROPERTIES('{mapping.UCX_SKIP_PROPERTY}' = true)"
    )
    assert len(caplog.records) == 0
    mapping.skip_schema(schema="schema")
    sbe.execute.assert_called_with(f"ALTER SCHEMA `schema` SET DBPROPERTIES('{mapping.UCX_SKIP_PROPERTY}' = true)")
    assert len(caplog.records) == 0


def test_skip_missing_schema(mocker, caplog):
    ws = mocker.patch("databricks.sdk.WorkspaceClient.__init__")
    sbe = mocker.patch("databricks.labs.ucx.framework.crawlers.StatementExecutionBackend.__init__")
    sbe.execute.side_effect = NotFound("[SCHEMA_NOT_FOUND]")
    mapping = TableMapping(ws, sbe)
    mapping.skip_schema(schema="schema")
    assert [rec.message for rec in caplog.records if "schema not found" in rec.message.lower()]


def test_skip_missing_table(mocker, caplog):
    ws = mocker.patch("databricks.sdk.WorkspaceClient.__init__")
    sbe = mocker.patch("databricks.labs.ucx.framework.crawlers.StatementExecutionBackend.__init__")
    sbe.execute.side_effect = NotFound("[TABLE_OR_VIEW_NOT_FOUND]")
    mapping = TableMapping(ws, sbe)
    mapping.skip_table(sbe, table="table")
    assert [rec.message for rec in caplog.records if "table not found" in rec.message.lower()]


def test_extract_database_skip_property():
    errors = {}
    rows = {}
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = TablesCrawler(backend, "ucx")
    assert "databricks.labs.ucx.skip" in table_crawler.parse_database_props("(databricks.labs.ucx.skip,true)")


def test_skip_tables_marked_for_skipping_or_upgraded():
    errors = {}
    rows = {
        "SHOW DATABASES": [
            ["test_schema1"],
            ["test_schema2"],
            ["test_schema3"],
        ],
        "SHOW TBLPROPERTIES `test_schema1`.`test_table1`": [
            {"key": "upgraded_to", "value": "fake_dest"},
        ],
        "SHOW TBLPROPERTIES `test_schema1`.`test_view1`": [
            {"key": "databricks.labs.ucx.skip", "value": "true"},
        ],
        "DESCRIBE SCHEMA EXTENDED test_schema1": [],
        "DESCRIBE SCHEMA EXTENDED test_schema2": [],
        "DESCRIBE SCHEMA EXTENDED test_schema3": [
            {
                "database_description_item": "Properties",
                "database_description_value": "((databricks.labs.ucx.skip,true))",
            },
        ],
    }
    backend = MockBackend(fails_on_first=errors, rows=rows)
    table_crawler = create_autospec(TablesCrawler)
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
                full_name="cat1.schema1.test_table1",
                properties={"upgraded_from": "hive_metastore.test_schema1.test_table1"},
            ),
        ],
        [],
        [],
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
        ),
        Table(
            object_type="EXTERNAL",
            table_format="DELTA",
            catalog="hive_metastore",
            database="test_schema2",
            name="test_table3",
        ),
        Table(
            object_type="EXTERNAL",
            table_format="DELTA",
            catalog="hive_metastore",
            database="test_schema3",
            name="test_table4",
        ),
    ]
    client.workspace.download.return_value = io.StringIO(
        "workspace_name,catalog_name,src_schema,dst_schema,src_table,dst_table\r\n"
        "foo-bar,cat1,test_schema1,schema1,test_table1,test_table1\r\n"
        "foo-bar,cat1,test_schema1,schema1,test_view1,test_view1\r\n"
        "foo-bar,cat1,test_schema1,schema1,test_table2,test_table2\r\n"
        "foo-bar,cat1,test_schema2,schema2,test_table3,test_table3\r\n"
        "foo-bar,cat1,test_schema3,schema3,test_table4,test_table4\r\n"
    )
    table_crawler.snapshot.return_value = test_tables
    table_mapping = TableMapping(client, backend)

    tables_to_migrate = table_mapping.get_tables_to_migrate(table_crawler)
    assert len(tables_to_migrate) == 2
    tables = (table_to_migrate.src for table_to_migrate in tables_to_migrate)
    assert (
        Table(
            object_type="EXTERNAL",
            table_format="DELTA",
            catalog="hive_metastore",
            database="test_schema3",
            name="test_table4",
        )
        not in tables
    )
    assert (table for table in tables_to_migrate)

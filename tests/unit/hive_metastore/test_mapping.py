import io
from unittest.mock import MagicMock, create_autospec

import pytest
from databricks.sdk.errors import NotFound

from databricks.labs.ucx.account import WorkspaceInfo
from databricks.labs.ucx.hive_metastore.mapping import Rule, TableMapping
from databricks.labs.ucx.hive_metastore.tables import Table, TablesCrawler


def test_current_tables_empty_fails():
    ws = MagicMock()
    table_mapping = TableMapping(ws, "~/.ucx")

    tables_crawler = create_autospec(TablesCrawler)
    tables_crawler.snapshot.return_value = []

    with pytest.raises(ValueError):
        list(table_mapping.current_tables(tables_crawler, "a", "b"))


def test_current_tables_some_rules():
    ws = MagicMock()
    table_mapping = TableMapping(ws, "~/.ucx")

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


def test_save_mapping():
    ws = MagicMock()
    table_mapping = TableMapping(ws, "~/.ucx")

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
    table_mapping = TableMapping(ws, "~/.ucx")

    with pytest.raises(ValueError):
        table_mapping.load()


def test_load_mapping():
    ws = MagicMock()
    ws.workspace.download.return_value = io.StringIO(
        "workspace_name,catalog_name,src_schema,dst_schema,src_table,dst_table\r\n"
        "foo-bar,foo_bar,foo,foo,bar,bar\r\n"
    )
    table_mapping = TableMapping(ws, "~/.ucx")

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
    mapping = TableMapping(ws)
    mapping.skip_table(sbe, schema="schema", table="table")
    sbe.execute.assert_called_with(
        f"ALTER TABLE `schema`.`table` SET TBLPROPERTIES('{mapping.UCX_SKIP_PROPERTY}' = true)"
    )
    assert len(caplog.records) == 0
    mapping.skip_schema(sbe, schema="schema")
    sbe.execute.assert_called_with(f"ALTER SCHEMA `schema` SET DBPROPERTIES('{mapping.UCX_SKIP_PROPERTY}' = true)")
    assert len(caplog.records) == 0


def test_skip_missing_schema(mocker, caplog):
    ws = mocker.patch("databricks.sdk.WorkspaceClient.__init__")
    sbe = mocker.patch("databricks.labs.ucx.framework.crawlers.StatementExecutionBackend.__init__")
    sbe.execute.side_effect = NotFound("[SCHEMA_NOT_FOUND]")
    mapping = TableMapping(ws)
    mapping.skip_schema(sbe, schema="schema")
    assert [rec.message for rec in caplog.records if "schema not found" in rec.message.lower()]


def test_skip_missing_table(mocker, caplog):
    ws = mocker.patch("databricks.sdk.WorkspaceClient.__init__")
    sbe = mocker.patch("databricks.labs.ucx.framework.crawlers.StatementExecutionBackend.__init__")
    sbe.execute.side_effect = NotFound("[TABLE_OR_VIEW_NOT_FOUND]")
    mapping = TableMapping(ws)
    mapping.skip_table(sbe, schema="schema", table="table")
    assert [rec.message for rec in caplog.records if "table not found" in rec.message.lower()]

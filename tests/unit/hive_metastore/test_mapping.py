import io
from unittest.mock import MagicMock, create_autospec

import pytest
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import ExternalLocationInfo

from databricks.labs.ucx.account import WorkspaceInfo
from databricks.labs.ucx.hive_metastore.locations import (
    ExternalLocation,
    ExternalLocations,
)
from databricks.labs.ucx.hive_metastore.mapping import (
    ExternalLocationMapping,
    Rule,
    TableMapping,
)
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


def test_save_external_location_mapping_missing_location():
    ws = MagicMock()
    ext_location_mapping = ExternalLocationMapping(ws, "~/.ucx")
    location_crawler = create_autospec(ExternalLocations)
    location_crawler.snapshot.return_value = [ExternalLocation(location="s3://test_location/test1", table_count=1)]
    ws.external_locations.list.return_value = [ExternalLocationInfo(name="loc1", url="s3://test_location/test11")]
    ext_location_mapping.save(location_crawler)
    (path, content), _ = ws.workspace.upload.call_args
    assert "~/.ucx/external_locations.tf" == path
    assert (
        'resource "databricks_external_location" "name_1" { \n'
        'name = "name_1"\n'
        'url  = "s3://test_location/test1"\n'
        "credential_name = <storage_credential_reference>\n"
        "}\ngit "
    ) == content.read()


def test_save_external_location_mapping_no_missing_location():
    ws = MagicMock()
    ext_location_mapping = ExternalLocationMapping(ws, "~/.ucx")
    location_crawler = create_autospec(ExternalLocations)
    location_crawler.snapshot.return_value = [ExternalLocation(location="s3://test_location/test1", table_count=1)]
    ws.external_locations.list.return_value = [ExternalLocationInfo(name="loc1", url="s3://test_location/test1")]
    ext_location_mapping.save(location_crawler)
    ws.workspace.upload.assert_not_called()

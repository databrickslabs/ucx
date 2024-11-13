import dataclasses
import logging
from unittest.mock import create_autospec

import pytest
from databricks.labs.lsql.backends import MockBackend

from databricks.labs.ucx.framework.owners import AdministratorLocator, LegacyQueryOwnership, WorkspacePathOwnership
from databricks.labs.ucx.source_code.base import LineageAtom, UsedTable
from databricks.labs.ucx.source_code.used_table import UsedTableOwnership, UsedTablesCrawler


@pytest.fixture
def used_table() -> UsedTable:
    return UsedTable(
        catalog_name="test-catalog",
        schema_name="test-schema",
        table_name="test-table",
        is_write=True,
        source_id="test",
        source_lineage=[LineageAtom(object_type="QUERY", object_id="dashboard/query")],
    )


def test_crawler_appends_tables(used_table: UsedTable) -> None:
    backend = MockBackend()
    crawler = UsedTablesCrawler.for_paths(backend, "schema")
    existing = list(crawler.snapshot())
    assert not existing

    crawler.dump_all([used_table] * 3)
    rows = backend.rows_written_for(crawler.full_name, "append")
    assert len(rows) == 3


def test_used_table_ownership_is_workspace_admin_if_not_in_used_tables_snapshot(used_table: UsedTable) -> None:
    administrator_locator = create_autospec(AdministratorLocator)
    administrator_locator.get_workspace_administrator.return_value = "John Doe"
    used_tables_crawler = create_autospec(UsedTablesCrawler)
    used_tables_crawler.snapshot.return_value = [used_table]
    legacy_query_ownership = create_autospec(LegacyQueryOwnership)
    workspace_path_ownership = create_autospec(WorkspacePathOwnership)

    ownership = UsedTableOwnership(
        administrator_locator,
        used_tables_crawler,
        used_tables_crawler,
        legacy_query_ownership,
        workspace_path_ownership,
    )

    owner = ownership.owner_of(UsedTable())

    assert owner == "John Doe"
    administrator_locator.get_workspace_administrator.assert_called_once()
    legacy_query_ownership.owner_of.assert_not_called()
    workspace_path_ownership.owner_of_path.assert_not_called()


def test_used_table_ownership_is_workspace_admin_if_not_write(used_table: UsedTable) -> None:
    used_table = dataclasses.replace(used_table, is_write=False)
    administrator_locator = create_autospec(AdministratorLocator)
    administrator_locator.get_workspace_administrator.return_value = "John Doe"
    used_tables_crawler = create_autospec(UsedTablesCrawler)
    used_tables_crawler.snapshot.return_value = [used_table]
    legacy_query_ownership = create_autospec(LegacyQueryOwnership)
    workspace_path_ownership = create_autospec(WorkspacePathOwnership)

    ownership = UsedTableOwnership(
        administrator_locator,
        used_tables_crawler,
        used_tables_crawler,
        legacy_query_ownership,
        workspace_path_ownership,
    )

    owner = ownership.owner_of(used_table)

    assert owner == "John Doe"
    administrator_locator.get_workspace_administrator.assert_called_once()
    legacy_query_ownership.owner_of.assert_not_called()
    workspace_path_ownership.owner_of_path.assert_not_called()


@pytest.mark.parametrize("object_type", ["QUERY", "NOTEBOOK", "FILE"])
def test_used_table_ownership_from_code(used_table: UsedTable, object_type: str) -> None:
    source_lineage = [LineageAtom(object_type=object_type, object_id="dashboard/query")]
    used_table = dataclasses.replace(used_table, source_lineage=source_lineage)
    administrator_locator = create_autospec(AdministratorLocator)
    administrator_locator.get_workspace_administrator.return_value = "John Doe"
    used_tables_crawler = create_autospec(UsedTablesCrawler)
    used_tables_crawler.snapshot.return_value = [used_table]
    legacy_query_ownership = create_autospec(LegacyQueryOwnership)
    legacy_query_ownership.owner_of.side_effect = lambda i: "Mary Jane" if i == used_table.query_id else None
    workspace_path_ownership = create_autospec(WorkspacePathOwnership)
    workspace_path_ownership.owner_of_path.side_effect = lambda i: "Mary Jane" if i == used_table.source_id else None

    ownership = UsedTableOwnership(
        administrator_locator,
        used_tables_crawler,
        used_tables_crawler,
        legacy_query_ownership,
        workspace_path_ownership,
    )

    owner = ownership.owner_of(used_table)

    assert owner == "Mary Jane"
    administrator_locator.get_workspace_administrator.assert_not_called()
    if object_type == "QUERY":
        legacy_query_ownership.owner_of.assert_called_once_with(used_table.query_id)
        workspace_path_ownership.owner_of_path.assert_not_called()
    else:
        legacy_query_ownership.owner_of.assert_not_called()
        workspace_path_ownership.owner_of_path.assert_called_once_with(used_table.source_id)


def test_used_table_ownership_from_unknown_code_type(caplog, used_table: UsedTable) -> None:
    source_lineage = [LineageAtom(object_type="UNKNOWN", object_id="dashboard/query")]
    used_table = dataclasses.replace(used_table, source_lineage=source_lineage)
    administrator_locator = create_autospec(AdministratorLocator)
    administrator_locator.get_workspace_administrator.return_value = "John Doe"
    used_tables_crawler = create_autospec(UsedTablesCrawler)
    used_tables_crawler.snapshot.return_value = [used_table]
    legacy_query_ownership = create_autospec(LegacyQueryOwnership)
    legacy_query_ownership.owner_of.side_effect = lambda i: "Mary Jane" if i == used_table.query_id else None
    workspace_path_ownership = create_autospec(WorkspacePathOwnership)
    workspace_path_ownership.owner_of_path.side_effect = lambda i: "Mary Jane" if i == used_table.source_id else None

    ownership = UsedTableOwnership(
        administrator_locator,
        used_tables_crawler,
        used_tables_crawler,
        legacy_query_ownership,
        workspace_path_ownership,
    )

    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.source_code.used_table"):
        owner = ownership.owner_of(used_table)
    assert owner == "John Doe"
    assert f"Unknown source type 'UNKNOWN' for {used_table.source_id}" in caplog.messages
    administrator_locator.get_workspace_administrator.assert_called_once()
    legacy_query_ownership.owner_of.assert_not_called()
    workspace_path_ownership.owner_of_path.assert_not_called()

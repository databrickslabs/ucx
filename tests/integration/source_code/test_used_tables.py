import dataclasses
from unittest.mock import create_autospec

import pytest

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


def test_used_table_ownership_from_query(used_table: UsedTable) -> None:
    administrator_locator = create_autospec(AdministratorLocator)
    administrator_locator.get_workspace_administrator.return_value = "John Doe"
    used_tables_crawler = create_autospec(UsedTablesCrawler)
    used_tables_crawler.snapshot.return_value = [used_table]
    legacy_query_ownership = create_autospec(LegacyQueryOwnership)
    legacy_query_ownership.owner_of.side_effect = lambda id_: "Query Owner" if id_ == used_table.query_id else None

    workspace_path_ownership = create_autospec(WorkspacePathOwnership)
    ownership = UsedTableOwnership(
        administrator_locator,
        used_tables_crawler,
        used_tables_crawler,
        legacy_query_ownership,
        workspace_path_ownership,
    )

    owner = ownership.owner_of(used_table)

    assert owner == "Query Owner"

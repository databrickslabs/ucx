from unittest.mock import create_autospec

from databricks.labs.ucx.framework.owners import (
    AdministratorLocator,
    LegacyQueryOwnership,
    WorkspacePathOwnership
)
from databricks.labs.ucx.source_code.base import UsedTable
from databricks.labs.ucx.source_code.used_table import UsedTableOwnership, UsedTablesCrawler


def test_used_table_ownership_is_workspace_admin_if_not_in_used_tables_snapshot() -> None:
    administrator_locator = create_autospec(AdministratorLocator)
    administrator_locator.get_workspace_administrator.return_value = "John Doe"
    used_tables_crawler = create_autospec(UsedTablesCrawler)
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


def test_used_table_ownership_is_workspace_admin_if_not_write() -> None:
    administrator_locator = create_autospec(AdministratorLocator)
    administrator_locator.get_workspace_administrator.return_value = "John Doe"
    used_tables_crawler = create_autospec(UsedTablesCrawler)
    legacy_query_ownership = create_autospec(LegacyQueryOwnership)
    workspace_path_ownership = create_autospec(WorkspacePathOwnership)
    ownership = UsedTableOwnership(
        administrator_locator,
        used_tables_crawler,
        used_tables_crawler,
        legacy_query_ownership,
        workspace_path_ownership,
    )

    owner = ownership.owner_of(UsedTable(is_write=False))

    assert owner == "John Doe"

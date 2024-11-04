"""
Separate table ownership module to account for classes in the `:mod:hive_metastore.grants` using the `:class:Table` and
`:class:TableOwnership` using the `GrantsCrawler` in return.

"""

from functools import cached_property

from databricks.labs.ucx.framework.owners import (
    Ownership,
    AdministratorLocator,
)
from databricks.labs.ucx.hive_metastore.grants import GrantsCrawler
from databricks.labs.ucx.hive_metastore.tables import Table
from databricks.labs.ucx.source_code.base import UsedTable
from databricks.labs.ucx.source_code.used_table import UsedTableOwnership


class TableOwnership(Ownership[Table]):
    """Determine ownership of tables in the inventory based on the following rules:
    - If a table is owned by a principal in the grants table, then that principal is the owner.
    - If a table is written to by a query, then the owner of that query is the owner of the table.
    - If a table is written to by a notebook or file, then the owner of the path is the owner of the table.
    """

    def __init__(
        self,
        administrator_locator: AdministratorLocator,
        grants_crawler: GrantsCrawler,
        used_table_ownership: UsedTableOwnership,
    ) -> None:
        super().__init__(administrator_locator)
        self._grants_crawler = grants_crawler
        self._used_table_ownership = used_table_ownership

    def _maybe_direct_owner(self, record: Table) -> str | None:
        owner = self._maybe_from_grants(record)
        if owner:
            return owner
        # Read and write do - or should - not affect ownership
        used_table = UsedTable.from_table(record, is_read=False, is_write=False)
        # This call defers the `administrator_locator` to the one of `UsedTableOwnership`, we expect them to be the same
        return self._used_table_ownership.owner_of(used_table)

    def _maybe_from_grants(self, record: Table) -> str | None:
        for grant in self._grants_snapshot:
            if not grant.action_type == 'OWN':
                continue
            object_type, full_name = grant.this_type_and_key()
            if object_type == 'TABLE' and full_name == record.key:
                return grant.principal
            if object_type in {'DATABASE', 'SCHEMA'} and full_name == f"{record.catalog}.{record.database}":
                return grant.principal
        return None

    @cached_property
    def _grants_snapshot(self):
        return self._grants_crawler.snapshot()

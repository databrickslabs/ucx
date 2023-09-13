import logging

from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.tacl._internal import (
    RuntimeBackend,
    SqlBackend,
    StatementExecutionBackend,
)
from databricks.labs.ucx.tacl.grants import GrantsCrawler
from databricks.labs.ucx.tacl.tables import TablesCrawler

logger = logging.getLogger(__name__)


class TaclToolkit:
    def __init__(
        self,
        ws: WorkspaceClient,
        inventory_catalog,
        inventory_schema,
        warehouse_id=None,
        databases=None,
    ):
        self._tc = TablesCrawler(self._backend(ws, warehouse_id), inventory_catalog, inventory_schema)
        self._gc = GrantsCrawler(self._tc)

        self._databases = databases if databases else [database for (database,) in self._tc._all_databases()]

    def database_snapshot(self):
        tables = []
        for db in self._databases:
            for t in self._tc.snapshot("hive_metastore", db):
                tables.append(t)
        return tables

    def grants_snapshot(self):
        grants = []
        for db in self._databases:
            for grant in self._gc.snapshot("hive_metastore", db):
                grants.append(grant)
        return grants

    @staticmethod
    def _backend(ws: WorkspaceClient, warehouse_id: str | None = None) -> SqlBackend:
        if warehouse_id is None:
            return RuntimeBackend()
        return StatementExecutionBackend(ws, warehouse_id)

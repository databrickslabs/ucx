import logging

from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.config import MigrationConfig
from databricks.labs.ucx.tacl._internal import (
    RuntimeBackend,
    SqlBackend,
    StatementExecutionBackend,
)
from databricks.labs.ucx.tacl.grants import GrantsCrawler
from databricks.labs.ucx.tacl.tables import TablesCrawler

logger = logging.getLogger(__name__)


class TaclToolkit:
    def __init__(self, ws: WorkspaceClient, config: MigrationConfig, warehouse_id=None):
        self.inventory_catalog = config.inventory.table.catalog
        self.inventory_schema = config.inventory.table.database

        self._tc = TablesCrawler(self._backend(ws, warehouse_id), self.inventory_catalog, self.inventory_schema)
        self._gc = GrantsCrawler(self._tc)

        self.databases = (
            config.tacl.databases
            if config.tacl.databases
            else [database.as_dict()["databaseName"] for database in self._tc._all_databases()]
        )

    def database_snapshot(self):
        return self._tc.snapshot("hive_metastore", self.databases)

    def grants_snapshot(self):
        return self._gc.snapshot("hive_metastore", self.databases)

    @staticmethod
    def _backend(ws: WorkspaceClient, warehouse_id: str | None = None) -> SqlBackend:
        if warehouse_id is None:
            return RuntimeBackend()
        return StatementExecutionBackend(ws, warehouse_id)

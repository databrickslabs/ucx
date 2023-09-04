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
    def __init__(self, ws: WorkspaceClient, inventory_catalog, inventory_schema, warehouse_id=None):
        self._tc = TablesCrawler(self._backend(ws, warehouse_id), inventory_catalog, inventory_schema)
        self._gc = GrantsCrawler(self._tc)

    def database_snapshot(self, databases):
        return self._tc.snapshot("hive_metastore", databases)

    def grants_snapshot(self, databases):
        return self._gc.snapshot("hive_metastore", databases)

    @staticmethod
    def _backend(ws: WorkspaceClient, warehouse_id: str | None = None) -> SqlBackend:
        if warehouse_id is None:
            return RuntimeBackend()
        return StatementExecutionBackend(ws, warehouse_id)

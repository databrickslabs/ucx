from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.tacl.grants import GrantsCrawler
from databricks.labs.ucx.tacl.tables import TablesCrawler


class TaclToolkit:
    def __init__(self, ws: WorkspaceClient, warehouse_id, inventory_catalog, inventory_schema):
        self._tc = TablesCrawler(ws, warehouse_id, inventory_catalog, inventory_schema)
        self._gc = GrantsCrawler(self._tc)

    def database_snapshot(self, schema):
        return self._tc.snapshot("hive_metastore", schema)

    def grants_snapshot(self, schema):
        return self._gc.snapshot("hive_metastore", schema)

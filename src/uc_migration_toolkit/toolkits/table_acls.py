from databricks.sdk import WorkspaceClient
from uc_migration_toolkit.tacl.grants import Grant, GrantsCrawler
from uc_migration_toolkit.tacl.tables import Table, TablesCrawler


class TaclToolkit:
    def __init__(self, ws: WorkspaceClient, warehouse_id, inventory_catalog, inventory_schema):
        self._tc = TablesCrawler(ws, warehouse_id, inventory_catalog, inventory_schema)
        self._gc = GrantsCrawler(self._tc)

    def database_snapshot(self, schema):
        return self._tc.snapshot('hive_metastore', schema)

    def grants_snapshot(self, schema):
        return self._gc.snapshot('hive_metastore', schema)


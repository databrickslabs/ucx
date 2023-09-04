import argparse
import logging

from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.tacl._internal import (
    RuntimeBackend,
    SqlBackend,
    StatementExecutionBackend,
)
from databricks.labs.ucx.tacl.grants import GrantsCrawler
from databricks.labs.ucx.tacl.tables import TablesCrawler

logging.getLogger("databricks.labs.ucx").setLevel("DEBUG")


class TaclToolkit:
    def __init__(self, ws: WorkspaceClient, inventory_catalog, inventory_schema, warehouse_id=None):
        self._tc = TablesCrawler(self._backend(ws, warehouse_id), inventory_catalog, inventory_schema)
        self._gc = GrantsCrawler(self._tc)

    def database_snapshot(self, schema):
        return self._tc.snapshot("hive_metastore", schema)

    def grants_snapshot(self, schema):
        return self._gc.snapshot("hive_metastore", schema)

    @staticmethod
    def _backend(ws: WorkspaceClient, warehouse_id: str | None = None) -> SqlBackend:
        if warehouse_id is None:
            return RuntimeBackend()
        return StatementExecutionBackend(ws, warehouse_id)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--inventory_catalog")
    parser.add_argument("--inventory_schema")
    parser.add_argument("--databases", required=False, default="default")
    parser.add_argument("--warehouse_id", required=False, default=None)

    args = parser.parse_args()

    inventory_catalog = args.inventory_catalog
    inventory_schema = args.inventory_schema
    warehouse_id = args.warehouse_id
    databases = args.databases.split(",")

    ws = WorkspaceClient
    tak = TaclToolkit(ws, inventory_catalog, inventory_schema, warehouse_id)

    for database in databases:
        tak.grants_snapshot(database)

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import Language

from databricks.labs.ucx.config import MigrationConfig
from databricks.labs.ucx.inventory.permissions import PermissionManager
from databricks.labs.ucx.inventory.table import InventoryTableManager
from databricks.labs.ucx.managers.group import GroupManager
from databricks.labs.ucx.providers.client import ImprovedWorkspaceClient
from databricks.labs.ucx.tacl._internal import (
    RuntimeBackend,
    SqlBackend,
    StatementExecutionBackend,
)
from databricks.labs.ucx.tacl.grants import GrantsCrawler
from databricks.labs.ucx.tacl.tables import TablesCrawler
from databricks.labs.ucx.providers.mixins.compute import CommandExecutor


class Assessment:
    def __init__(self, ws: WorkspaceClient, inventory_catalog, inventory_schema, warehouse_id=None):
        self._ws = ws

    @staticmethod
    def _verify_ws_client(w: ImprovedWorkspaceClient):
        _me = w.current_user.me()
        is_workspace_admin = any(g.display == "admins" for g in _me.groups)
        if not is_workspace_admin:
            msg = "Current user is not a workspace admin"
            raise RuntimeError(msg)

    def table_inventory(self):
        commands = CommandExecutor(self._ws,language=Language.PYTHON)

        command_output = commands.run(
            """
            print('TEST OUTPUT')
            """
        )
        print("command_output")

    @staticmethod
    def _backend(ws: WorkspaceClient, warehouse_id: str | None = None) -> SqlBackend:
        if warehouse_id is None:
            return RuntimeBackend()
        return StatementExecutionBackend(ws, warehouse_id)

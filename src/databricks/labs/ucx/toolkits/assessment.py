from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import Language

from databricks.labs.ucx.providers.client import ImprovedWorkspaceClient
from databricks.labs.ucx.providers.mixins.compute import CommandExecutor
from databricks.labs.ucx.tacl._internal import (
    RuntimeBackend,
    SqlBackend,
    StatementExecutionBackend,
)


class Assessment:
    def __init__(self, ws: WorkspaceClient, inventory_catalog, inventory_schema, warehouse_id=None):
        self._ws = ws
        self._inventory_catalog = inventory_catalog
        self._inventory_schema = inventory_schema
        self._warehouse_id = warehouse_id

    @staticmethod
    def _verify_ws_client(w: ImprovedWorkspaceClient):
        _me = w.current_user.me()
        is_workspace_admin = any(g.display == "admins" for g in _me.groups)
        if not is_workspace_admin:
            msg = "Current user is not a workspace admin"
            raise RuntimeError(msg)

    def table_inventory(self):
        commands = CommandExecutor(self._ws, language=Language.SCALA)

        from importlib import resources as impresources
        from databricks.labs.ucx.assessment import scala

        inp_file = (impresources.files(scala) / 'assessment.scala')
        with inp_file.open("rt") as f:
            template = f.read()

        command_output = commands.run(template)
        print(command_output)

    def external_locations(self):
        pass

    @staticmethod
    def _backend(ws: WorkspaceClient, warehouse_id: str | None = None) -> SqlBackend:
        if warehouse_id is None:
            return RuntimeBackend()
        return StatementExecutionBackend(ws, warehouse_id)

import os
import os.path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import Language
from databricks.labs.ucx.tacl.tables import Table
from databricks.labs.ucx.providers.mixins.compute import CommandExecutor
from databricks.labs.ucx.tacl._internal import (
    RuntimeBackend,
    SqlBackend,
    StatementExecutionBackend,
)


class AssessmentToolkit:
    def __init__(self, ws: WorkspaceClient, cluster_id, inventory_catalog, inventory_schema, warehouse_id=None):
        self._ws = ws
        self._inventory_catalog = inventory_catalog
        self._inventory_schema = inventory_schema
        self._warehouse_id = warehouse_id
        self._cluster_id = cluster_id
        self._external_locations = None

    @staticmethod
    def _verify_ws_client(w: WorkspaceClient):
        _me = w.current_user.me()
        is_workspace_admin = any(g.display == "admins" for g in _me.groups)
        if not is_workspace_admin:
            msg = "Current user is not a workspace admin"
            raise RuntimeError(msg)

    def table_inventory(self):
        commands = CommandExecutor(self._ws, language=Language.SCALA, cluster_id=self._cluster_id)

        from importlib import resources as impresources
        from databricks.labs.ucx.assessment import scala

        inp_file = (impresources.files(scala) / 'assessment.scala')
        with inp_file.open("rt") as f:
            template = f.read()
        setup_code = f"""
        val schema="{self._inventory_schema}";
        """
        command_output = commands.run(setup_code + template)
        print(command_output)

    def external_locations(self, tables: [Table]):
        ext_locations = []
        for table in tables:
            dupe = False
            loc = 0
            while loc < len(ext_locations) and not dupe:
                common = os.path.commonprefix([ext_locations[loc], os.path.dirname(table.location) + '/'])
                if common.count("/") > 2:
                    ext_locations[loc] = common
                    dupe = True
                loc += 1
            if not dupe:
                ext_locations.append((os.path.dirname(table.location) + '/'))
        return ext_locations

    @staticmethod
    def _backend(ws: WorkspaceClient, warehouse_id: str | None = None) -> SqlBackend:
        if warehouse_id is None:
            return RuntimeBackend()
        return StatementExecutionBackend(ws, warehouse_id)


if __name__ == "__main__":
    ws = WorkspaceClient()
    cluster_id = os.getenv("CLUSTER_ID")
    print(cluster_id)
    assess = AssessmentToolkit(ws, cluster_id, "UCX", "UCX_assessment")
    # assess.table_inventory()
    print(assess.external_locations(["1", "2"]))

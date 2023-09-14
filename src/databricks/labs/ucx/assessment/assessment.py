import logging
import re
from importlib import resources

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import Language

from databricks.labs.ucx.assessment import commands
from databricks.labs.ucx.mixins.compute import CommandExecutor

logger = logging.getLogger(__name__)


class AssessmentToolkit:
    def __init__(self, ws: WorkspaceClient, cluster_id, inventory_catalog, inventory_schema, warehouse_id=None):
        self._ws = ws
        self._inventory_catalog = inventory_catalog
        self._inventory_schema = inventory_schema
        self._warehouse_id = warehouse_id
        self._cluster_id = cluster_id
        self._command_executor = None
        self._managed_executor = False

    def _get_command_executor(self, executor: CommandExecutor | None = None, language=None):
        ce = executor
        if ce is None:
            if language:
                ce = CommandExecutor(self._ws, language=language, cluster_id=self._cluster_id)
            else:
                ce = CommandExecutor(self._ws, cluster_id=self._cluster_id)
            self._managed_executor = True
        self._command_executor = ce
        return ce

    def _remove_command_executor(self):
        if self._managed_executor:
            self._command_executor = None
            self._managed_executor = False

    @staticmethod
    def _load_command_code(name):
        cmd_file = resources.files(commands) / name
        with cmd_file.open("rt") as f:
            cmd_code = f.read()
        return cmd_code

    def _get_command(self, name, params: dict | None = None):
        cmd_code = self._load_command_code(name)
        if params:
            for pattern, replace in params.items():
                p = re.compile(pattern)
                cmd_code = p.sub(replace, cmd_code)
        return cmd_code

    @staticmethod
    def _verify_ws_client(w: WorkspaceClient):
        _me = w.current_user.me()
        is_workspace_admin = any(g.display == "admins" for g in _me.groups)
        if not is_workspace_admin:
            msg = "Current user is not a workspace admin"
            raise RuntimeError(msg)

    def table_inventory(self, executor: CommandExecutor | None = None):
        logger.info("Started dataset inventorization...")
        ce = self._get_command_executor(executor, language=Language.SCALA)
        params = {"SCHEMA": self._inventory_schema}
        cmd_code = self._get_command("create_table_inventory.scala", params=params)
        command_output = ce.run(cmd_code)
        logger.debug(command_output)
        if executor is None:
            self._remove_command_executor()
        logger.info("Completed dataset inventorization...")

    def compile_report(self):
        logger.info("Started report compilation...")
        ce = self._get_command_executor(None, language=Language.SCALA)
        self.table_inventory(ce)
        self._remove_command_executor()
        logger.info("Completed report compilation...")

import logging
import os

from databricks.labs.blueprint.installation import Installation
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import PermissionDenied
from databricks.sdk.service.sql import EndpointInfoWarehouseType

from databricks.labs.ucx.config import WorkspaceConfig

logger = logging.getLogger(__name__)


class InstallationMixin:
    def __init__(self, config: WorkspaceConfig, installation: Installation, ws: WorkspaceClient):
        self._config = config
        self._installation = installation
        self._ws = ws

    def _name(self, name: str) -> str:
        prefix = os.path.basename(self._installation.install_folder()).removeprefix('.')
        return f"[{prefix.upper()}] {name}"

    @property
    def _my_username(self):
        if not hasattr(self, "_me"):
            self._me = self._ws.current_user.me()
            is_workspace_admin = any(g.display == "admins" for g in self._me.groups)
            if not is_workspace_admin:
                msg = "Current user is not a workspace admin"
                raise PermissionDenied(msg)
        return self._me.user_name

    @property
    def _warehouse_id(self) -> str:
        if self._config.warehouse_id is not None:
            logger.info("Fetching warehouse_id from a config")
            return self._config.warehouse_id
        warehouses = [_ for _ in self._ws.warehouses.list() if _.warehouse_type == EndpointInfoWarehouseType.PRO]
        warehouse_id = self._config.warehouse_id
        if not warehouse_id and not warehouses:
            msg = "need either configured warehouse_id or an existing PRO SQL warehouse"
            raise ValueError(msg)
        if not warehouse_id:
            warehouse_id = warehouses[0].id
        self._config.warehouse_id = warehouse_id
        return warehouse_id

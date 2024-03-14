import logging
import os
from datetime import datetime
from pathlib import Path

from databricks.labs.blueprint.installation import Installation
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import EndpointInfoWarehouseType

from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.framework.tasks import _TASKS, Task

logger = logging.getLogger(__name__)


class InstallationMixin:
    def __init__(self, config: WorkspaceConfig, installation: Installation, ws: WorkspaceClient):
        self._config = config
        self._installation = installation
        self._ws = ws
        self._this_file = Path(__file__)

    @staticmethod
    def sorted_tasks() -> list[Task]:
        return sorted(_TASKS.values(), key=lambda x: x.task_id)

    @classmethod
    def step_list(cls) -> list[str]:
        step_list = []
        for task in cls.sorted_tasks():
            if task.workflow not in step_list:
                step_list.append(task.workflow)
        return step_list

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
                raise PermissionError(msg)
        return self._me.user_name

    @property
    def _short_name(self):
        if "@" in self._my_username:
            username = self._my_username.split("@")[0]
        else:
            username = self._me.display_name
        return username

    @staticmethod
    def _readable_timedelta(epoch):
        when = datetime.utcfromtimestamp(epoch)
        duration = datetime.now() - when
        data = {}
        data["days"], remaining = divmod(duration.total_seconds(), 86_400)
        data["hours"], remaining = divmod(remaining, 3_600)
        data["minutes"], data["seconds"] = divmod(remaining, 60)

        time_parts = ((name, round(value)) for (name, value) in data.items())
        time_parts = [f"{value} {name[:-1] if value == 1 else name}" for name, value in time_parts if value > 0]
        if len(time_parts) > 0:
            time_parts.append("ago")
        if time_parts:
            return " ".join(time_parts)
        return "less than 1 second ago"

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

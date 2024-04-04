from functools import cached_property
from pathlib import Path
from typing import Protocol

from databricks.labs.blueprint.installation import Installation
from databricks.labs.lsql.backends import RuntimeBackend, SqlBackend
from databricks.sdk import WorkspaceClient, core

from databricks.labs.ucx.__about__ import __version__
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.contexts.application import GlobalContext


class Snapshot(Protocol):
    def __init__(self, ws: WorkspaceClient, backend: SqlBackend, schema: str): ...

    def snapshot(self): ...


class RuntimeContext(GlobalContext):
    @cached_property
    def _config_path(self) -> Path:
        config = self.flags.get("config")
        if not config:
            raise ValueError("config flag is required")
        return Path(config)

    @cached_property
    def config(self) -> WorkspaceConfig:
        return Installation.load_local(WorkspaceConfig, self._config_path)

    @cached_property
    def connect_config(self) -> core.Config:
        # this is to calm down mypy:
        # Argument "config" to "WorkspaceClient" has incompatible
        # type "Config | None"; expected "Config"  [arg-type]
        return self.workspace_client.config

    @cached_property
    def workspace_client(self) -> WorkspaceClient:
        return WorkspaceClient(config=self.connect_config, product='ucx', product_version=__version__)

    @cached_property
    def sql_backend(self) -> SqlBackend:
        return RuntimeBackend(debug_truncate_bytes=self.connect_config.debug_truncate_bytes)

    @cached_property
    def installation(self):
        install_folder = self._config_path.parent.as_posix().removeprefix("/Workspace")
        return Installation(self.workspace_client, "ucx", install_folder=install_folder)

    def simple_snapshot(self, crawler_class: type[Snapshot]):
        crawler = crawler_class(self.workspace_client, self.sql_backend, self.config.inventory_database)
        crawler.snapshot()

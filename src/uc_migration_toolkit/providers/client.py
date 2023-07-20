from dataclasses import asdict

from databricks.sdk import WorkspaceClient

from uc_migration_toolkit.config import MigrationConfig
from uc_migration_toolkit.providers.logger import LoggerMixin


class ClientMixin(LoggerMixin):
    def __init__(self, config: MigrationConfig):
        super().__init__()
        self._ws_client = self.get_workspace_client(config)

    @property
    def ws_client(self) -> WorkspaceClient:
        return self._ws_client

    def _get_workspace_client(self, config: MigrationConfig) -> WorkspaceClient:
        self.logger.info("Initializing the workspace client")
        if config.auth_config and config.auth_config.workspace:
            self.logger.info("Using the provided workspace client credentials")
            return WorkspaceClient(**asdict(config.auth_config.account))
        else:
            self.logger.info("Trying standard workspace auth mechanisms")
            return WorkspaceClient()

    def get_workspace_client(self, config: MigrationConfig) -> WorkspaceClient:
        client = self._get_workspace_client(
            config,
        )
        assert client.current_user.me(), "Cannot authenticate with the workspace client"
        _me = client.current_user.me()
        is_workspace_admin = any(g.display == "admins" for g in _me.groups)
        if not is_workspace_admin:
            msg = "Current user is not a workspace admin"
            raise RuntimeError(msg)
        return client

from dataclasses import asdict

from databricks.sdk import WorkspaceClient

from uc_migration_toolkit.providers.config import provider as config_provider
from uc_migration_toolkit.providers.logger import logger


class ClientProvider:
    def __init__(self):
        self._ws_client = None

    @staticmethod
    def _verify_ws_client(w: WorkspaceClient):
        assert w.current_user.me(), "Cannot authenticate with the workspace client"
        _me = w.current_user.me()
        is_workspace_admin = any(g.display == "admins" for g in _me.groups)
        if not is_workspace_admin:
            msg = "Current user is not a workspace admin"
            raise RuntimeError(msg)

    def set_ws_client(self):
        auth_config = config_provider.config.auth
        logger.info("Initializing the workspace client")
        if auth_config and auth_config.workspace:
            logger.info("Using the provided workspace client credentials")
            _client = WorkspaceClient(**asdict(auth_config.workspace))
        else:
            logger.info("Trying standard workspace auth mechanisms")
            _client = WorkspaceClient()

        self._verify_ws_client(_client)
        self._ws_client = _client

    @property
    def ws(self) -> WorkspaceClient:
        assert self._ws_client, "Workspace client not initialized"
        return self._ws_client


provider = ClientProvider()

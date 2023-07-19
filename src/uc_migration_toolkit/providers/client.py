from dataclasses import asdict

from databricks.sdk import AccountClient, WorkspaceClient

from uc_migration_toolkit.config import MigrationConfig
from uc_migration_toolkit.providers.logger import LoggerProvider
from uc_migration_toolkit.utils import get_dbutils


class ClientProvider(LoggerProvider):
    def _get_workspace_client(self, config: MigrationConfig) -> WorkspaceClient:
        self.logger.info("Initializing the workspace client")
        dbutils = get_dbutils()
        if config.auth_config.workspace:
            self.logger.info("Using the provided workspace client credentials")
            return WorkspaceClient(host=config.auth_config.workspace.host, token=config.auth_config.workspace.token)
        else:
            self.logger.info("Using the current notebook workspace client credentials")
            workspace_url = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
            token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
            if not workspace_url or not token:
                msg = "Cannot pickup credentials from the current notebook"
                raise RuntimeError(msg)
            return WorkspaceClient(host=workspace_url, token=token)

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

    def _get_account_client(self, config: MigrationConfig) -> AccountClient:
        self.logger.info("Initializing the account client")
        return AccountClient(**asdict(config.auth_config.account))

    def get_account_client(self, config: MigrationConfig) -> AccountClient:
        client = self._get_account_client(config)

        current_user_option = list(
            client.users.list(
                filter=f'userName eq "{config.auth_config.account.username}"', attributes="userName,roles"
            )
        )

        if not current_user_option:
            msg = f"Cannot find current user {config.auth_config.account.username} across the account users"
            raise RuntimeError(msg)

        current_user = current_user_option[0]

        if not current_user.roles:
            msg = f"Current user {current_user.user_name} has no roles, therefore is not an account admin"
            raise RuntimeError(msg)

        is_admin = any(role.value == "account_admin" for role in current_user.roles)
        if not is_admin:
            msg = f"Current user {current_user.user_name} is not an account admin"
            raise RuntimeError(msg)

        return client

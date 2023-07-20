from dataclasses import asdict

import typer
from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.core import DatabricksError

from uc_migration_toolkit.config import MigrationConfig
from uc_migration_toolkit.providers.logger import LoggerMixin


class ClientProvider(LoggerMixin):
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

    def _get_account_client(self, config: MigrationConfig) -> AccountClient:
        self.logger.info("Initializing the account client")
        if config.auth_config and config.auth_config.account:
            return AccountClient(**asdict(config.auth_config.account))
        else:
            return AccountClient()

    def get_account_client(self, config: MigrationConfig) -> AccountClient | None:
        try:
            client = self._get_account_client(config)

            current_user_option = list(
                client.users.list(filter=f'userName eq "{client.config.username}"', attributes="userName,roles")
            )

            if not current_user_option:
                msg = f"Cannot find current user {client.config.username} across the account users"
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

        except DatabricksError:
            typer.echo("Cannot authenticate to the account level, account-level operations will not be available")

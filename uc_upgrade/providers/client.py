from dataclasses import asdict

from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.runtime import dbutils

from uc_upgrade.config import MigrationConfig
from uc_upgrade.providers.logger import LoggerProvider


class ClientProvider:
    logger = LoggerProvider.get_logger()

    @classmethod
    def _get_workspace_client(cls, config: MigrationConfig) -> WorkspaceClient:
        cls.logger.info("Initializing the workspace client")
        if config.auth_config.workspace:
            cls.logger.info("Using the provided workspace client credentials")
            return WorkspaceClient(host=config.auth_config.workspace.host, token=config.auth_config.workspace.token)
        else:
            cls.logger.info("Using the current notebook workspace client credentials")
            workspace_url = (
                dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
            )  # noqa: F405
            token = (
                dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
            )  # noqa: F405
            if not workspace_url or not token:
                raise RuntimeError("Cannot pickup credentials from the current notebook")
            return WorkspaceClient(host=workspace_url, token=token)

    @classmethod
    def get_workspace_client(cls, config: MigrationConfig) -> WorkspaceClient:
        client = cls._get_workspace_client(config)
        assert client.current_user.me(), "Cannot authenticate with the workspace client"
        _me = client.current_user.me()
        is_workspace_admin = any([g.display == "admins" for g in _me.groups])
        if not is_workspace_admin:
            raise RuntimeError("Current user is not a workspace admin")
        return client

    @classmethod
    def _get_account_client(cls, config: MigrationConfig) -> AccountClient:
        cls.logger.info("Initializing the account client")
        return AccountClient(**asdict(config.auth_config.account))

    @classmethod
    def get_account_client(cls, config: MigrationConfig) -> AccountClient:
        client = cls._get_account_client(config)

        current_user_option = list(
            client.users.list(
                filter=f'userName eq "{config.auth_config.account.username}"', attributes="userName,roles"
            )
        )

        if not current_user_option:
            raise RuntimeError(
                f"Cannot find current user {config.auth_config.account.username} across the account users"
            )

        current_user = current_user_option[0]

        if not current_user.roles:
            raise RuntimeError(f"Current user {current_user.user_name} has no roles, therefore is not an account admin")

        is_admin = any([role.value == "account_admin" for role in current_user.roles])
        if not is_admin:
            raise RuntimeError(f"Current user {current_user.user_name} is not an account admin")

        return client

from dataclasses import asdict

from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.runtime import dbutils

from uc_upgrade.config import MigrationConfig
from uc_upgrade.providers.logger import LoggerProvider


class ClientProvider:
    logger = LoggerProvider.get_logger()

    @classmethod
    def get_workspace_client(cls, config: MigrationConfig) -> WorkspaceClient:
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
    def get_account_client(cls, config: MigrationConfig) -> AccountClient:
        cls.logger.info("Initializing the account client")
        return AccountClient(**asdict(config.auth_config.account))

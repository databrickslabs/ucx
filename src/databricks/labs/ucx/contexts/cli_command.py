import abc
import logging
from functools import cached_property

from databricks.labs.blueprint.tui import Prompts
from databricks.labs.lsql.backends import SqlBackend, StatementExecutionBackend
from databricks.sdk import AccountClient, WorkspaceClient

from databricks.labs.ucx.account import AccountWorkspaces
from databricks.labs.ucx.contexts.application import GlobalContext
from databricks.labs.ucx.source_code.files import LocalFileMigrator
from databricks.labs.ucx.workspace_access.clusters import ClusterAccess

logger = logging.getLogger(__name__)


class CliContext(GlobalContext, abc.ABC):
    @cached_property
    def prompts(self) -> Prompts:
        return Prompts()


class WorkspaceContext(CliContext):
    def __init__(self, ws: WorkspaceClient, named_parameters: dict[str, str] | None = None):
        super().__init__(named_parameters)
        self._ws = ws

    @cached_property
    def workspace_client(self) -> WorkspaceClient:
        return self._ws

    @cached_property
    def sql_backend(self) -> SqlBackend:
        return StatementExecutionBackend(self.workspace_client, self.config.warehouse_id)

    @cached_property
    def local_file_migrator(self):
        return LocalFileMigrator(self.languages)

    @cached_property
    def cluster_access(self):
        return ClusterAccess(self.installation, self.workspace_client, self.prompts)

    def create_uber_principal(self, prompts: Prompts):
        if self.is_azure:
            return self.azure_resource_permissions.create_uber_principal(prompts)
        if self.is_aws:
            return self.aws_resource_permissions.create_uber_principal(prompts)
        raise ValueError("Unsupported cloud provider")

    def principal_prefix_access(self):
        if self.is_azure:
            return self.azure_resource_permissions.save_spn_permissions()
        if self.is_aws:
            instance_role_path = self.aws_resource_permissions.save_instance_profile_permissions()
            logger.info(f"Instance profile and bucket info saved {instance_role_path}")
            logger.info("Generating UC roles and bucket permission info")
            return self.aws_resource_permissions.save_uc_compatible_roles()
        raise ValueError("Unsupported cloud provider")


class AccountContext(CliContext):
    def __init__(self, ac: AccountClient, named_parameters: dict[str, str] | None = None):
        super().__init__(named_parameters)
        self._ac = ac

    @cached_property
    def account_client(self) -> AccountClient:
        return self._ac

    @cached_property
    def account_workspaces(self):
        return AccountWorkspaces(self.account_client)

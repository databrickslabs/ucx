import abc
from functools import cached_property

from databricks.labs.blueprint.tui import Prompts
from databricks.labs.lsql.backends import SqlBackend, StatementExecutionBackend
from databricks.sdk import AccountClient, WorkspaceClient

from databricks.labs.ucx.account import AccountWorkspaces
from databricks.labs.ucx.contexts.application import GlobalContext
from databricks.labs.ucx.source_code.files import LocalFileMigrator
from databricks.labs.ucx.workspace_access.clusters import ClusterAccess


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

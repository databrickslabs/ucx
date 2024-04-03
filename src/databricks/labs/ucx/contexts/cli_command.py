from functools import cached_property

from databricks.labs.blueprint.tui import Prompts
from databricks.labs.lsql.backends import SqlBackend, StatementExecutionBackend
from databricks.sdk import AccountClient, WorkspaceClient

from databricks.labs.ucx.account import AccountWorkspaces
from databricks.labs.ucx.contexts.application import GlobalContext
from databricks.labs.ucx.source_code.files import Files


class CliContext(GlobalContext):
    def __init__(self, flags: dict[str, str] | None = None):
        super().__init__()
        if not flags:
            flags = {}
        self.flags = flags

    @cached_property
    def prompts(self) -> Prompts:
        return Prompts()


class WorkspaceContext(CliContext):
    def __init__(self, ws: WorkspaceClient, flags: dict[str, str] | None = None):
        super().__init__(flags)
        self._ws = ws

    @cached_property
    def workspace_client(self) -> WorkspaceClient:
        return self._ws

    @cached_property
    def sql_backend(self) -> SqlBackend:
        return StatementExecutionBackend(self.workspace_client, self.config.warehouse_id)

    @cached_property
    def local_file_migrator(self):
        return Files(self.languages)


class AccountContext(CliContext):
    def __init__(self, ac: AccountClient, flags: dict[str, str] | None = None):
        super().__init__(flags)
        self._ac = ac

    @cached_property
    def account_client(self) -> AccountClient:
        return self._ac

    @cached_property
    def account_workspaces(self):
        return AccountWorkspaces(self.account_client)

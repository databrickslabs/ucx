from functools import cached_property
from os import environ

from databricks.sdk import AccountClient, Workspace, WorkspaceClient

from databricks.labs.ucx.account.aggregate import AccountAggregate
from databricks.labs.ucx.account.metastores import AccountMetastores
from databricks.labs.ucx.account.workspaces import AccountWorkspaces
from databricks.labs.ucx.contexts.application import CliContext


class AccountContext(CliContext):
    def __init__(self, ac: AccountClient, named_parameters: dict[str, str] | None = None):
        super().__init__(named_parameters)
        self._ac = ac

    @cached_property
    def account_client(self) -> AccountClient:
        return self._ac

    @cached_property
    def workspace_client(self) -> WorkspaceClient:
        """The workspace client when workspace_id is provided."""
        workspace_id = self.named_parameters.get("workspace_id")
        if workspace_id is not None:
            workspace_id_casted = int(workspace_id)
        else:
            workspace_id_casted = None
        workspace = Workspace(workspace_id=workspace_id_casted)
        workspace_client = self.account_workspaces.client_for(workspace)
        return workspace_client

    @cached_property
    def workspace_ids(self):
        return [int(_.strip()) for _ in self.named_parameters.get("workspace_ids", "").split(",") if _]

    @cached_property
    def account_workspaces(self):
        return AccountWorkspaces(self.account_client, self.workspace_ids)

    @cached_property
    def account_aggregate(self):
        return AccountAggregate(self.account_workspaces)

    @cached_property
    def is_account_install(self):
        return environ.get("UCX_FORCE_INSTALL") == "account"

    @cached_property
    def account_metastores(self):
        return AccountMetastores(self.account_client)

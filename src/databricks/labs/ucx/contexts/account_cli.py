from functools import cached_property

from databricks.sdk import AccountClient


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
    def workspace_ids(self):
        return [int(_.strip()) for _ in self.named_parameters.get("workspace_ids", "").split(",") if _]

    @cached_property
    def account_workspaces(self):
        return AccountWorkspaces(self.account_client, self.workspace_ids)

    @cached_property
    def account_aggregate(self):
        return AccountAggregate(self.account_workspaces)

    @cached_property
    def account_metastores(self):
        return AccountMetastores(self.account_client)

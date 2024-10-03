from functools import cached_property
from os import environ

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
    def workspace_ids(self) -> list[int]:
        return [int(_.strip()) for _ in self.named_parameters.get("workspace_ids", "").split(",") if _]

    @cached_property
    def account_workspaces(self) -> AccountWorkspaces:
        return AccountWorkspaces(self.account_client, self.workspace_ids)

    @cached_property
    def account_aggregate(self) -> AccountAggregate:
        return AccountAggregate(self.account_workspaces)

    @cached_property
    def is_account_install(self) -> bool:
        return environ.get("UCX_FORCE_INSTALL") == "account"

    @cached_property
    def account_metastores(self) -> AccountMetastores:
        return AccountMetastores(self.account_client)

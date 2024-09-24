from functools import cached_property
from os import environ

from databricks.sdk import AccountClient, WorkspaceClient

from databricks.labs.ucx.account.aggregate import AccountAggregate
from databricks.labs.ucx.account.metastores import AccountMetastores
from databricks.labs.ucx.account.workspaces import AccountWorkspaces
from databricks.labs.ucx.contexts.application import CliContext
from databricks.labs.ucx.hive_metastore.catalog_schema import CatalogSchema


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
        if workspace_id is None:
            raise ValueError(f"Missing workspace id to create workspace client: {self.named_parameters}")
        workspace = self._ac.workspaces.get(int(workspace_id))
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

    @cached_property
    def catalog_schema(self):
        # TODO: Split `CatalogSchema` into `Catalogs` and `Schemas` https://github.com/databrickslabs/ucx/issues/2735
        return CatalogSchema(self.workspace_client, self.config.ucx_catalog)

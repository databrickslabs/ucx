from dataclasses import dataclass

from databricks.labs.blueprint.installation import Installation
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import Privilege

from databricks.labs.ucx.assessment.crawlers import logger
from databricks.labs.ucx.azure.resources import AzureResource, AzureResources
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.framework.crawlers import StatementExecutionBackend
from databricks.labs.ucx.hive_metastore.locations import ExternalLocations


@dataclass
class StoragePermissionMapping:
    prefix: str
    client_id: str
    principal: str
    privilege: str
    # Need this directory_id/tenant_id when create UC storage credentials using service principal
    directory_id: str


class AzureResourcePermissions:
    def __init__(
        self,
        installation: Installation,
        ws: WorkspaceClient,
        azurerm: AzureResources,
        external_locations: ExternalLocations,
    ):
        self._filename = 'azure_storage_account_info.csv'
        self._installation = installation
        self._locations = external_locations
        self._azurerm = azurerm
        self._ws = ws
        self._levels = {
            "Storage Blob Data Contributor": Privilege.WRITE_FILES,
            "Storage Blob Data Owner": Privilege.WRITE_FILES,
            "Storage Blob Data Reader": Privilege.READ_FILES,
        }

    @classmethod
    def for_cli(cls, ws: WorkspaceClient, product='ucx', include_subscriptions=None):
        installation = Installation.current(ws, product)
        config = installation.load(WorkspaceConfig)
        sql_backend = StatementExecutionBackend(ws, config.warehouse_id)
        azurerm = AzureResources(ws, include_subscriptions=include_subscriptions)
        locations = ExternalLocations(ws, sql_backend, config.inventory_database)
        return cls(installation, ws, azurerm, locations)

    def _map_storage(self, storage: AzureResource) -> list[StoragePermissionMapping]:
        logger.info(f"Fetching role assignment for {storage.storage_account}")
        out = []
        for container in self._azurerm.containers(storage):
            for role_assignment in self._azurerm.role_assignments(str(container)):
                # one principal may be assigned multiple roles with overlapping dataActions, hence appearing
                # here in duplicates. hence, role name -> permission level is not enough for the perfect scenario.
                if role_assignment.role_name not in self._levels:
                    continue
                privilege = self._levels[role_assignment.role_name].value
                out.append(
                    StoragePermissionMapping(
                        prefix=f"abfss://{container.container}@{container.storage_account}.dfs.core.windows.net/",
                        client_id=role_assignment.principal.client_id,
                        principal=role_assignment.principal.display_name,
                        privilege=privilege,
                        directory_id = role_assignment.principal.directory_id
                    )
                )
        return out

    def save_spn_permissions(self) -> str | None:
        used_storage_accounts = self._get_storage_accounts()
        if len(used_storage_accounts) == 0:
            logger.warning(
                "There are no external table present with azure storage account. "
                "Please check if assessment job is run"
            )
            return None
        storage_account_infos = []
        for storage in self._azurerm.storage_accounts():
            if storage.storage_account not in used_storage_accounts:
                continue
            for mapping in self._map_storage(storage):
                storage_account_infos.append(mapping)
        if len(storage_account_infos) == 0:
            logger.error("No storage account found in current tenant with spn permission")
            return None
        return self._installation.save(storage_account_infos, filename=self._filename)

    def load(self):
        return self._installation.load(list[StoragePermissionMapping], filename=self._filename)

    def _get_storage_accounts(self) -> list[str]:
        external_locations = self._locations.snapshot()
        storage_accounts = []
        for location in external_locations:
            if location.location.startswith("abfss://"):
                start = location.location.index("@")
                end = location.location.index(".dfs.core.windows.net")
                storage_acct = location.location[start + 1 : end]
                if storage_acct not in storage_accounts:
                    storage_accounts.append(storage_acct)
        return storage_accounts

    def load_spn_permission(self, customized_csv: str) -> list[StoragePermissionMapping]:
        """
        Load StoragePermissionMapping info from azure_storage_account_info.csv
        """
        return self._installation.load(list[StoragePermissionMapping], self._filename)

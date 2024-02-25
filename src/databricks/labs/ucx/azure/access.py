import json
from dataclasses import dataclass

from databricks.labs.blueprint.installation import Installation
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.catalog import Privilege

from databricks.labs.ucx.assessment.crawlers import logger
from databricks.labs.ucx.azure.resources import (
    AzureAPIClient,
    AzureResource,
    AzureResources,
    Principal,
)
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
        api_client = AzureAPIClient(ws)
        azurerm = AzureResources(include_subscriptions=include_subscriptions, api_client=api_client)
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
                        directory_id=role_assignment.principal.directory_id,
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
        for storage in used_storage_accounts:
            for mapping in self._map_storage(storage):
                storage_account_infos.append(mapping)
        if len(storage_account_infos) == 0:
            logger.error("No storage account found in current tenant with spn permission")
            return None
        return self._installation.save(storage_account_infos, filename=self._filename)

    def _update_cluster_policy_definition(
        self, policy_definition: str, storage_accounts: list[AzureResource], global_principal: Principal
    ) -> str:
        policy_dict = json.loads(policy_definition)
        tenant_id = self._azurerm.tenant_id()
        endpoint = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
        for storage in storage_accounts:
            policy_dict[
                f"spark_conf.fs.azure.account.oauth2.client.id.{storage.storage_account}.dfs.core.windows.net"
            ] = self._policy_config(global_principal.client_id)
            policy_dict[
                f"spark_conf.fs.azure.account.oauth.provider.type.{storage.storage_account}.dfs.core.windows.net"
            ] = self._policy_config("org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
            policy_dict[
                f"spark_conf.fs.azure.account.oauth2.client.endpoint.{storage.storage_account}.dfs.core.windows.net"
            ] = self._policy_config(endpoint)
            policy_dict[f"spark_conf.fs.azure.account.auth.type.{storage.storage_account}.dfs.core.windows.net"] = (
                self._policy_config("OAuth")
            )
            if global_principal.secret is not None:
                policy_dict[
                    f"spark_conf.fs.azure.account.oauth2.client.secret.{storage.storage_account}.dfs.core.windows.net"
                ] = self._policy_config(global_principal.secret)
        return json.dumps(policy_dict)

    @staticmethod
    def _policy_config(value: str):
        return {"type": "fixed", "value": value}

    def _update_cluster_policy_with_spn(
        self, policy_id: str, storage_accounts: list[AzureResource], global_principal: Principal
    ):
        try:
            policy_definition = ""
            cluster_policy = self._ws.cluster_policies.get(policy_id)
            #self._installation.save(cluster_policy.as_dict(), filename="policy-backup.json")
            if cluster_policy.definition is not None:
                policy_definition = self._update_cluster_policy_definition(
                    cluster_policy.definition, storage_accounts, global_principal
                )
            if cluster_policy.name is not None:
                self._ws.cluster_policies.edit(policy_id, cluster_policy.name, definition=policy_definition)
        except NotFound:
            msg = f"cluster policy {policy_id} not found, please run UCX installation to create UCX cluster policy"
            raise NotFound(msg) from None

    def create_global_spn(self):
        config = self._installation.load(WorkspaceConfig)
        policy_id = config.policy_id
        if policy_id is None:
            logger.error(
                "UCX cluster policy not found in config. Please run latest UCX installation to set cluster policy"
            )
            return
        if config.global_spn_id is not None:
            logger.error("Global service principal already created for this workspace.")
            return
        used_storage_accounts = self._get_storage_accounts()
        if len(used_storage_accounts) == 0:
            logger.warning(
                "There are no external table present with azure storage account. "
                "Please check if assessment job is run"
            )
            return
        logger.info("Creating service principal")
        global_principal = self._azurerm.create_service_principal()
        config = self._installation.load(WorkspaceConfig)
        config.global_spn_id = global_principal.client_id
        self._installation.save(config)
        logger.info(f"Created service principal of client_id {global_principal.client_id}")
        logger.info("Applying permission on storage accounts")
        for storage in used_storage_accounts:
            self._azurerm.apply_storage_permission(global_principal.object_id, storage, "STORAGE_BLOB_READER")
            logger.debug(
                f"Storage Data Blob Reader permission applied for spn {global_principal.client_id} "
                f"to storage account {storage.storage_account}"
            )
        self._update_cluster_policy_with_spn(policy_id, used_storage_accounts, global_principal)

        logger.info(f"Update UCX cluster policy {policy_id} with spn connection details for storage accounts")

    def load(self):
        return self._installation.load(list[StoragePermissionMapping], filename=self._filename)

    def _get_storage_accounts(self) -> list[AzureResource]:
        external_locations = self._locations.snapshot()
        storage_accounts = []
        azure_storage = []
        for location in external_locations:
            if location.location.startswith("abfss://"):
                start = location.location.index("@")
                end = location.location.index(".dfs.core.windows.net")
                storage_acct = location.location[start + 1 : end]
                if storage_acct not in storage_accounts:
                    storage_accounts.append(storage_acct)
        for storage in self._azurerm.storage_accounts():
            if storage.storage_account in storage_accounts:
                azure_storage.append(storage)
        return azure_storage

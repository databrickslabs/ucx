import json
import uuid
from dataclasses import dataclass

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.tui import Prompts
from databricks.labs.lsql.backends import StatementExecutionBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, ResourceAlreadyExists
from databricks.sdk.service.catalog import Privilege

from databricks.labs.ucx.assessment.crawlers import logger
from databricks.labs.ucx.azure.resources import (
    AzureAPIClient,
    AzureResource,
    AzureResources,
    PrincipalSecret,
)
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.hive_metastore.locations import ExternalLocations


@dataclass
class StoragePermissionMapping:
    prefix: str
    client_id: str
    principal: str
    privilege: str
    type: str
    # Need this directory_id/tenant_id when create UC storage credentials using service principal
    directory_id: str | None = None


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
        azure_mgmt_client = AzureAPIClient(
            ws.config.arm_environment.resource_manager_endpoint,
            ws.config.arm_environment.service_management_endpoint,
        )
        graph_client = AzureAPIClient("https://graph.microsoft.com", "https://graph.microsoft.com")
        azurerm = AzureResources(azure_mgmt_client, graph_client, include_subscriptions)
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
                        type=role_assignment.principal.type,
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
        for storage in self._azurerm.storage_accounts():
            if storage.storage_account not in used_storage_accounts:
                continue
            for mapping in self._map_storage(storage):
                storage_account_infos.append(mapping)
        if len(storage_account_infos) == 0:
            logger.error("No storage account found in current tenant with spn permission")
            return None
        return self._installation.save(storage_account_infos, filename=self._filename)

    def _update_cluster_policy_definition(
        self,
        policy_definition: str,
        storage_accounts: list[AzureResource],
        uber_principal: PrincipalSecret,
        inventory_database: str,
    ) -> str:
        policy_dict = json.loads(policy_definition)
        tenant_id = self._azurerm.tenant_id()
        endpoint = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
        for storage in storage_accounts:
            policy_dict[
                f"spark_conf.fs.azure.account.oauth2.client.id.{storage.storage_account}.dfs.core.windows.net"
            ] = self._policy_config(uber_principal.client.client_id)
            policy_dict[
                f"spark_conf.fs.azure.account.oauth.provider.type.{storage.storage_account}.dfs.core.windows.net"
            ] = self._policy_config("org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
            policy_dict[
                f"spark_conf.fs.azure.account.oauth2.client.endpoint.{storage.storage_account}.dfs.core.windows.net"
            ] = self._policy_config(endpoint)
            policy_dict[f"spark_conf.fs.azure.account.auth.type.{storage.storage_account}.dfs.core.windows.net"] = (
                self._policy_config("OAuth")
            )
            policy_dict[
                f"spark_conf.fs.azure.account.oauth2.client.secret.{storage.storage_account}.dfs.core.windows.net"
            ] = self._policy_config(f"{{secrets/{inventory_database}/uber_principal_secret}}")
        return json.dumps(policy_dict)

    @staticmethod
    def _policy_config(value: str):
        return {"type": "fixed", "value": value}

    def _update_cluster_policy_with_spn(
        self,
        policy_id: str,
        storage_accounts: list[AzureResource],
        uber_principal: PrincipalSecret,
        inventory_database: str,
    ):
        try:
            policy_definition = ""
            cluster_policy = self._ws.cluster_policies.get(policy_id)

            self._installation.save(cluster_policy, filename="policy-backup.json")

            if cluster_policy.definition is not None:
                policy_definition = self._update_cluster_policy_definition(
                    cluster_policy.definition, storage_accounts, uber_principal, inventory_database
                )
            if cluster_policy.name is not None:
                self._ws.cluster_policies.edit(policy_id, cluster_policy.name, definition=policy_definition)
        except NotFound:
            msg = f"cluster policy {policy_id} not found, please run UCX installation to create UCX cluster policy"
            raise NotFound(msg) from None

    def create_uber_principal(self, prompts: Prompts):
        config = self._installation.load(WorkspaceConfig)
        inventory_database = config.inventory_database
        display_name = f"unity-catalog-migration-{inventory_database}-{self._ws.get_workspace_id()}"
        uber_principal_name = prompts.question(
            "Enter a name for the uber service principal to be created", default=display_name
        )
        policy_id = config.policy_id
        if policy_id is None:
            msg = "UCX cluster policy not found in config. Please run latest UCX installation to set cluster policy"
            logger.error(msg)
            raise ValueError(msg) from None
        if config.uber_spn_id is not None:
            logger.warning("Uber service principal already created for this workspace.")
            return
        used_storage_accounts = self._get_storage_accounts()
        if len(used_storage_accounts) == 0:
            logger.warning(
                "There are no external table present with azure storage account. "
                "Please check if assessment job is run"
            )
            return
        storage_account_info = []
        for storage in self._azurerm.storage_accounts():
            if storage.storage_account in used_storage_accounts:
                storage_account_info.append(storage)
        logger.info("Creating service principal")
        uber_principal = self._azurerm.create_service_principal(uber_principal_name)
        self._create_scope(uber_principal, inventory_database)
        config.uber_spn_id = uber_principal.client.client_id
        logger.info(
            f"Created service principal of client_id {config.uber_spn_id}. " f"Applying permission on storage accounts"
        )
        try:
            self._apply_storage_permission(storage_account_info, uber_principal)
            self._installation.save(config)
            self._update_cluster_policy_with_spn(policy_id, storage_account_info, uber_principal, inventory_database)
        except PermissionError:
            self._azurerm.delete_service_principal(uber_principal.client.object_id)
        logger.info(f"Update UCX cluster policy {policy_id} with spn connection details for storage accounts")

    def _apply_storage_permission(self, storage_account_info: list[AzureResource], uber_principal: PrincipalSecret):
        for storage in storage_account_info:
            role_name = str(uuid.uuid4())
            self._azurerm.apply_storage_permission(
                uber_principal.client.object_id, storage, "STORAGE_BLOB_DATA_READER", role_name
            )
            logger.debug(
                f"Storage Data Blob Reader permission applied for spn {uber_principal.client.client_id} "
                f"to storage account {storage.storage_account}"
            )

    def _create_scope(self, uber_principal: PrincipalSecret, inventory_database: str):
        logger.info(f"Creating secret scope {inventory_database}.")
        try:
            self._ws.secrets.create_scope(inventory_database)
        except ResourceAlreadyExists:
            logger.warning(f"Secret scope {inventory_database} already exists, using the same")
        self._ws.secrets.put_secret(inventory_database, "uber_principal_secret", string_value=uber_principal.secret)

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

import json
import logging
import re
import uuid
from collections.abc import ValuesView
from dataclasses import dataclass
from functools import partial

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.parallel import ManyError, Threads
from databricks.labs.blueprint.tui import Prompts
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, ResourceAlreadyExists
from databricks.sdk.service.catalog import Privilege
from databricks.sdk.service.sql import EndpointConfPair

from databricks.labs.ucx.azure.resources import (
    AccessConnector,
    AzureResources,
    AzureRoleAssignment,
    PrincipalSecret,
    StorageAccount,
)
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.hive_metastore.locations import ExternalLocations

logger = logging.getLogger(__name__)


@dataclass
class StoragePermissionMapping:
    prefix: str
    client_id: str
    principal: str
    privilege: str
    type: str
    default_network_action: str = "Unknown"  # "Deny", "Allow" or "Unknown"
    # Need this directory_id/tenant_id when create UC storage credentials using service principal
    directory_id: str | None = None


class AzureResourcePermissions:
    FILENAME = 'azure_storage_account_info.csv'

    def __init__(
        self,
        installation: Installation,
        ws: WorkspaceClient,
        azurerm: AzureResources,
        external_locations: ExternalLocations,
    ):
        self._installation = installation
        self._locations = external_locations
        self._azurerm = azurerm
        self._ws = ws
        self._levels = {
            "Storage Blob Data Contributor": Privilege.WRITE_FILES,
            "Storage Blob Data Owner": Privilege.WRITE_FILES,
            "Storage Blob Data Reader": Privilege.READ_FILES,
        }
        self._permission_levels = {
            "Microsoft.Storage/storageAccounts/blobServices/containers/write": Privilege.WRITE_FILES,
            "Microsoft.Storage/storageAccounts/blobServices/containers/blobs/write": Privilege.WRITE_FILES,
            "Microsoft.Storage/storageAccounts/blobServices/containers/read": Privilege.READ_FILES,
            "Microsoft.Storage/storageAccounts/blobServices/containers/blobs/read": Privilege.READ_FILES,
        }

    def _get_permission_level(self, permission_to_match: str) -> Privilege | None:
        # String might contain '*', check for wildcard match
        pattern = re.sub(r'\*', '.*', permission_to_match)
        permission_compiled = re.compile(pattern)
        for each_level, privilege_level in self._permission_levels.items():
            # Check for storage blob permission with regex to account for star pattern
            match = permission_compiled.match(each_level)
            # If a match is found, return the privilege level, no need to check for lower levels
            if match:
                return privilege_level
        return None

    def _get_custom_role_privilege(self, role_permissions: list[str]) -> Privilege | None:
        # If both read and write privileges are found, only write privilege will be considered
        higher_privilege = None
        for each_permission in role_permissions:
            privilege = self._get_permission_level(each_permission)
            if privilege is None:
                continue
            # WRITE_FILES is the higher permission, don't need to check further
            if privilege == Privilege.WRITE_FILES:
                return privilege
            if privilege == Privilege.READ_FILES:
                higher_privilege = privilege
        return higher_privilege

    def _get_role_privilege(self, role_assignment: AzureRoleAssignment) -> Privilege | None:
        privilege = None
        # Check for custom role permissions on the storage accounts
        if role_assignment.role_permissions:
            privilege = self._get_custom_role_privilege(role_assignment.role_permissions)
        elif role_assignment.role_name in self._levels:
            privilege = self._levels[role_assignment.role_name]
        return privilege

    def _map_storage(self, storage: StorageAccount) -> ValuesView[StoragePermissionMapping]:
        logger.info(f"Fetching role assignment for {storage.name}")
        principal_spm_mapping: dict[str, StoragePermissionMapping] = {}
        for container in self._azurerm.containers(storage.id):
            for role_assignment in self._azurerm.role_assignments(str(container)):
                # Skip the role assignments that already have WRITE_FILES privilege
                spm_mapping_key = f"{container.container}_{role_assignment.principal.client_id}"
                if (
                    spm_mapping_key in principal_spm_mapping
                    and principal_spm_mapping[spm_mapping_key].privilege == Privilege.WRITE_FILES.value
                ):
                    continue
                # one principal may be assigned multiple roles with overlapping dataActions, hence appearing
                # here in duplicates. hence, role name -> permission level is not enough for the perfect scenario.
                returned_privilege = self._get_role_privilege(role_assignment)
                if not returned_privilege:
                    continue
                privilege = returned_privilege.value
                principal_spm_mapping[spm_mapping_key] = StoragePermissionMapping(
                    prefix=f"abfss://{container.container}@{container.storage_account}.dfs.core.windows.net/",
                    client_id=role_assignment.principal.client_id,
                    principal=role_assignment.principal.display_name,
                    privilege=privilege,
                    type=role_assignment.principal.type,
                    default_network_action=storage.default_network_action,
                    directory_id=role_assignment.principal.directory_id,
                )
        return principal_spm_mapping.values()

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
            if storage.name not in used_storage_accounts:
                continue
            for mapping in self._map_storage(storage):
                storage_account_infos.append(mapping)
        if len(storage_account_infos) == 0:
            logger.error("No storage account found in current tenant with spn permission")
            return None
        return self._installation.save(storage_account_infos, filename=self.FILENAME)

    def _update_cluster_policy_definition(
        self,
        policy_definition: str,
        storage_accounts: list[StorageAccount],
        uber_principal: PrincipalSecret,
        inventory_database: str,
    ) -> str:
        policy_dict = json.loads(policy_definition)
        tenant_id = self._azurerm.tenant_id()
        endpoint = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
        for storage in storage_accounts:
            policy_dict[f"spark_conf.fs.azure.account.oauth2.client.id.{storage.name}.dfs.core.windows.net"] = (
                self._policy_config(uber_principal.client.client_id)
            )
            policy_dict[f"spark_conf.fs.azure.account.oauth.provider.type.{storage.name}.dfs.core.windows.net"] = (
                self._policy_config("org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
            )
            policy_dict[f"spark_conf.fs.azure.account.oauth2.client.endpoint.{storage.name}.dfs.core.windows.net"] = (
                self._policy_config(endpoint)
            )
            policy_dict[f"spark_conf.fs.azure.account.auth.type.{storage.name}.dfs.core.windows.net"] = (
                self._policy_config("OAuth")
            )
            policy_dict[f"spark_conf.fs.azure.account.oauth2.client.secret.{storage.name}.dfs.core.windows.net"] = (
                self._policy_config("{{secrets/" + inventory_database + "/uber_principal_secret}}")
            )
        return json.dumps(policy_dict)

    @staticmethod
    def _policy_config(value: str):
        return {"type": "fixed", "value": value}

    def _update_cluster_policy_with_spn(
        self,
        policy_id: str,
        storage_accounts: list[StorageAccount],
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

    def _update_sql_dac_with_spn(
        self,
        storage_account_info: list[StorageAccount],
        uber_principal: PrincipalSecret,
        inventory_database: str,
    ):

        warehouse_config = self._ws.warehouses.get_workspace_warehouse_config()
        sql_dac = warehouse_config.data_access_config
        if sql_dac is None:
            sql_dac = []
        tenant_id = self._azurerm.tenant_id()
        endpoint = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
        for storage in storage_account_info:
            sql_dac.extend(
                [
                    EndpointConfPair(
                        f"spark_conf.fs.azure.account.oauth2.client.id.{storage.name}.dfs.core.windows.net",
                        uber_principal.client.client_id,
                    ),
                    EndpointConfPair(
                        f"spark_conf.fs.azure.account.oauth.provider.type.{storage.name}.dfs.core.windows.net",
                        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                    ),
                    EndpointConfPair(
                        f"spark_conf.fs.azure.account.oauth2.client.endpoint.{storage.name}.dfs.core.windows.net",
                        endpoint,
                    ),
                    EndpointConfPair(
                        f"spark_conf.fs.azure.account.auth.type.{storage.name}.dfs.core.windows.net", "OAuth"
                    ),
                    EndpointConfPair(
                        f"spark_conf.fs.azure.account.oauth2.client.secret.{storage.name}.dfs.core.windows.net",
                        "{{secrets/" + inventory_database + "/uber_principal_secret}}",
                    ),
                ]
            )
        self._ws.warehouses.set_workspace_warehouse_config(
            data_access_config=sql_dac,
            sql_configuration_parameters=warehouse_config.sql_configuration_parameters,
        )

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
            if storage.name in used_storage_accounts:
                storage_account_info.append(storage)
        logger.info("Creating service principal")
        uber_principal = self._azurerm.create_service_principal(uber_principal_name)
        self._create_scope(uber_principal, inventory_database)
        config.uber_spn_id = uber_principal.client.client_id
        logger.info(
            f"Created service principal of client_id {config.uber_spn_id}. " f"Applying permission on storage accounts"
        )
        try:
            self._apply_storage_permission(
                uber_principal.client.object_id, "STORAGE_BLOB_DATA_CONTRIBUTOR", *storage_account_info
            )
            self._installation.save(config)
            self._update_cluster_policy_with_spn(policy_id, storage_account_info, uber_principal, inventory_database)
            self._update_sql_dac_with_spn(storage_account_info, uber_principal, inventory_database)
        except PermissionError:
            self._azurerm.delete_service_principal(uber_principal.client.object_id)
        logger.info(f"Update UCX cluster policy {policy_id} with spn connection details for storage accounts")

    def _create_access_connector_for_storage_account(
        self, storage_account: StorageAccount, role_name: str = "STORAGE_BLOB_DATA_READER"
    ) -> tuple[AccessConnector, str]:
        access_connector = self._azurerm.create_or_update_access_connector(
            storage_account.id.subscription_id,
            storage_account.id.resource_group,
            f"ac-{storage_account.name}",
            storage_account.location,
            tags={"CreatedBy": "ucx"},
            wait_for_provisioning=True,
        )
        self._apply_storage_permission(access_connector.principal_id, role_name, storage_account)

        container = next(self._azurerm.containers(storage_account.id), None)
        if container is None:
            url = f"abfss://{storage_account.name}.dfs.core.windows.net/"
        else:
            url = f"abfss://{container.container}@{container.storage_account}.dfs.core.windows.net/"

        return access_connector, url

    def create_access_connectors_for_storage_accounts(self) -> list[tuple[AccessConnector, str]]:
        """Create access connectors for storage accounts

        Returns:
            list[AccessConnector, str] : The access connectors with a storage url to which it has access.
        """
        used_storage_accounts = self._get_storage_accounts()
        if len(used_storage_accounts) == 0:
            logger.warning(
                "There are no external table present with azure storage account. "
                "Please check if assessment job is run"
            )
            return []

        tasks = []
        for storage_account in self._azurerm.storage_accounts():
            if storage_account.name not in used_storage_accounts:
                continue
            task = partial(
                self._create_access_connector_for_storage_account,
                storage_account=storage_account,
                # Fine-grained access is configured within Databricks through unity
                role_name="STORAGE_BLOB_DATA_CONTRIBUTOR",
            )
            tasks.append(task)

        thread_name = "Creating access connectors for storage accounts"
        results, errors = Threads.gather(thread_name, tasks)
        if len(errors) > 0:
            raise ManyError(errors)
        return list(results)

    def _apply_storage_permission(
        self,
        principal_id: str,
        role_name: str,
        *storage_accounts: StorageAccount,
    ):
        for storage in storage_accounts:
            role_guid = str(uuid.uuid4())
            self._azurerm.apply_storage_permission(principal_id, storage, role_name, role_guid)
            logger.debug(f"{role_name} permission applied for spn {principal_id} to storage account {storage.name}")

    def _create_scope(self, uber_principal: PrincipalSecret, inventory_database: str):
        logger.info(f"Creating secret scope {inventory_database}.")
        try:
            self._ws.secrets.create_scope(inventory_database)
        except ResourceAlreadyExists:
            logger.warning(f"Secret scope {inventory_database} already exists, using the same")
        self._ws.secrets.put_secret(inventory_database, "uber_principal_secret", string_value=uber_principal.secret)

    def load(self):
        return self._installation.load(list[StoragePermissionMapping], filename=self.FILENAME)

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

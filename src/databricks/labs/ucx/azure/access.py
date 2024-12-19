import json
import logging
import re
import uuid
from collections.abc import Callable, ValuesView
from dataclasses import dataclass
from functools import partial, wraps
from datetime import timedelta
from typing import Any, ParamSpec, TypeVar

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.parallel import ManyError, Threads
from databricks.labs.blueprint.tui import Prompts
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import ApiClient
from databricks.sdk.errors import (
    BadRequest,
    DatabricksError,
    InvalidParameterValue,
    NotFound,
    PermissionDenied,
    ResourceAlreadyExists,
    ResourceDoesNotExist,
)
from databricks.sdk.retries import retried
from databricks.sdk.service.catalog import Privilege
from databricks.sdk.service.compute import Policy
from databricks.sdk.service.sql import (
    Channel,
    EndpointConfPair,
    GetWorkspaceWarehouseConfigResponse,
    RepeatedEndpointConfPairs,
    SetWorkspaceWarehouseConfigRequestSecurityPolicy,
    WarehouseTypePair,
)
from databricks.sdk.service.workspace import GetSecretResponse

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
P = ParamSpec('P')
R = TypeVar('R')


def set_workspace_warehouse_config_wrapper(  # pylint: disable=too-many-arguments,missing-param-doc
    api: ApiClient,
    *,
    channel: Channel | None = None,
    config_param: RepeatedEndpointConfPairs | None = None,
    data_access_config: list[EndpointConfPair] | None = None,
    enabled_warehouse_types: list[WarehouseTypePair] | None = None,
    global_param: RepeatedEndpointConfPairs | None = None,
    google_service_account: str | None = None,
    instance_profile_arn: str | None = None,
    security_policy: SetWorkspaceWarehouseConfigRequestSecurityPolicy | None = None,
    sql_configuration_parameters: RepeatedEndpointConfPairs | None = None,
    enable_serverless_compute: bool = False,
):
    """Sets the workspace level configuration that is shared by all SQL warehouses in a workspace.

    See :meth:WorkspaceClient.warehouses.set_workspace_warehouse_config.

    :param enable_serverless_compute: bool (optional)
        Enable serverless compute. Note that this value does not enforce serverless compute but it allows for serverless
        compute when `True`. Otherwise, serverless compute it not allowed.

    TODO: Once https://github.com/databricks/databricks-sdk-py/issues/305 is fixed this wrapper should be discarded.
    """
    body: dict[Any, Any] = {}
    if channel is not None:
        body['channel'] = channel.as_dict()
    if config_param is not None:
        body['config_param'] = config_param.as_dict()
    if data_access_config is not None:
        body['data_access_config'] = [v.as_dict() for v in data_access_config]
    if enabled_warehouse_types is not None:
        body['enabled_warehouse_types'] = [v.as_dict() for v in enabled_warehouse_types]
    if global_param is not None:
        body['global_param'] = global_param.as_dict()
    if google_service_account is not None:
        body['google_service_account'] = google_service_account
    if instance_profile_arn is not None:
        body['instance_profile_arn'] = instance_profile_arn
    if security_policy is not None:
        body['security_policy'] = security_policy.value
    if sql_configuration_parameters is not None:
        body['sql_configuration_parameters'] = sql_configuration_parameters.as_dict()
    body['enable_serverless_compute'] = str(enable_serverless_compute).lower()

    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
    }

    api.do('PUT', '/api/2.0/sql/config/warehouses', body=body, headers=headers)


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
    _UBER_PRINCIPAL_SECRET_KEY = "uber_principal_secret"

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
        storage_accounts = self._get_storage_accounts()
        if len(storage_accounts) == 0:
            logger.warning(
                "There are no external table present with azure storage account. "
                "Please check if assessment job is run"
            )
            return None
        storage_account_infos = []
        for storage in storage_accounts:
            for mapping in self._map_storage(storage):
                storage_account_infos.append(mapping)
        if len(storage_account_infos) == 0:
            logger.error("No storage account found in current tenant with spn permission")
            return None
        return self._installation.save(storage_account_infos, filename=self.FILENAME)

    @staticmethod
    def _policy_config(value: str) -> dict[str, str]:
        return {"type": "fixed", "value": value}

    def _create_service_principal_cluster_policy_configuration_pairs(
        self,
        principal_client_id: str,
        principal_secret_identifier: str,
        storage: StorageAccount,
        *,
        configuration_prefix: str,
    ) -> list[tuple[str, dict[str, str]]]:
        """Create the cluster policy configuration pairs to access the storage"""
        tenant_id = self._azurerm.tenant_id()
        endpoint = f"https://login.microsoftonline.com/{tenant_id}/oauth2/token"
        configuration_pairs = [
            (
                f"{configuration_prefix}.fs.azure.account.oauth2.client.id.{storage.name}.dfs.core.windows.net",
                self._policy_config(principal_client_id),
            ),
            (
                f"{configuration_prefix}.fs.azure.account.oauth.provider.type.{storage.name}.dfs.core.windows.net",
                self._policy_config("org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"),
            ),
            (
                f"{configuration_prefix}.fs.azure.account.oauth2.client.endpoint.{storage.name}.dfs.core.windows.net",
                self._policy_config(endpoint),
            ),
            (
                f"{configuration_prefix}.fs.azure.account.auth.type.{storage.name}.dfs.core.windows.net",
                self._policy_config("OAuth"),
            ),
            (
                f"{configuration_prefix}.fs.azure.account.oauth2.client.secret.{storage.name}.dfs.core.windows.net",
                self._policy_config("{{" + principal_secret_identifier + "}}"),
            ),
        ]
        return configuration_pairs

    def _add_service_principal_configuration_to_cluster_policy_definition(
        self,
        policy_definition: str,
        principal_client_id: str,
        principal_secret_identifier: str,
        storage_accounts: list[StorageAccount],
    ) -> str:
        policy_dict = json.loads(policy_definition)
        for storage in storage_accounts:
            for key, value in self._create_service_principal_cluster_policy_configuration_pairs(
                principal_client_id,
                principal_secret_identifier,
                storage,
                configuration_prefix="spark_conf",
            ):
                policy_dict[key] = value
        return json.dumps(policy_dict)

    def _add_service_principal_configuration_to_cluster_policy(
        self,
        policy_id: str,
        principal_client_id: str,
        principal_secret_identifier: str,
        storage_accounts: list[StorageAccount],
    ):
        policy_definition = ""
        try:
            cluster_policy = self._ws.cluster_policies.get(policy_id)
            self._installation.save(cluster_policy, filename="policy-backup.json")
            if cluster_policy.definition is not None:
                policy_definition = self._add_service_principal_configuration_to_cluster_policy_definition(
                    cluster_policy.definition,
                    principal_client_id,
                    principal_secret_identifier,
                    storage_accounts,
                )
            if cluster_policy.name is not None:
                self._ws.cluster_policies.edit(policy_id, name=cluster_policy.name, definition=policy_definition)
            logger.info(
                f"Updated UCX cluster policy {policy_id} with service principal connection details for accesing storage accounts"
            )
        except NotFound:
            msg = f"cluster policy {policy_id} not found, please run UCX installation to create UCX cluster policy"
            raise NotFound(msg) from None

    def _remove_service_principal_configuration_from_cluster_policy_definition(
        self,
        policy_definition: str,
        principal_client_id: str,
        principal_secret_identifier: str,
        storage_accounts: list[StorageAccount],
    ) -> str:
        policy_dict = json.loads(policy_definition)
        for storage in storage_accounts:
            for key, _ in self._create_service_principal_cluster_policy_configuration_pairs(
                principal_client_id,
                principal_secret_identifier,
                storage,
                configuration_prefix="spark_conf",
            ):
                if key in policy_dict:
                    del policy_dict[key]
        return json.dumps(policy_dict)

    def _remove_service_principal_configuration_from_cluster_policy(
        self,
        policy_id: str,
        principal_client_id: str,
        principal_secret_identifier: str,
        storage_accounts: list[StorageAccount],
    ):
        """Revert the cluster policy."""
        try:
            policy = self._installation.load(Policy, filename="policy-backup.json")
        except NotFound:
            try:
                policy = self._ws.cluster_policies.get(policy_id)
            except (InvalidParameterValue, NotFound):
                return  # No policy to revert
        policy_definition = policy.definition
        if policy_definition is not None:
            policy_definition = self._remove_service_principal_configuration_from_cluster_policy_definition(
                policy_definition,
                principal_client_id,
                principal_secret_identifier,
                storage_accounts,
            )
        if policy.name is not None:
            self._ws.cluster_policies.edit(policy_id, name=policy.name, definition=policy_definition)

    def _create_storage_account_data_access_configuration_pairs(
        self,
        principal_client_id: str,
        principal_secret_identifier: str,
        storage: StorageAccount,
    ) -> list[EndpointConfPair]:
        """Create the data access configuration pairs to access the storage"""
        configuration_pairs = []
        for key, value in self._create_service_principal_cluster_policy_configuration_pairs(
            principal_client_id,
            principal_secret_identifier,
            storage,
            configuration_prefix="spark.hadoop",
        ):
            configuration_pairs.append(EndpointConfPair(key, value["value"]))
        return configuration_pairs

    def _add_service_principal_configuration_to_workspace_warehouse_config(
        self,
        principal_client_id: str,
        principal_secret_identifier: str,
        storage_accounts: list[StorageAccount],
    ):
        warehouse_config = self._ws.warehouses.get_workspace_warehouse_config()
        self._installation.save(warehouse_config, filename="warehouse-config-backup.json")

        sql_dac = warehouse_config.data_access_config or []
        for storage in storage_accounts:
            configuration_pairs = self._create_storage_account_data_access_configuration_pairs(
                principal_client_id, principal_secret_identifier, storage
            )
            sql_dac.extend(configuration_pairs)

        security_policy = (
            SetWorkspaceWarehouseConfigRequestSecurityPolicy(warehouse_config.security_policy.value)
            if warehouse_config.security_policy
            else SetWorkspaceWarehouseConfigRequestSecurityPolicy.NONE
        )

        succeeded_message = "Updated workspace warehouse config with service principal connection details for accessing storage accounts"
        sql_dac_log_msg = "\n".join(f"{config_pair.key} {config_pair.value}" for config_pair in sql_dac)
        error_message = (
            f"Adding uber principal to SQL warehouse Data Access Properties is failed using Python SDK with error. "
            f"Please try applying the following configs manually in the workspace admin UI:\n{sql_dac_log_msg}",
        )
        # TODO: Once https://github.com/databricks/databricks-sdk-py/issues/305 is fixed:
        # - Remove second try-except completely
        # - Remove first -except, only keeping the contents in the try-statement
        try:
            self._ws.warehouses.set_workspace_warehouse_config(
                data_access_config=sql_dac,
                sql_configuration_parameters=warehouse_config.sql_configuration_parameters,
                security_policy=security_policy,
            )
            logger.info(succeeded_message)
        except InvalidParameterValue as e:
            if "enable_serverless_compute" not in str(e):
                logger.error(error_message, exc_info=e)
                raise
        try:
            set_workspace_warehouse_config_wrapper(
                self._ws.api_client,
                data_access_config=sql_dac,
                sql_configuration_parameters=warehouse_config.sql_configuration_parameters,
                security_policy=security_policy,
                enable_serverless_compute=True,
            )
        except InvalidParameterValue as e:
            logger.error(error_message, exc_info=e)
            raise

    def _remove_service_principal_configuration_from_workspace_warehouse_config(  # pylint: disable=too-complex
        self,
        principal_client_id: str,
        principal_secret_identifier: str,
        storage_accounts: list[StorageAccount],
    ):
        try:
            warehouse_config = self._installation.load(
                GetWorkspaceWarehouseConfigResponse, filename="warehouse-config-backup.json"
            )
        except NotFound:  # For legacy reasons we can not assume the backup to always be present
            warehouse_config = self._ws.warehouses.get_workspace_warehouse_config()
        sql_dac = warehouse_config.data_access_config or []

        for storage_account in storage_accounts:
            configuration_pairs = self._create_storage_account_data_access_configuration_pairs(
                principal_client_id, principal_secret_identifier, storage_account
            )
            for configuration_pair in configuration_pairs:
                if configuration_pair in sql_dac:
                    sql_dac.remove(configuration_pair)

        security_policy = (
            SetWorkspaceWarehouseConfigRequestSecurityPolicy(warehouse_config.security_policy.value)
            if warehouse_config.security_policy
            else SetWorkspaceWarehouseConfigRequestSecurityPolicy.NONE
        )

        succeeded_message = "Updated workspace warehouse config with service principal connection details for accessing storage accounts"
        sql_dac_log_msg = "\n".join(f"{config_pair.key} {config_pair.value}" for config_pair in sql_dac)
        error_message = (
            f"Adding uber principal to SQL warehouse Data Access Properties is failed using Python SDK with error. "
            f"Please try applying the following configs manually in the workspace admin UI:\n{sql_dac_log_msg}",
        )
        # TODO: Once https://github.com/databricks/databricks-sdk-py/issues/305 is fixed:
        # - Remove second try-except completely
        # - Remove first -except, only keeping the contents in the try-statement
        try:
            self._ws.warehouses.set_workspace_warehouse_config(
                data_access_config=sql_dac,
                sql_configuration_parameters=warehouse_config.sql_configuration_parameters,
                security_policy=security_policy,
            )
            logger.info(succeeded_message)
        except InvalidParameterValue as e:
            if "enable_serverless_compute" not in str(e):
                logger.error(error_message, exc_info=e)
                raise
        try:
            set_workspace_warehouse_config_wrapper(
                self._ws.api_client,
                data_access_config=sql_dac,
                sql_configuration_parameters=warehouse_config.sql_configuration_parameters,
                security_policy=security_policy,
                enable_serverless_compute=True,
            )
        except InvalidParameterValue as e:
            logger.error(error_message, exc_info=e)
            raise

    def create_uber_principal(self, prompts: Prompts) -> None:
        config = self._installation.load(WorkspaceConfig)
        inventory_database = config.inventory_database
        display_name = f"unity-catalog-migration-{inventory_database}-{self._ws.get_workspace_id()}"
        policy_id = config.policy_id
        if policy_id is None:
            msg = "UCX cluster policy not found in config. Please run latest UCX installation to set cluster policy"
            logger.error(msg)
            raise ValueError(msg) from None
        if config.uber_spn_id is not None:
            logger.warning("Uber service principal already created for this workspace.")
            return
        storage_accounts = self._get_storage_accounts()
        if len(storage_accounts) == 0:
            logger.warning(
                "There are no external table present with azure storage account. "
                "Please check if assessment job is run"
            )
            return
        uber_principal_name = prompts.question(
            "Enter a name for the uber service principal to be created", default=display_name
        )
        logger.info("Creating service principal")

        secret_identifier = f"secrets/{inventory_database}/{self._UBER_PRINCIPAL_SECRET_KEY}"
        try:
            uber_principal = self._azurerm.create_service_principal(uber_principal_name)
            config.uber_spn_id = uber_principal.client.client_id
            self._installation.save(config)
            self._apply_storage_permission(
                uber_principal.client.object_id, "STORAGE_BLOB_DATA_CONTRIBUTOR", *storage_accounts
            )
            self._create_and_get_secret_for_uber_principal(uber_principal, inventory_database)
            self._add_service_principal_configuration_to_cluster_policy(
                policy_id,
                uber_principal.client.client_id,
                secret_identifier,
                storage_accounts,
            )
            self._add_service_principal_configuration_to_workspace_warehouse_config(
                uber_principal.client.client_id, secret_identifier, storage_accounts
            )
        except (PermissionDenied, NotFound, BadRequest):
            self._delete_uber_principal()  # Clean up dangling resources
            raise

    def _delete_uber_principal(self) -> None:

        def safe_call(function: Callable[P, R], *, error_message: str) -> Callable[P, R | None]:
            """Make a safe call of the function

            Args:
                function (Callable[P, R]) : Function to safely call.
                error_message (str) : Message to log if error raised by the function
            """

            @wraps(function)
            def wrapper(*args: Any, **kwargs: Any) -> R | None:
                try:
                    return function(*args, **kwargs)
                except DatabricksError as e:
                    logger.error(error_message, exc_info=e)
                    return None

            return wrapper

        message = "Missing permissions to load the configuration"
        config = safe_call(self._installation.load, error_message=message)(WorkspaceConfig)
        if config is None or config.uber_spn_id is None:
            return

        secret_identifier = f"secrets/{config.inventory_database}/{self._UBER_PRINCIPAL_SECRET_KEY}"
        storage_accounts = self._get_storage_accounts()

        storage_account_ids = ' '.join(str(st.id) for st in storage_accounts)
        message = f"Missing permissions to delete storage permissions for: {storage_account_ids}"
        safe_call(self._azurerm.delete_storage_permission, error_message=message)(
            config.uber_spn_id, *storage_accounts, safe=True
        )
        message = f"Missing permissions to delete service principal: {config.uber_spn_id}"
        safe_call(self._azurerm.delete_service_principal, error_message=message)(config.uber_spn_id, safe=True)
        if config.policy_id is not None:
            message = "Missing permissions to revert cluster policy"
            safe_call(self._remove_service_principal_configuration_from_cluster_policy, error_message=message)(
                config.policy_id, config.uber_spn_id, secret_identifier, storage_accounts
            )
        message = "Missing permissions to revert SQL warehouse config"
        safe_call(self._remove_service_principal_configuration_from_workspace_warehouse_config, error_message=message)(
            config.uber_spn_id, secret_identifier, storage_accounts
        )
        message = "Missing permissions to delete secret scope"
        safe_call(self._safe_delete_scope, error_message=message)(config.inventory_database)
        message = "Missing permissions to save the configuration"
        config.uber_spn_id = None
        safe_call(self._installation.save, error_message=message)(config)

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
        storage_accounts = self._get_storage_accounts()
        if len(storage_accounts) > 200:
            raise RuntimeWarning('Migration will breach UC limits (Storage Credentials > 200).')
        if len(storage_accounts) == 0:
            logger.warning(
                "There are no external table present with azure storage account. "
                "Please check if assessment job is run"
            )
            return []

        tasks = []
        for storage_account in storage_accounts:
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
            logger.error(
                "Error creating access connectors. Please review the error message and resolve the issue before trying again. "
                "Removing successfully created access connectors."
            )
            delete_access_connectors = [access_connector for access_connector, _ in results]
            self.delete_access_connectors(*delete_access_connectors)
            raise ManyError(errors)
        return list(results)

    def delete_storage_credential(self, *storage_credentials: Any):
        for storage_credential in storage_credentials:
            self._ws.storage_credentials.delete(storage_credential.name)

    def delete_access_connectors(self, *access_connectors: Any):
        for access_connector in access_connectors:
            self._azurerm.delete_access_connector(str(access_connector.id))

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

    def _create_and_get_secret_for_uber_principal(
        self,
        principal_secret: PrincipalSecret,
        scope: str,
    ) -> GetSecretResponse:
        """Create and get a workspace secret for the principal.

        If the secret scope does not, it wil be recreated. If the secret already exists, it will be overwritten.
        """
        logger.info(f"Creating secret scope {scope}.")
        try:
            self._ws.secrets.create_scope(scope)
        except ResourceAlreadyExists:
            logger.warning(f"Secret scope {scope} already exists, using the same")
        self._ws.secrets.put_secret(scope, self._UBER_PRINCIPAL_SECRET_KEY, string_value=principal_secret.secret)
        return self._get_secret(scope, self._UBER_PRINCIPAL_SECRET_KEY)

    @retried(on=[ResourceDoesNotExist], timeout=timedelta(minutes=1))
    def _get_secret(self, scope: str, secret: str) -> GetSecretResponse:
        return self._ws.secrets.get_secret(scope, secret)

    def _safe_delete_scope(self, scope: str) -> None:
        try:
            self._ws.secrets.delete_scope(scope)
        except ResourceDoesNotExist:
            logger.warning(f"Secret scope {scope} does not exist, skipping delete.")
        except PermissionDenied:
            logger.error(f"Missing permissions to delete secret scope: {scope}", exc_info=True)

    def load(self):
        return self._installation.load(list[StoragePermissionMapping], filename=self.FILENAME)

    def _get_storage_accounts(self) -> list[StorageAccount]:
        external_locations = self._locations.external_locations_with_root()
        used_storage_accounts = []
        for location in external_locations:
            if location.location.startswith("abfss://"):
                start = location.location.index("@")
                end = location.location.index(".dfs.core.windows.net")
                storage_acct = location.location[start + 1 : end]
                if storage_acct not in used_storage_accounts:
                    used_storage_accounts.append(storage_acct)
        if not used_storage_accounts:  # Avoid unnecessary crawling storage accounts below
            return []
        storage_accounts = []
        for storage_account in self._azurerm.storage_accounts():
            if storage_account.name in used_storage_accounts:
                storage_accounts.append(storage_account)
        return storage_accounts

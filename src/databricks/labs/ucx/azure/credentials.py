import logging
from dataclasses import dataclass, field

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.tui import Prompts
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import BadRequest
from databricks.sdk.errors.platform import InvalidParameterValue, PermissionDenied, NotFound
from databricks.sdk.service.catalog import (
    AzureManagedIdentityRequest,
    AzureServicePrincipal,
    Privilege,
    StorageCredentialInfo,
    ValidationResultResult,
)

from databricks.labs.ucx.assessment.azure import AzureServicePrincipalCrawler
from databricks.labs.ucx.assessment.secrets import SecretsMixin
from databricks.labs.ucx.azure.access import (
    AzureResourcePermissions,
    StoragePermissionMapping,
)

logger = logging.getLogger(__name__)


# A dataclass to host service_principal info and its client_secret info
@dataclass
class ServicePrincipalMigrationInfo:
    permission_mapping: StoragePermissionMapping
    client_secret: str


@dataclass
class StorageCredentialValidationResult:
    name: str
    application_id: str
    read_only: bool | None
    validated_on: str
    directory_id: str | None = None  # str when storage credential created for AzureServicePrincipal
    failures: list[str] = field(default_factory=list)

    @classmethod
    def _get_application_and_directory_id(
        cls, storage_credential_info: StorageCredentialInfo
    ) -> tuple[str, str | None]:
        if storage_credential_info.azure_service_principal is not None:
            application_id = storage_credential_info.azure_service_principal.application_id
            directory_id = storage_credential_info.azure_service_principal.directory_id
            return application_id, directory_id

        if storage_credential_info.azure_managed_identity is not None:
            if storage_credential_info.azure_managed_identity.managed_identity_id is not None:
                return storage_credential_info.azure_managed_identity.managed_identity_id, None
            return storage_credential_info.azure_managed_identity.access_connector_id, None

        raise KeyError("Storage credential info is missing an application id.")

    @classmethod
    def from_storage_credential_info(
        cls,
        storage_credential_info: StorageCredentialInfo,
        validated_on: str,
        failures: list[str],
    ):
        assert storage_credential_info.name is not None
        application_id, directory_id = cls._get_application_and_directory_id(storage_credential_info)
        return cls(
            storage_credential_info.name,
            application_id,
            storage_credential_info.read_only,
            validated_on,
            directory_id,
            failures,
        )


class StorageCredentialManager:
    def __init__(self, ws: WorkspaceClient):
        self._ws = ws

    def list(self, include_names: set[str] | None = None) -> set[str]:
        # list existed storage credentials that is using service principal, capture the service principal's application_id
        application_ids = set()

        storage_credentials = self._ws.storage_credentials.list(max_results=0)

        if include_names:
            # we only check UC storage credentials listed in include_names
            for storage_credential in storage_credentials:
                if not storage_credential.azure_service_principal:
                    continue
                if storage_credential.name in include_names:
                    application_ids.add(storage_credential.azure_service_principal.application_id)
            logger.info(
                f"Found {len(application_ids)} distinct service principals already used in UC storage credentials listed in include_names"
            )
            return application_ids

        for storage_credential in storage_credentials:
            # only add service principal's application_id, ignore managed identity based storage_credential
            if storage_credential.azure_service_principal:
                application_ids.add(storage_credential.azure_service_principal.application_id)

        logger.info(f"Found {len(application_ids)} distinct service principals already used in UC storage credentials")
        return application_ids

    def create_with_client_secret(self, spn: ServicePrincipalMigrationInfo) -> StorageCredentialInfo:
        # this function should only be used to migrate service principal, fail the command here if
        # it's misused to create storage credential with managed identity
        assert spn.permission_mapping.directory_id is not None

        # prepare the storage credential properties
        name = spn.permission_mapping.principal
        service_principal = AzureServicePrincipal(
            spn.permission_mapping.directory_id,
            spn.permission_mapping.client_id,
            spn.client_secret,
        )
        comment = (
            f"Created by UCX during migration to UC using Azure Service Principal: {spn.permission_mapping.principal}"
        )

        # create the storage credential
        return self._ws.storage_credentials.create(
            name,
            azure_service_principal=service_principal,
            comment=comment,
            read_only=spn.permission_mapping.privilege == Privilege.READ_FILES.value,
        )

    def validate(self, storage_credential_info: StorageCredentialInfo, url: str) -> StorageCredentialValidationResult:
        try:
            validation = self._ws.storage_credentials.validate(
                storage_credential_name=storage_credential_info.name,
                url=url,
                read_only=storage_credential_info.read_only,
            )
        except InvalidParameterValue:
            logger.warning(
                "There is an existing external location overlaps with the prefix that is mapped to "
                "the service principal and used for validating the migrated storage credential. "
                "Skip the validation"
            )
            return StorageCredentialValidationResult.from_storage_credential_info(
                storage_credential_info,
                url,
                [
                    "The validation is skipped because an existing external location overlaps "
                    "with the location used for validation."
                ],
            )

        if not validation.results:
            return StorageCredentialValidationResult.from_storage_credential_info(
                storage_credential_info, url, ["Validation returned no results."]
            )

        failures = []
        for result in validation.results:
            if result.operation is None:
                continue
            if result.result == ValidationResultResult.FAIL:
                failures.append(f"{result.operation.value} validation failed with message: {result.message}")

        return StorageCredentialValidationResult.from_storage_credential_info(storage_credential_info, url, failures)


class ServicePrincipalMigration(SecretsMixin):

    def __init__(
        self,
        installation: Installation,
        ws: WorkspaceClient,
        resource_permissions: AzureResourcePermissions,
        service_principal_crawler: AzureServicePrincipalCrawler,
        storage_credential_manager: StorageCredentialManager,
    ):
        self._output_file = "azure_service_principal_migration_result.csv"
        self._installation = installation
        self._ws = ws
        self._resource_permissions = resource_permissions
        self._sp_crawler = service_principal_crawler
        self._storage_credential_manager = storage_credential_manager

    def _fetch_client_secret(self, sp_list: list[StoragePermissionMapping]) -> list[ServicePrincipalMigrationInfo]:
        # check AzureServicePrincipalInfo from AzureServicePrincipalCrawler, if AzureServicePrincipalInfo
        # has secret_scope and secret_key not empty, fetch the client_secret and put it to the client_secret field
        #
        # The input StoragePermissionMapping may have managed identity mixed in, we will ignore them for now, as
        # they won't have any client_secret, we will process managed identity in the future.

        # fetch client_secrets of crawled service principal, if any
        sp_info_with_client_secret: dict[str, str] = {}
        sp_infos = self._sp_crawler.snapshot()

        for sp_info in sp_infos:
            if not sp_info.secret_scope:
                continue
            if not sp_info.secret_key:
                continue

            secret_value = self._get_secret_if_exists(sp_info.secret_scope, sp_info.secret_key)

            if secret_value:
                sp_info_with_client_secret[sp_info.application_id] = secret_value
            else:
                logger.info(
                    f"Cannot fetch the service principal client_secret for {sp_info.application_id}. "
                    f"This service principal will be skipped for migration"
                )

        # update the list of ServicePrincipalMigrationInfo if client_secret is found
        sp_list_with_secret = []
        for spn in sp_list:
            if spn.client_id in sp_info_with_client_secret:
                sp_list_with_secret.append(
                    ServicePrincipalMigrationInfo(spn, sp_info_with_client_secret[spn.client_id])
                )
        return sp_list_with_secret

    def _print_action_plan(self, sp_list: list[StoragePermissionMapping]):
        # print action plan to console for customer to review.
        for spn in sp_list:
            logger.info(
                f"Service Principal name: {spn.principal}, "
                f"application_id: {spn.client_id}, "
                f"privilege {spn.privilege} "
                f"on location {spn.prefix}"
            )

    def _generate_migration_list(self, include_names: set[str] | None = None) -> list[ServicePrincipalMigrationInfo]:
        """
        Create the list of SP that need to be migrated, output an action plan as a csv file for users to confirm
        """
        # load sp list from azure_storage_account_info.csv
        permission_mappings = self._resource_permissions.load()
        # For now we only migrate Service Principal and ignore Managed Identity
        sp_list = [mapping for mapping in permission_mappings if mapping.type == "Application"]
        # list existed storage credentials
        sc_set = self._storage_credential_manager.list(include_names)
        # check if the sp is already used in UC storage credential
        filtered_sp_list = [sp for sp in sp_list if sp.client_id not in sc_set]
        # fetch sp client_secret if any
        sp_list_with_secret = self._fetch_client_secret(filtered_sp_list)

        # output the action plan for customer to confirm
        # but first make a copy of the list and strip out the client_secret
        sp_candidates = [sp.permission_mapping for sp in sp_list_with_secret]
        self._print_action_plan(sp_candidates)

        return sp_list_with_secret

    def save(self, migration_results: list[StorageCredentialValidationResult]) -> str:
        return self._installation.save(migration_results, filename=self._output_file)

    def _migrate_service_principals(
        self, sp_migration_infos: list[ServicePrincipalMigrationInfo]
    ) -> list[StorageCredentialValidationResult]:
        execution_result = []
        for spn in sp_migration_infos:
            if spn.permission_mapping.default_network_action != "Allow":
                logger.warning(
                    f"Service principal '{spn.permission_mapping.principal}' accesses storage account "
                    f"'{spn.permission_mapping.prefix}' with non-Allow network configuration"
                )
            try:

                storage_credential_info = self._storage_credential_manager.create_with_client_secret(spn)
                validation_results = self._storage_credential_manager.validate(
                    storage_credential_info,
                    spn.permission_mapping.prefix,
                )
                execution_result.append(validation_results)
            except (PermissionDenied, NotFound, BadRequest) as e:
                logger.error(
                    "Error creating storage credentials. Please check error message and try again."
                    "Rolling back storage credential that are created."
                )
                self._resource_permissions.delete_storage_credential(execution_result)
                raise e
        return execution_result

    def _create_storage_credentials_for_storage_accounts(
        self,
    ) -> list[StorageCredentialValidationResult]:
        access_connectors = self._resource_permissions.create_access_connectors_for_storage_accounts()

        execution_results: list[StorageCredentialValidationResult] = []
        for access_connector, url in access_connectors:
            storage_credential_info = self._ws.storage_credentials.create(
                access_connector.name,
                azure_managed_identity=AzureManagedIdentityRequest(str(access_connector.id)),
                comment="Created by UCX",
                read_only=False,  # Access connectors get "STORAGE_BLOB_DATA_CONTRIBUTOR" permissions
            )
            try:
                validation_results = self._storage_credential_manager.validate(storage_credential_info, url)
            except BadRequest:
                logger.warning(f"Could not validate storage credential {storage_credential_info.name} for url {url}")
            except (PermissionDenied, NotFound):
                logger.error(
                    "Error creating access connectors. Please review the error message and resolve the issue before "
                    "trying again. "
                    "Removing successfully created storage credentials."
                )
                self._resource_permissions.delete_storage_credential(*execution_results)
                delete_access_connectors = [access_connector for access_connector, _ in access_connectors]
                self._resource_permissions.delete_access_connectors(*delete_access_connectors)
            else:
                execution_results.append(validation_results)

        return execution_results

    def run(self, prompts: Prompts, include_names: set[str] | None = None) -> list[StorageCredentialValidationResult]:
        plan_confirmed = prompts.confirm(
            "[RECOMMENDED] Please confirm to create an access connector with a managed identity for each storage "
            "account."
        )
        ac_results: list[StorageCredentialValidationResult] = []
        if plan_confirmed:
            ac_results = self._create_storage_credentials_for_storage_accounts()

        sp_migration_infos = self._generate_migration_list(include_names)
        if any(spn.permission_mapping.default_network_action != "Allow" for spn in sp_migration_infos):
            logger.warning(
                "At least one Azure Service Principal accesses a storage account with non-Allow default network "
                "configuration, which might cause connectivity issues. We recommend using Databricks Access "
                "Connectors instead"
            )
        plan_confirmed = prompts.confirm(
            "Above Azure Service Principals will be migrated to UC storage credentials, please review and confirm."
        )
        sp_results = []
        if plan_confirmed:
            sp_results = self._migrate_service_principals(sp_migration_infos)

        execution_results = ac_results + sp_results
        if execution_results:
            results_file = self.save(execution_results)
            logger.info(
                "Completed migration from Azure Service Principal to UC Storage credentials "
                "and creation of UC Storage credentials for storage access with Databricks Access Connectors. "
                f"Please check {results_file} for validation results"
            )
        else:
            logger.info(
                "No UC Storage credentials created during Azure Service Principal migration "
                "nor for storage access with Databricks Access Connectors."
            )

        return execution_results

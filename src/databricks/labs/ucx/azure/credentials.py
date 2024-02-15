import base64
import logging
from dataclasses import dataclass

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.tui import Prompts
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import InternalError, ResourceDoesNotExist
from databricks.sdk.errors.platform import InvalidParameterValue
from databricks.sdk.service.catalog import (
    AzureServicePrincipal,
    Privilege,
    StorageCredentialInfo,
    ValidateStorageCredentialResponse,
    ValidationResult,
)

from databricks.labs.ucx.assessment.azure import AzureServicePrincipalCrawler
from databricks.labs.ucx.azure.access import (
    AzureResourcePermissions,
    StoragePermissionMapping,
)
from databricks.labs.ucx.azure.resources import AzureResources
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.framework.crawlers import StatementExecutionBackend
from databricks.labs.ucx.hive_metastore.locations import ExternalLocations

logger = logging.getLogger(__name__)


# A dataclass to host service_principal info and its client_secret info
@dataclass
class ServicePrincipalMigrationInfo:
    permission_mapping: StoragePermissionMapping
    client_secret: str


@dataclass
class StorageCredentialValidationResult:
    name: str | None = None
    azure_service_principal: AzureServicePrincipal | None = None
    created_by: str | None = None
    read_only: bool | None = None
    results: list[ValidationResult] | None = None

    @classmethod
    def from_storage_credential_validation(
        cls, storage_credential: StorageCredentialInfo, validation: ValidateStorageCredentialResponse
    ):
        if storage_credential.azure_service_principal:
            # Guard rail to explicitly remove the client_secret, just in case the azure_service_principal
            # in StorageCredentialInfo returned by WorkspaceClient.storage_credentials.create exposes the
            # client_secret due to potential bugs in the future.
            service_principal = AzureServicePrincipal(
                storage_credential.azure_service_principal.directory_id,
                storage_credential.azure_service_principal.application_id,
                "",
            )

        return cls(
            name=storage_credential.name,
            azure_service_principal=service_principal,
            created_by=storage_credential.created_by,
            read_only=storage_credential.read_only,
            results=validation.results,
        )


class StorageCredentialManager:
    def __init__(self, ws: WorkspaceClient):
        self._ws = ws

    def list_storage_credentials(self) -> set[str]:
        # list existed storage credentials that is using service principal, capture the service principal's application_id
        application_ids = set()

        storage_credentials = self._ws.storage_credentials.list(max_results=0)

        for storage_credential in storage_credentials:
            # only add service principal's application_id, ignore managed identity based storage_credential
            if storage_credential.azure_service_principal:
                application_ids.add(storage_credential.azure_service_principal.application_id)

        logger.info(f"Found {len(application_ids)} distinct service principals already used in UC storage credentials")
        return application_ids

    def create_storage_credential(self, sp: ServicePrincipalMigrationInfo) -> StorageCredentialInfo:
        # prepare the storage credential properties
        name = sp.permission_mapping.principal
        service_principal = AzureServicePrincipal(
            directory_id=sp.permission_mapping.directory_id,
            application_id=sp.permission_mapping.client_id,
            client_secret=sp.client_secret,
        )
        comment = f"Created by UCX during migration to UC using Azure Service Principal: {sp.permission_mapping.principal}"
        read_only = False
        if sp.permission_mapping.privilege == Privilege.READ_FILES.value:
            read_only = True
        # create the storage credential
        return self._ws.storage_credentials.create(
            name, azure_service_principal=service_principal, comment=comment, read_only=read_only
        )

    def validate_storage_credential(
        self, storage_credential, sp_migration: ServicePrincipalMigrationInfo
    ) -> StorageCredentialValidationResult:
        read_only = False
        if sp_migration.permission_mapping.privilege == Privilege.READ_FILES.value:
            read_only = True
        # storage_credential validation creates a temp UC external location, which cannot overlap with
        # existing UC external locations. So add a sub folder to the validation location just in case
        try:
            validation = self._ws.storage_credentials.validate(
                storage_credential_name=storage_credential.name,
                url=sp_migration.permission_mapping.prefix,
                read_only=read_only,
            )
            return StorageCredentialValidationResult.from_storage_credential_validation(storage_credential, validation)
        except InvalidParameterValue:
            logger.warning(
                "There is an existing external location overlaps with the prefix that is mapped to the service principal and used for validating the migrated storage credential. Skip the validation"
            )
            return StorageCredentialValidationResult.from_storage_credential_validation(
                storage_credential,
                ValidateStorageCredentialResponse(
                    is_dir=None,
                    results=[
                        ValidationResult(
                            message="The validation is skipped because an existing external location overlaps with the location used for validation."
                        )
                    ],
                ),
            )


class ServicePrincipalMigration:

    def __init__(
        self,
        installation: Installation,
        ws: WorkspaceClient,
        resource_permissions: AzureResourcePermissions,
        sp_crawler: AzureServicePrincipalCrawler,
        storage_credential_manager: StorageCredentialManager,
    ):
        self._output_file = "azure_service_principal_migration_result.csv"
        self._installation = installation
        self._ws = ws
        self._resource_permissions = resource_permissions
        self._sp_crawler = sp_crawler
        self._storage_credential_manager = storage_credential_manager

    @classmethod
    def for_cli(cls, ws: WorkspaceClient, prompts: Prompts, product='ucx'):
        if not ws.config.is_azure:
            logger.error("Workspace is not on azure, please run this command on azure databricks workspaces.")
            raise SystemExit()

        msg = (
            "Have you reviewed the azure_storage_account_info.csv "
            "and confirm listed service principals are allowed to be checked for migration?"
        )
        if not prompts.confirm(msg):
            raise SystemExit()

        installation = Installation.current(ws, product)
        config = installation.load(WorkspaceConfig)
        sql_backend = StatementExecutionBackend(ws, config.warehouse_id)
        azurerm = AzureResources(ws)
        locations = ExternalLocations(ws, sql_backend, config.inventory_database)

        resource_permissions = AzureResourcePermissions(installation, ws, azurerm, locations)
        sp_crawler = AzureServicePrincipalCrawler(ws, sql_backend, config.inventory_database)

        storage_credential_manager = StorageCredentialManager(ws)

        return cls(installation, ws, resource_permissions, sp_crawler, storage_credential_manager)

    def _read_databricks_secret(self, scope: str, key: str, application_id: str) -> str | None:
        try:
            secret_response = self._ws.secrets.get_secret(scope, key)
        except ResourceDoesNotExist:
            logger.info(
                f"Secret {scope}.{key} does not exists. "
                f"Cannot fetch the service principal client_secret for {application_id}. "
                f"Will not reuse this client_secret"
            )
            return None
        except InternalError:
            logger.info(
                f"InternalError while reading secret {scope}.{key}. "
                f"Cannot fetch the service principal client_secret for {application_id}. "
                f"Will not reuse this client_secret. "
                f"You may rerun the migration command later to retry this service principal"
            )
            return None

        # decode the bytes string from GetSecretResponse to utf-8 string
        # TODO: handle different encoding if we have feedback from the customer
        try:
            if secret_response.value is None:
                return None
            return base64.b64decode(secret_response.value).decode("utf-8")
        except UnicodeDecodeError:
            logger.info(
                f"Secret {scope}.{key} has Base64 bytes that cannot be decoded to utf-8 string . "
                f"Cannot fetch the service principal client_secret for {application_id}. "
                f"Will not reuse this client_secret"
            )
            return None

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
            secret_value = self._read_databricks_secret(
                sp_info.secret_scope, sp_info.secret_key, sp_info.application_id
            )
            if secret_value:
                sp_info_with_client_secret[sp_info.application_id] = secret_value

        # update the list of ServicePrincipalMigrationInfo if client_secret is found
        sp_list_with_secret = []
        for sp in sp_list:
            if sp.client_id in sp_info_with_client_secret:
                sp_list_with_secret.append(ServicePrincipalMigrationInfo(sp, sp_info_with_client_secret[sp.client_id]))
        return sp_list_with_secret

    def _print_action_plan(self, sp_list: list[ServicePrincipalMigrationInfo]):
        # print action plan to console for customer to review.
        for sp in sp_list:
            logger.info(
                f"Service Principal name: {sp.permission_mapping.principal}, "
                f"application_id: {sp.permission_mapping.client_id}, "
                f"privilege {sp.permission_mapping.privilege} "
                f"on location {sp.permission_mapping.prefix}"
            )

    def _generate_migration_list(self) -> list[ServicePrincipalMigrationInfo]:
        """
        Create the list of SP that need to be migrated, output an action plan as a csv file for users to confirm
        """
        # load sp list from azure_storage_account_info.csv
        sp_list = self._resource_permissions.load()
        # list existed storage credentials
        sc_set = self._storage_credential_manager.list_storage_credentials()
        # check if the sp is already used in UC storage credential
        filtered_sp_list = [sp for sp in sp_list if sp.client_id not in sc_set]
        # fetch sp client_secret if any
        sp_list_with_secret = self._fetch_client_secret(filtered_sp_list)
        # output the action plan for customer to confirm
        self._print_action_plan(sp_list_with_secret)
        return sp_list_with_secret

    def save(self, migration_results: list[StorageCredentialValidationResult]) -> str:
        return self._installation.save(migration_results, filename=self._output_file)

    def run(self, prompts: Prompts) -> list[StorageCredentialValidationResult]:

        sp_list_with_secret = self._generate_migration_list()

        plan_confirmed = prompts.confirm(
            "Above Azure Service Principals will be migrated to UC storage credentials, please review and confirm."
        )
        if plan_confirmed is not True:
            return []

        execution_result = []
        for sp in sp_list_with_secret:
            storage_credential = self._storage_credential_manager.create_storage_credential(sp)
            execution_result.append(
                self._storage_credential_manager.validate_storage_credential(storage_credential, sp)
            )

        results_file = self.save(execution_result)
        logger.info("Completed migration from Azure Service Principal migrated to UC Storage credentials")
        print(
            f"Completed migration from Azure Service Principal migrated to UC Storage credentials. "
            f"Please check {results_file} for validation results"
        )
        return execution_result

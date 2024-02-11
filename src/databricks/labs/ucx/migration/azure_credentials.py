import base64
import logging
from collections import namedtuple
from dataclasses import dataclass

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.tui import Prompts
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import (
    InternalError,
    PermissionDenied,
    ResourceDoesNotExist,
    Unauthenticated,
)
from databricks.sdk.service.catalog import (
    AzureServicePrincipal,
    Privilege,
    StorageCredentialInfo,
    ValidateStorageCredentialResponse,
    ValidationResult,
    ValidationResultOperation,
)

from databricks.labs.ucx.assessment.azure import (
    AzureResourcePermissions,
    AzureResources,
    AzureServicePrincipalCrawler,
    StoragePermissionMapping,
)
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.framework.crawlers import StatementExecutionBackend
from databricks.labs.ucx.hive_metastore.locations import ExternalLocations

logger = logging.getLogger(__name__)


# A namedtuple to host service_principal and its client_secret info
ServicePrincipalMigrationInfo = namedtuple("ServicePrincipalMigrationInfo", "service_principal client_secret")


@dataclass
class StorageCredentialValidationResult:
    name: str
    azure_service_principal: AzureServicePrincipal
    created_by: str
    read_only: bool
    message: str
    operation: ValidationResultOperation
    results: list[ValidationResult]

    @classmethod
    def from_storage_credential_validation(
        cls, storage_credential: StorageCredentialInfo, validation: ValidateStorageCredentialResponse
    ):
        return cls(
            name=storage_credential.name,
            azure_service_principal=storage_credential.azure_service_principal,
            created_by=storage_credential.created_by,
            read_only=storage_credential.read_only,
            results=validation.results,
        )


class AzureServicePrincipalMigration:

    def __init__(
        self,
        installation: Installation,
        ws: WorkspaceClient,
        azure_resource_permissions: AzureResourcePermissions,
        azure_sp_crawler: AzureServicePrincipalCrawler,
    ):
        self._output_file = "azure_service_principal_migration_result.csv"
        self._final_sp_list: list[ServicePrincipalMigrationInfo] = []
        self._installation = installation
        self._ws = ws
        self._azure_resource_permissions = azure_resource_permissions
        self._azure_sp_crawler = azure_sp_crawler
        self._action_plan = 'service_principals_for_storage_credentials.csv'

    @classmethod
    def for_cli(cls, ws: WorkspaceClient, product='ucx'):
        installation = Installation.current(ws, product)
        config = installation.load(WorkspaceConfig)
        sql_backend = StatementExecutionBackend(ws, config.warehouse_id)
        azurerm = AzureResources(ws)
        locations = ExternalLocations(ws, sql_backend, config.inventory_database)

        azure_resource_permissions = AzureResourcePermissions(installation, ws, azurerm, locations)
        azure_sp_crawler = AzureServicePrincipalCrawler(ws, sql_backend, config.inventory_database)

        return cls(installation, ws, azure_resource_permissions, azure_sp_crawler)

    def _list_storage_credentials(self) -> set[str]:
        # list existed storage credentials that is using service principal, capture the service principal's application_id
        storage_credential_app_ids = set()

        storage_credentials = self._ws.storage_credentials.list(max_results=0)
        for storage_credential in storage_credentials:
            # only add service principal's application_id, ignore managed identity based storage_credential
            if storage_credential.azure_service_principal:
                storage_credential_app_ids.add(storage_credential.azure_service_principal.application_id)
        logger.info(
            f"Found {len(storage_credential_app_ids)} distinct service principals already used in UC storage credentials"
        )
        return storage_credential_app_ids


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
                f"Will not reuse this client_secret"
            )
            print(
                f"{application_id} is not migrated due to InternalError while fetching the client_secret from Databricks secret."
                f"You may rerun the migration command later to retry this service principal"
            )
            return None

        # decode the bytes string from GetSecretResponse to utf-8 string
        # TODO: handle different encoding if we have feedback from the customer
        try:
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
        azure_sp_info_with_client_secret: dict[str, str] = {}
        azure_sp_infos = self._azure_sp_crawler.snapshot()

        for azure_sp_info in azure_sp_infos:
            if azure_sp_info.secret_scope is None:
                continue
            if azure_sp_info.secret_key is None:
                continue
            secret_value = self._read_databricks_secret(
                azure_sp_info.secret_scope, azure_sp_info.secret_key, azure_sp_info.application_id
            )
            if secret_value:
                azure_sp_info_with_client_secret[azure_sp_info.application_id] = secret_value

        # update the list of ServicePrincipalMigrationInfo if client_secret is found
        sp_list_with_secret = []
        for sp in sp_list:
            if sp.client_id in azure_sp_info_with_client_secret:
                sp_list_with_secret.append(
                    ServicePrincipalMigrationInfo(service_principal=sp, client_secret=azure_sp_info_with_client_secret[sp.client_id])
                )
        return sp_list_with_secret

    def _print_action_plan(self, sp_list_with_secret: list[ServicePrincipalMigrationInfo]):
        # print action plan to console for customer to review.
        for sp in sp_list_with_secret:
            print(
                f"Service Principal name: {sp.service_principal.principal}, "
                f"application_id: {sp.service_principal.client_id}, "
                f"privilege {sp.service_principal.privilege} "
                f"on location {sp.service_principal.prefix}"
            )

    def _generate_migration_list(self):
        """
        Create the list of SP that need to be migrated, output an action plan as a csv file for users to confirm
        :return:
        """
        # load sp list from azure_storage_account_info.csv
        sp_list = self._azure_resource_permissions.load()
        # list existed storage credentials
        sc_set = self._list_storage_credentials()
        # check if the sp is already used in UC storage credential
        filtered_sp_list = [sp for sp in sp_list if sp not in sc_set]
        # fetch sp client_secret if any
        sp_list_with_secret = self._fetch_client_secret(filtered_sp_list)
        self._final_sp_list = sp_list_with_secret
        # output the action plan for customer to confirm
        self._print_action_plan(sp_list_with_secret)
        return

    def _create_storage_credential(self, sp_migration: ServicePrincipalMigrationInfo):
        # prepare the storage credential properties
        name = sp_migration.service_principal.principal
        azure_service_principal = AzureServicePrincipal(
            directory_id=sp_migration.service_principal.directory_id,
            application_id=sp_migration.service_principal.client_id,
            client_secret=sp_migration.client_secret
        )
        comment = f"Created by UCX during migration to UC using Azure Service Principal: {sp_migration.service_principal.principal}"
        read_only = False
        if sp_migration.service_principal.privilege == Privilege.READ_FILES:
            read_only = True
        # create the storage credential
        storage_credential = self._ws.storage_credentials.create(
            name=name, azure_service_principal=azure_service_principal, comment=comment, read_only=read_only
        )

        validation_result = self._validate_storage_credential(storage_credential, sp_migration.service_principal.prefix)
        return validation_result

    def _validate_storage_credential(self, storage_credential, location) -> StorageCredentialValidationResult:
        validation = self._ws.storage_credentials.validate(
            storage_credential_name=storage_credential.name, url=location
        )
        return StorageCredentialValidationResult.from_storage_credential_validation(storage_credential, validation)

    def execute_migration(self, prompts: Prompts):
        if not self._ws.config.is_azure:
            logger.error("Workspace is not on azure, please run this command on azure databricks workspaces.")
            return

        csv_confirmed = prompts.confirm(
            "Have you reviewed the azure_storage_account_info.csv "
            "and confirm listed service principals are allowed to be checked for migration?"
        )
        if csv_confirmed is not True:
            return

        self._generate_migration_list()

        plan_confirmed = prompts.confirm(
            "Above Azure Service Principals will be migrated to UC storage credentials, please review and confirm."
        )
        if plan_confirmed is not True:
            return

        execution_result = []
        for sp in self._final_sp_list:
            execution_result.append(self._create_storage_credential(sp))

        results_file = self._installation.save(execution_result, filename=self._output_file)
        logger.info("Completed migration from Azure Service Principal migrated to UC Storage credentials")
        print(
            f"Completed migration from Azure Service Principal migrated to UC Storage credentials. "
            f"Please check {results_file} for validation results"
        )
        return

import logging
from dataclasses import dataclass

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.tui import Prompts
from databricks.labs.lsql.backends import StatementExecutionBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors.platform import InvalidParameterValue
from databricks.sdk.service.catalog import (
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
from databricks.labs.ucx.azure.resources import AzureAPIClient, AzureResources
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.hive_metastore.locations import ExternalLocations

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
    read_only: bool
    validated_on: str
    directory_id: str | None = None
    failures: list[str] | None = None

    @classmethod
    def from_validation(cls, permission_mapping: StoragePermissionMapping, failures: list[str] | None):
        return cls(
            permission_mapping.principal,
            permission_mapping.client_id,
            permission_mapping.privilege == Privilege.READ_FILES.value,
            permission_mapping.prefix,
            permission_mapping.directory_id,
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

    def validate(self, permission_mapping: StoragePermissionMapping) -> StorageCredentialValidationResult:
        try:
            validation = self._ws.storage_credentials.validate(
                storage_credential_name=permission_mapping.principal,
                url=permission_mapping.prefix,
                read_only=permission_mapping.privilege == Privilege.READ_FILES.value,
            )
        except InvalidParameterValue:
            logger.warning(
                "There is an existing external location overlaps with the prefix that is mapped to "
                "the service principal and used for validating the migrated storage credential. "
                "Skip the validation"
            )
            return StorageCredentialValidationResult.from_validation(
                permission_mapping,
                [
                    "The validation is skipped because an existing external location overlaps "
                    "with the location used for validation."
                ],
            )

        if not validation.results:
            return StorageCredentialValidationResult.from_validation(
                permission_mapping, ["Validation returned no results."]
            )

        failures = []
        for result in validation.results:
            if result.operation is None:
                continue
            if result.result == ValidationResultResult.FAIL:
                failures.append(f"{result.operation.value} validation failed with message: {result.message}")
        return StorageCredentialValidationResult.from_validation(permission_mapping, None if not failures else failures)


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

    @classmethod
    def for_cli(cls, ws: WorkspaceClient, installation: Installation, prompts: Prompts):
        msg = (
            "Have you reviewed the azure_storage_account_info.csv "
            "and confirm listed service principals are allowed to be checked for migration?"
        )
        if not prompts.confirm(msg):
            raise SystemExit()

        config = installation.load(WorkspaceConfig)
        sql_backend = StatementExecutionBackend(ws, config.warehouse_id)
        azure_mgmt_client = AzureAPIClient(
            ws.config.arm_environment.resource_manager_endpoint,
            ws.config.arm_environment.service_management_endpoint,
        )
        graph_client = AzureAPIClient("https://graph.microsoft.com", "https://graph.microsoft.com")
        azurerm = AzureResources(azure_mgmt_client, graph_client)
        locations = ExternalLocations(ws, sql_backend, config.inventory_database)

        resource_permissions = AzureResourcePermissions(installation, ws, azurerm, locations)
        sp_crawler = AzureServicePrincipalCrawler(ws, sql_backend, config.inventory_database)

        storage_credential_manager = StorageCredentialManager(ws)

        return cls(installation, ws, resource_permissions, sp_crawler, storage_credential_manager)

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

    def run(self, prompts: Prompts, include_names: set[str] | None = None) -> list[StorageCredentialValidationResult]:

        sp_list_with_secret = self._generate_migration_list(include_names)

        plan_confirmed = prompts.confirm(
            "Above Azure Service Principals will be migrated to UC storage credentials, please review and confirm."
        )
        if plan_confirmed is not True:
            return []

        execution_result = []
        for spn in sp_list_with_secret:
            self._storage_credential_manager.create_with_client_secret(spn)
            execution_result.append(self._storage_credential_manager.validate(spn.permission_mapping))

        if execution_result:
            results_file = self.save(execution_result)
            logger.info(
                f"Completed migration from Azure Service Principal to UC Storage credentials"
                f"Please check {results_file} for validation results"
            )
        else:
            logger.info("No Azure Service Principal migrated to UC Storage credentials")
        return execution_result

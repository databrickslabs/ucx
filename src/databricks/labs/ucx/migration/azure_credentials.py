import base64
import logging
from dataclasses import dataclass

from databricks.labs.blueprint.installation import Installation
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import Unauthenticated, PermissionDenied, ResourceDoesNotExist, InternalError

from databricks.labs.ucx.assessment.azure import AzureResourcePermissions, AzureResources, \
    StoragePermissionMapping, AzureServicePrincipalCrawler
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.framework.crawlers import StatementExecutionBackend
from databricks.labs.ucx.hive_metastore.locations import ExternalLocations

logger = logging.getLogger(__name__)


@dataclass
class ServicePrincipalMigrationInfo(StoragePermissionMapping):
    # Service Principal's client_secret stored in Databricks secret
    client_secret: str

    @classmethod
    def from_storage_permission_mapping(cls, storage_permission_mapping: StoragePermissionMapping, client_secret: str):
        return cls(prefix=storage_permission_mapping.prefix,
                   client_id=storage_permission_mapping.client_id,
                   principal=storage_permission_mapping.principal,
                   privilege=storage_permission_mapping.privilege,
                   client_secret=client_secret)


class AzureServicePrincipalMigration:

    def __init__(self, installation: Installation, ws: WorkspaceClient, azure_resource_permissions: AzureResourcePermissions,
                 azure_sp_crawler: AzureServicePrincipalCrawler):
        self._final_sp_list = None
        self._installation = installation
        self._ws = ws
        self._azure_resource_permissions = azure_resource_permissions
        self._azure_sp_crawler = azure_sp_crawler
        self._action_plan = 'service_principals_for_storage_credentials.csv'


    @classmethod
    def for_cli(cls, ws: WorkspaceClient, customized_csv: str, replace_with_ac: bool, product='ucx'):
        installation = Installation.current(ws, product)
        config = installation.load(WorkspaceConfig)
        sql_backend = StatementExecutionBackend(ws, config.warehouse_id)
        azurerm = AzureResources(ws)
        locations = ExternalLocations(ws, sql_backend, config.inventory_database)

        azure_resource_permissions = AzureResourcePermissions(installation, ws, azurerm, locations)
        azure_sp_crawler = AzureServicePrincipalCrawler(ws, sql_backend, config.inventory_database)

        return cls(installation, ws, azure_resource_permissions, azure_sp_crawler, customized_csv, replace_with_ac)


    def _list_storage_credentials(self):
        # list existed storage credentials that is using service principal, capture the service principal's application_id
        return {}


    def _check_sp_in_storage_credentials(self, sp_list, sc_set):
        # if sp is already used, take it off from the sp_list
        return list()


    def _fetch_client_secret(self, sp_list: list[StoragePermissionMapping]) -> list[ServicePrincipalMigrationInfo]:
        # check AzureServicePrincipalInfo from AzureServicePrincipalCrawler, if AzureServicePrincipalInfo
        # has secret_scope and secret_key not empty, fetch the client_secret and put it to the client_secret field
        #
        # The input StoragePermissionMapping may have managed identity mixed in, we will ignore them for now, as
        # they won't have any client_secret, we will process managed identity in the future.

        # fetch client_secrets of crawled service principal, if any
        azure_sp_info_with_client_secret = {}
        azure_sp_infos = self._azure_sp_crawler.snapshot()

        for azure_sp_info in azure_sp_infos:
            if azure_sp_info.secret_scope is None:
                continue
            if azure_sp_info.secret_key is None:
                continue

            try:
                secret_response = self._ws.secrets.get_secret(azure_sp_info.secret_scope, azure_sp_info.secret_key)
            except Unauthenticated:
                logger.info(f"User is unauthenticated to fetch secret value. Cannot fetch the service principal "
                            f"client_secret for {azure_sp_info.application_id}. Will not reuse this client_secret")
                continue
            except PermissionDenied:
                logger.info(f"User does not have permission to read secret value for {azure_sp_info.secret_scope}.{azure_sp_info.secret_key}. "
                            f"Cannot fetch the service principal client_secret for {azure_sp_info.application_id}. "
                            f"Will not reuse this client_secret")
                continue
            except ResourceDoesNotExist:
                logger.info(f"Secret {azure_sp_info.secret_scope}.{azure_sp_info.secret_key} does not exists. "
                            f"Cannot fetch the service principal client_secret for {azure_sp_info.application_id}. "
                            f"Will not reuse this client_secret")
                continue
            except InternalError:
                logger.info(f"InternalError while reading secret {azure_sp_info.secret_scope}.{azure_sp_info.secret_key}. "
                            f"Cannot fetch the service principal client_secret for {azure_sp_info.application_id}. "
                            f"Will not reuse this client_secret")
                continue

            # decode the bytes string from GetSecretResponse to utf-8 string
            # TODO: handle different encoding if we have feedback from the customer
            try:
                secret_value = base64.b64decode(secret_response.value).decode("utf-8")
                azure_sp_info_with_client_secret.update(azure_sp_info.application_id, secret_value)
            except UnicodeDecodeError:
                logger.info(f"Secret {azure_sp_info.secret_scope}.{azure_sp_info.secret_key} has Base64 bytes that cannot be decoded to utf-8 string . "
                            f"Cannot fetch the service principal client_secret for {azure_sp_info.application_id}. "
                            f"Will not reuse this client_secret")

        # update the list of ServicePrincipalMigrationInfo if client_secret is found
        for sp in sp_list:
            if sp.client_id in azure_sp_info_with_client_secret:
                yield ServicePrincipalMigrationInfo.from_storage_permission_mapping(sp, azure_sp_info_with_client_secret[sp.client_id])


    def _save_action_plan(self, sp_list_with_secret) -> str | None:
        # save action plan to a file for customer to review.
        # client_secret need to be removed
        sp_list_wo_secret = []
        for sp in sp_list_with_secret:
            sp_list_wo_secret.append(StoragePermissionMapping(sp.prefix, sp.client_id, sp.principal, sp.privilege))

        return self._installation.save(sp_list_wo_secret, filename=self._action_plan)


    def generate_migration_list(self):
        """
        Create the list of SP that need to be migrated, output an action plan as a csv file for users to confirm
        :return:
        """
        # load sp list from azure_storage_account_info.csv
        loaded_sp_list = self._azure_resource_permissions.load_spn_permission()
        # list existed storage credentials
        sc_set = self._list_storage_credentials()
        # check if the sp is already used in UC storage credential
        filtered_sp_list = self._check_sp_in_storage_credentials(loaded_sp_list, sc_set)
        # fetch sp client_secret if any
        sp_list_with_secret = self._fetch_client_secret(filtered_sp_list)
        self._final_sp_list = sp_list_with_secret
        # output the action plan for customer to confirm
        return self._save_action_plan(sp_list_with_secret)


    def _create_sc_with_client_secret(self, sp):

        storage_credential=""
        self._validate_sc(storage_credential)
        return


    def _validate_sc(self, storage_credential):
        return

    def execute_migration(self):
        """
        Execute the action plan after user confirmed
        :return:
        """
        for sp in self._final_sp_list:
            self._create_sc_with_client_secret(sp)
        return



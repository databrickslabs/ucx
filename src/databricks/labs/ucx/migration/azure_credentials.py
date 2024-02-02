import csv
import io
import logging
from dataclasses import dataclass, fields

from databricks.labs.blueprint.installation import Installation
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import Unauthenticated, PermissionDenied, NotFound, InternalError

from databricks.labs.ucx.assessment.azure import AzureResourcePermissions, AzureResources, \
    StoragePermissionMapping, AzureServicePrincipalCrawler
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.framework.crawlers import StatementExecutionBackend
from databricks.labs.ucx.hive_metastore.locations import ExternalLocations

logger = logging.getLogger(__name__)


@dataclass
class ServicePrincipalMigrationInfo(StoragePermissionMapping):
    # if create access manager and managed identity for this SP
    replace_with_access_connector: bool
    # if a storage credential using this SP already exists
    already_in_storage_credential: bool
    # SP's client_secret stored in Databricks secret
    client_secret: str
    # if this is a managed identity
    if_managed_identity: bool

    @classmethod
    def from_storage_permission_mapping(cls, storage_permission_mapping, **kwargs):
        attrs = {field.name: getattr(storage_permission_mapping, field.name) for field in fields(storage_permission_mapping)}
        attrs.update(kwargs)
        return cls(**attrs)

class AzureServicePrincipalMigration:

    def __init__(self, installation: Installation, ws: WorkspaceClient, azure_resource_permissions: AzureResourcePermissions,
                 azure_sp_crawler: AzureServicePrincipalCrawler, customized_csv: str, replace_with_access_connector: bool):
        self._final_sp_list = None
        self._installation = installation
        self._ws = ws
        self._azure_resource_permissions = azure_resource_permissions
        self._azure_sp_crawler = azure_sp_crawler

        if customized_csv is not None:
            self._csv = customized_csv
        else:
            self._csv = azure_resource_permissions.filename
        self._use_access_connector = replace_with_access_connector if replace_with_access_connector is not None else False


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


    def _load_sp_csv(self):
        """
        Load SP info from azure_storage_account_info.csv
        :return:
        """
        storage_account_infos = self._installation.load(list[StoragePermissionMapping], self._csv)
        first_field_name = fields(StoragePermissionMapping)

        for storage_account_info in storage_account_infos:
            first_field_value = getattr(storage_account_info, first_field_name)
            if first_field_value is None or first_field_value.startswith("#"):
                logger.info(f"Skip migrate Azure Service Principal: {storage_account_info} to UC storage credential")
                #TODO: record and persist this skip in a table
                continue

            use_access_connector = self._use_access_connector
            if first_field_value.startswith("-"):
                use_access_connector = True

            sp_migration_info = ServicePrincipalMigrationInfo.from_storage_permission_mapping(
                storage_account_info,
                replace_with_access_connector = use_access_connector,
                already_in_storage_credential = False,
                client_secret = "",
                if_managed_identity = False
            )
            yield sp_migration_info


    def _check_sp_type(self, sp_list):
        # check if the sp with given client_id is Service Principal or Managed Identity
        # May use https://learn.microsoft.com/en-us/graph/api/directoryobject-getbyids?view=graph-rest-1.0&tabs=http
        # Managed Identity should have "@odata.type": "#microsoft.graph.managedIdentity"
        # https://learn.microsoft.com/en-us/graph/api/resources/managedidentity?view=graph-rest-beta (warning: beta api)
        # update if_mi field of the sp in sp_list
        # TODO: find out if azure databricks access connector with system assigned managed identity
        # TODO: has role assignment on storage, what ID will be returned by providers/Microsoft.Authorization/roleAssignments
        return


    def _list_storage_credentials(self):
        # list existed storage credentials
        # for SP storage credentials, capture its application_id
        # for MI storage credentials:
        #   if managed_identity_id is not empty capture it,
        #   else capture the access_connector_id
        # TODO: UC only has access_connector_id (resource id) for access connector with system assigned managed identity
        # TODO: need to find the mapping between the access_connector_id and principalId from providers/Microsoft.Authorization/roleAssignments
        return {}


    def _check_sp_in_storage_credentials(self, sp_list, sc_set):
        # if sp is already used, take it off from the sp_list
        return list()


    def _fetch_client_secret(self, sp_list: list[ServicePrincipalMigrationInfo]):
        # check AzureServicePrincipalInfo from AzureServicePrincipalCrawler, if AzureServicePrincipalInfo
        # has secret_scope and secret_key not empty, fetch the client_secret and put it to the
        # client_secret field

        # fetch client_secrets of crawled service principal, if any
        azure_sp_info_with_client_secret = {}
        azure_sp_infos = self._azure_sp_crawler.snapshot()

        for azure_sp_info in azure_sp_infos:
            if azure_sp_info.secret_scope and azure_sp_info.secret_key:
                try:
                    secret_response = self._ws.secrets.get_secret(azure_sp_info.secret_scope, azure_sp_info.secret_key)

                    # decode the binary string from GetSecretResponse to utf-8 string
                    # TODO: handle different encoding if we have feedback from the customer
                    binary_value_str = secret_response.value
                    num_bytes = len(binary_value_str) // 8 + (1 if len(binary_value_str) % 8 else 0)
                    byte_array = int(binary_value_str, 2).to_bytes(num_bytes, 'big')
                    try:
                        secret_value = byte_array.decode('utf-8')
                        azure_sp_info_with_client_secret.update(azure_sp_info.application_id, secret_value)
                    except UnicodeDecodeError as e:
                        logger.info(f"Secret {azure_sp_info.secret_scope}.{azure_sp_info.secret_key} does not exists. "
                                    f"Cannot fetch the service principal client_secret for {azure_sp_info.application_id}. "
                                    f"Will not reuse this client_secret")
                except Unauthenticated:
                    logger.info(f"User is unauthenticated to fetch secret value. Cannot fetch the service principal "
                                 f"client_secret for {azure_sp_info.application_id}. Will not reuse this client_secret")
                except PermissionDenied:
                    logger.info(f"User does not have permission to read secret value for {azure_sp_info.secret_scope}.{azure_sp_info.secret_key}. "
                                 f"Cannot fetch the service principal client_secret for {azure_sp_info.application_id}. "
                                 f"Will not reuse this client_secret")
                except NotFound:
                    logger.info(f"Secret {azure_sp_info.secret_scope}.{azure_sp_info.secret_key} does not exists. "
                                 f"Cannot fetch the service principal client_secret for {azure_sp_info.application_id}. "
                                 f"Will not reuse this client_secret")
                except InternalError:
                    logger.info(f"InternalError while reading secret {azure_sp_info.secret_scope}.{azure_sp_info.secret_key}. "
                                f"Cannot fetch the service principal client_secret for {azure_sp_info.application_id}. "
                                f"Will not reuse this client_secret")

        # update the list of ServicePrincipalMigrationInfo if client_secret is found
        for sp in sp_list:
            if sp.if_managed_identity is False and sp.client_id in azure_sp_info_with_client_secret:
                sp.client_secret = azure_sp_info_with_client_secret[sp.client_id]

        return


    def _save_action_plan(self, sp_list):
        # save action plan to a file for customer to review, maybe csv format
        # If SP with replace_with_ac = true, create access connector with same role assignment as the SP, and create SC with the connector
        # If SP with replace_with_ac = false and client_secret not empty, reuse the client_secret to create SC
        # If SP with replace_with_ac = false and empty client_secret, create a new client_secret and use it to create SC
        return


    def generate_migration_list(self):
        """
        Create the list of SP that need to be migrated, output an action plan as a csv file for users to confirm
        :return:
        """
        # load sp list from azure_storage_account_info.csv
        loaded_sp_list = self._load_sp_csv()
        # further check if the sp is Service Principal or Managed Identity
        self._check_sp_type(loaded_sp_list)
        # list existed storage credentials
        sc_set = self._list_storage_credentials()
        # check if the sp is already used in UC storage credential
        filtered_sp_list = self._check_sp_in_storage_credentials(loaded_sp_list, sc_set)
        # fetch sp client_secret if any
        self._fetch_client_secret(filtered_sp_list)
        # output the action plan for customer to confirm
        self._save_action_plan(filtered_sp_list)

        self._final_sp_list = filtered_sp_list



    def _create_sc_with_new_ac(self, sp):
        return


    def _create_sc_with_new_client_secret(self, sp):
        return


    def _create_sc_with_client_secret(self, sp):
        return


    def execute_migration(self):
        """
        Execute the action plan after user confirmed
        :return:
        """
        for sp in self._final_sp_list:
            if sp.replace_with_ac:
                # If SP with replace_with_ac = true, create access connector with same role assignment as the SP, and create SC with the connector
                self._create_sc_with_new_ac(sp)
            elif sp.client_secret is not "":
                # If SP with replace_with_ac = false and client_secret not empty, reuse the client_secret to create SC
                self._create_sc_with_client_secret(sp)
            else:
                # If SP with replace_with_ac = false and empty client_secret, create a new client_secret and use it to create SC
                self._create_sc_with_new_client_secret(sp)

        # TODO: validate the created storage credentials



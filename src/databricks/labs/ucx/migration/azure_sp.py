import csv
import io
import logging
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ExportFormat

logger = logging.getLogger(__name__)


@dataclass
class ServicePrincipalMigrationInfo:
    prefix: str
    client_id: str
    principal: str
    privilege: str
    # if create access manager and managed identity for this SP
    replace_with_ac: bool
    # if a storage credential using this SP already exists
    already_in_sc: bool
    # SP's client_secret stored in Databricks secret
    client_secret: str
    # if this is a managed identity
    if_mi: bool


class AzureServicePrincipalMigration:

    def __init__(self, ws: WorkspaceClient, csv: str, replace_with_ac: bool):
        self._ws = ws
        self._csv = csv if csv is not None else f"/Users/{ws.current_user.me().user_name}/.ucx/azure_storage_account_info.csv"
        self._use_ac = replace_with_ac if replace_with_ac is not None else False

    def _load_sp_csv(self):
        """
        Load SP info from azure_storage_account_info.csv
        :return:
        """
        #TODO: check the download status
        csv_source = self._ws.workspace.download(self._csv, format=ExportFormat.AUTO)
        csv_textio = io.TextIOWrapper(csv_source, encoding='utf-8')

        csv_reader = csv.DictReader(csv_textio)
        first_col = csv_reader.fieldnames[0]
        for row in csv_reader:
            if row[first_col].startswith("#"):
                logger.info(f"Skip migrate Azure Service Principal: {row} to UC storage credential")
                #TODO: record and persist this skip in a table
                continue
            use_ac = False
            if self._use_ac:
                use_ac = True
            elif row[first_col].startswith("-"):
                use_ac = True
            sp_migration_info = ServicePrincipalMigrationInfo(
                prefix = row["prefix"],
                client_id = row["client_id"],
                principal = row["principal"],
                privilege = row["privilege"],
                replace_with_ac = use_ac,
                already_in_sc = False,
                client_secret = "",
                if_mi = False
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

    def _list_sc(self):
        # list existed storage credentials
        # for SP storage credentials, capture its application_id
        # for MI storage credentials:
        #   if managed_identity_id is not empty capture it,
        #   else capture the access_connector_id
        # TODO: UC only has access_connector_id (resource id) for access connector with system assigned managed identity
        # TODO: need to find the mapping between the access_connector_id and principalId from providers/Microsoft.Authorization/roleAssignments
        return {}


    def _check_sp_sc(self, sp_list, sc_set):
        # if sp is already used, take it off from the sp_list
        return list()


    def _fetch_client_secret(self, sp_list):
        # check AzureServicePrincipalInfo from AzureServicePrincipalCrawler, if AzureServicePrincipalInfo
        # has secret_scope and secret_key not empty, fetch the client_secret and put it to the
        # client_secret field
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
        sc_set = self._list_sc()
        # check if the sp is already used in UC storage credential
        filtered_sp_list = self._check_sp_sc(loaded_sp_list, sc_set)
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





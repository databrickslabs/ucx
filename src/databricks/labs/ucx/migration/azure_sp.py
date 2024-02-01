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
    replace_with_ac: bool


class AzureServicePrincipalMigration:

    def __init__(self, ws: WorkspaceClient, csv: str, replace_with_ac: bool):
        self._ws = ws
        self._csv = csv if csv is not None else f"/Users/{ws.current_user.me().user_name}/.ucx/azure_storage_account_info.csv"
        self._use_ac = replace_with_ac if replace_with_ac is not None else False

    def _load_sp_csv(self):
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
            sp_migration_info = ServicePrincipalMigrationInfo(row["prefix"], row["client_id"], row["principal"], row["privilege"], use_ac)
            yield sp_migration_info
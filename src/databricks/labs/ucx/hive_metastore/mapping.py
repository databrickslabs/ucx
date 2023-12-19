import csv
import dataclasses
import io
import logging
import re
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.workspace import ImportFormat

from databricks.labs.ucx.account import WorkspaceInfo
from databricks.labs.ucx.hive_metastore import ExternalLocations, TablesCrawler
from databricks.labs.ucx.hive_metastore.locations import ExternalLocation
from databricks.labs.ucx.hive_metastore.tables import Table

logger = logging.getLogger(__name__)


@dataclass
class Rule:
    workspace_name: str
    catalog_name: str
    src_schema: str
    dst_schema: str
    src_table: str
    dst_table: str

    @classmethod
    def initial(cls, workspace_name: str, catalog_name: str, table: Table) -> "Rule":
        return cls(
            workspace_name=workspace_name,
            catalog_name=catalog_name,
            src_schema=table.database,
            dst_schema=table.database,
            src_table=table.name,
            dst_table=table.name,
        )


class TableMapping:
    def __init__(self, ws: WorkspaceClient, folder: str | None = None):
        if not folder:
            folder = f"/Users/{ws.current_user.me().user_name}/.ucx"
        self._ws = ws
        self._folder = folder
        self._field_names = [_.name for _ in dataclasses.fields(Rule)]

    def current_tables(self, tables: TablesCrawler, workspace_name: str, catalog_name: str):
        tables_snapshot = tables.snapshot()
        if len(tables_snapshot) == 0:
            msg = "No tables found. Please run: databricks labs ucx ensure-assessment-run"
            raise ValueError(msg)
        for table in tables_snapshot:
            yield Rule.initial(workspace_name, catalog_name, table)

    def save(self, tables: TablesCrawler, workspace_info: WorkspaceInfo) -> str:
        workspace_name = workspace_info.current()
        default_catalog_name = re.sub(r"\W+", "_", workspace_name)
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, self._field_na)
        writer.writeheader()
        for rule in self.current_tables(tables, workspace_name, default_catalog_name):
            writer.writerow(dataclasses.asdict(rule))
        buffer.seek(0)
        return self._overwrite_mapping(buffer)

    def _overwrite_mapping(self, buffer) -> str:
        path = f"{self._folder}/mapping.csv"
        self._ws.workspace.upload(path, buffer, overwrite=True, format=ImportFormat.AUTO)
        return path

    def load(self) -> list[Rule]:
        try:
            rules = []
            remote = self._ws.workspace.download(f"{self._folder}/mapping.csv")
            for row in csv.DictReader(remote):
                rules.append(Rule(**row))
            return rules
        except NotFound:
            msg = "Please run: databricks labs ucx table-mapping"
            raise ValueError(msg) from None


class ExternalLocationMapping:
    def __init__(self, ws: WorkspaceClient, folder: str | None = None):
        if not folder:
            folder = f"/Users/{ws.current_user.me().user_name}/.ucx"
        self._ws = ws
        self._folder = folder

    def _get_ext_location_definitions(self, missing_locations: list[ExternalLocation]) -> list:
        tf_script = []
        cnt = 1
        for loc in missing_locations:
            script = f'resource "databricks_external_location" "name_{cnt}" cc{{ \n'
            script += f'name = "name_{cnt}"\n'
            script += f'url  = "{loc.location}"\n'
            script += "credential_name = <storage_credential_reference>\n"
            script += "}"
            tf_script.append(script)
            cnt += 1
        return tf_script

    def _match_table_external_locations(self, locations: ExternalLocations) -> tuple(list, list):
        external_locations = list(self._ws.external_locations.list())
        location_path = [_.url for _ in external_locations]
        table_locations = locations.snapshot()
        matching_locations = []
        missing_locations = []
        for loc in table_locations:
            if loc.location in location_path:
                matching_locations.append(
                    [external_locations[external_locations.index(loc.location)].name, loc.table_count]
                )
                continue
            missing_locations.append(loc)
        return matching_locations, missing_locations

    def save(self, locations: ExternalLocations) -> str:
        matching_locations, missing_locations = self._match_table_external_locations(locations)
        logger.info("following external location already configured.")
        logger.info("sharing details of # tables that can be migrated for each location")
        for _ in matching_locations:
            logger.info(f"{_[1]} tables can be migrated using external location {_[0]}.")
        buffer = io.StringIO()
        writer = csv.writer(buffer)
        if len(missing_locations) > 0:
            logger.info("following external location need to be configured.")
            for _ in missing_locations:
                logger.info(f"{_[1]} tables can be migrated using external location {_[0]}.")
            for script in self._get_ext_location_definitions(missing_locations):
                writer.writerow(script)
            buffer.seek(0)
            return self._overwrite_mapping(buffer)
        else:
            return ""

    def _overwrite_mapping(self, buffer) -> str:
        path = f"{self._folder}/external_locations.tf"
        self._ws.workspace.upload(path, buffer, overwrite=True, format=ImportFormat.AUTO)
        return path

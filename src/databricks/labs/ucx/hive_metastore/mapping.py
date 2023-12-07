import csv
import dataclasses
import io
import json
import re
from dataclasses import dataclass

import requests
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.provisioning import Workspace

from databricks.labs.ucx.hive_metastore import TablesCrawler
from databricks.labs.ucx.hive_metastore.tables import Table


class WorkspaceInfoReader:
    def __init__(self, ws: WorkspaceClient, folder: str):
        self._ws = ws
        self._folder = folder

    def _current_workspace_id(self) -> int:
        headers = self._ws.config.authenticate()
        headers["User-Agent"] = self._ws.config.user_agent
        response = requests.get(f"{self._ws.config.host}/api/2.0/preview/scim/v2/Me", headers=headers, timeout=10)
        return int(response.headers.get("x-databricks-org-id"))

    def _load_workspace_info(self) -> dict[int, Workspace]:
        try:
            id_to_workspace = {}
            workspace_info = self._ws.workspace.download(f"{self._folder}/workspace-info.json")
            for workspace_metadata in json.loads(workspace_info):
                workspace = Workspace.from_dict(workspace_metadata)
                id_to_workspace[workspace.workspace_id] = workspace
            return id_to_workspace
        except NotFound:
            msg = "Please run as account-admin: databricks labs ucx sync-workspace-info"
            raise ValueError(msg) from None

    def current(self) -> str:
        workspace_id = self._current_workspace_id()
        workspaces = self._load_workspace_info()
        if workspace_id not in workspaces:
            msg = f"Current workspace is not known: {workspace_id}"
            raise KeyError(msg) from None
        return workspaces[workspace_id].workspace_name


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
    def __init__(self, ws: WorkspaceClient, folder: str):
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

    def save(self, tables: TablesCrawler, workspace_info: WorkspaceInfoReader):
        workspace_name = workspace_info.current()
        default_catalog_name = re.sub(r"\W+", "_", workspace_name)
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, self._field_names)
        writer.writeheader()
        for rule in self.current_tables(tables, workspace_name, default_catalog_name):
            writer.writerow(dataclasses.asdict(rule))
        buffer.seek(0)
        self.overwrite_mapping(buffer)

    def overwrite_mapping(self, buffer):
        self._ws.workspace.upload(f"{self._folder}/mapping.csv", buffer, overwrite=True)

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

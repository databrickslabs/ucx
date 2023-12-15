import csv
import dataclasses
import io
import re
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.workspace import ImportFormat

from databricks.labs.ucx.account import WorkspaceInfo
from databricks.labs.ucx.hive_metastore import TablesCrawler
from databricks.labs.ucx.hive_metastore.tables import Table


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
        writer = csv.DictWriter(buffer, self._field_names)
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
            for row in csv.DictReader(remote):  # type: ignore[arg-type]
                rules.append(Rule(**row))
            return rules
        except NotFound:
            msg = "Please run: databricks labs ucx table-mapping"
            raise ValueError(msg) from None

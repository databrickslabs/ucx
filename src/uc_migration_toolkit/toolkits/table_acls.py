from collections.abc import Iterator
from dataclasses import dataclass
from functools import partial

from databricks.sdk import WorkspaceClient

from uc_migration_toolkit.providers.mixins.sql import StatementExecutionExt


@dataclass
class Grant:
    principal: str
    action_type: str
    object_type: str
    object_key: str


@dataclass
class Table:
    database: str
    table: str
    object_type: str
    location: str
    view_text: str


class TableAclToolkit:
    def __init__(self, ws: WorkspaceClient, warehouse_id):
        self.ws = ws
        sql = StatementExecutionExt(ws.api_client)
        self.hive_exec = partial(sql.execute_fetch_all, warehouse_id, catalog="hive_metastore")

    def all_databases(self) -> Iterator[str]:
        yield from self.hive_exec("SHOW DATABASES")

    def all_tables_in_database(self, database: str) -> Iterator[str]:
        for _, table, is_tmp in self.hive_exec(f"SHOW TABLES IN {database}"):
            if is_tmp:
                continue
            yield table

    def describe(self, database: str, table: str) -> Table:
        describe = {}
        for key, value, _ in self.hive_exec(f"DESCRIBE TABLE EXTENDED {database}.{table}"):
            describe[key] = value
        return Table(
            database=database,
            table=table,
            object_type=describe["Type"],
            location=describe.get("Location", None),
            view_text=describe.get("View Text", None),
        )


def grants(self, on: str) -> Iterator[Grant]:
    # on: TABLE {schema}.{table}
    for row in self.hive_exec(f"SHOW GRANTS ON {on}"):
        (principal, action_type, object_type, object_key) = row
        yield Grant(principal, action_type, object_type, object_key)

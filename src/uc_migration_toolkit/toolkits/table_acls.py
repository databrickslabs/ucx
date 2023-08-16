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

    @property
    def full_name(self) -> str:
        return f'{self.database}.{self.table}'

    @property
    def object_key(self) -> str:
        return f'`{self.database}`.`{self.table}`'

    @property
    def kind(self) -> str:
        return 'VIEW' if self.view_text is not None else 'TABLE'


class TableAclToolkit:
    def __init__(self, ws: WorkspaceClient, warehouse_id):
        self.ws = ws
        sql = StatementExecutionExt(ws.api_client)
        self.hive_exec = partial(sql.execute_fetch_all, warehouse_id, catalog="hive_metastore")

    def all_databases(self) -> Iterator[str]:
        yield from self.hive_exec("SHOW DATABASES")

    def _all_tables_in_database(self, database: str) -> Iterator[str]:
        # TODO: all caching for this value
        for _, table, is_tmp in self.hive_exec(f"SHOW TABLES FROM {database}"):
            # if is_tmp: continue
            yield table

    def _describe(self, database: str, table: str) -> Table:
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

    def describe_all_tables_in_database(self, database: str) -> Iterator[Table]:
        for table in self._all_tables_in_database(database):
            yield self._describe(database, table)

    def all_grants_in_database(self, database: str) -> Iterator[Grant]:
        for g in self._grants(f'SCHEMA {database}'):
            if g.object_type != 'DATABASE':
                continue
            yield g
        for t in self.describe_all_tables_in_database(database):
            for g in self._grants(f'{t.kind} {t.full_name}'):
                if g.object_type != 'TABLE':
                    continue
                yield g

    def _grants(self, on: str) -> Iterator[Grant]:
        # on: TABLE {schema}.{table}
        for row in self.hive_exec(f"SHOW GRANTS ON {on}"):
            (principal, action_type, object_type, object_key) = row
            yield Grant(principal, action_type, object_type, object_key)

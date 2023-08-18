import dataclasses
from functools import partial
from typing import Iterator

from databricks.sdk import WorkspaceClient

from uc_migration_toolkit.providers.logger import logger
from uc_migration_toolkit.providers.mixins.sql import StatementExecutionExt


class CrawlerBase:
    def __init__(self, ws: WorkspaceClient, warehouse_id, catalog, schema, table):
        sql = StatementExecutionExt(ws.api_client)
        self._catalog = self._valid(catalog)
        self._schema = self._valid(schema)
        self._table = self._valid(table)
        self._exec = partial(sql.execute, warehouse_id)
        self._fetch = partial(sql.execute_fetch_all, warehouse_id)

    @property
    def _full_name(self) -> str:
        return f'{self._catalog}.{self._schema}.{self._table}'

    @staticmethod
    def _valid(name: str) -> str:
        if '.' in name:
            raise ValueError(f'no dots allowed in `{name}`')
        return name

    @classmethod
    def _try_valid(cls, name: str):
        if name is None: return None
        return cls._valid(name)

    def _snapshot(self, klass, fetcher, loader) -> list[any]:
        while True:
            try:
                logger.debug(f'[{self._full_name}] fetching {self._table} inventory')
                return list(fetcher())
            except RuntimeError as e:
                if 'TABLE_OR_VIEW_NOT_FOUND' not in str(e):
                    raise e
                logger.debug(f'[{self._full_name}] {self._table} inventory not found, crawling')
                self._append_records(klass, loader())

    @staticmethod
    def _row_to_sql(row, fields):
        data = []
        for f in fields:
            value = getattr(row, f.name)
            if value is None:
                data.append(f"NULL")
            elif f.type == bool:
                data.append('TRUE' if value else 'FALSE')
            elif f.type == str:
                data.append(f"'{value}'")
            else:
                raise ValueError(f'unknown type: {f.type}')
        return ', '.join(data)

    @staticmethod
    def _field_type(f):
        if f.type == bool:
            return 'BOOLEAN'
        elif f.type == str:
            return 'STRING'
        else:
            raise ValueError(f'unknown type: {f.type}')

    def _append_records(self, klass, records: Iterator[any]):
        fields = dataclasses.fields(klass)
        field_names = [f.name for f in fields]
        vals = '), ('.join(self._row_to_sql(r, fields) for r in records)
        sql = f'INSERT INTO {self._full_name} ({", ".join(field_names)}) VALUES ({vals})'
        while True:
            try:
                logger.debug(f'[{self._full_name}] appending records')
                self._exec(sql)
                return
            except RuntimeError as e:
                if 'TABLE_OR_VIEW_NOT_FOUND' not in str(e):
                    raise e
                logger.debug(f'[{self._full_name}] not found. creating')
                schema = ', '.join(f'{f.name} {self._field_type(f)}' for f in fields)
                ddl = f'CREATE TABLE {self._full_name} ({schema}) USING DELTA'
                self._exec(ddl)
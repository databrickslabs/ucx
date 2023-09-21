import dataclasses
import logging
import os
from abc import ABC, abstractmethod
from collections.abc import Iterator
from typing import ClassVar

from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.mixins.sql import StatementExecutionExt

logger = logging.getLogger(__name__)


class SqlBackend(ABC):
    @abstractmethod
    def execute(self, sql):
        raise NotImplementedError

    @abstractmethod
    def fetch(self, sql) -> Iterator[any]:
        raise NotImplementedError

    @abstractmethod
    def save_table(self, full_name: str, rows: list[any], mode: str = "append"):
        raise NotImplementedError

    _builtin_type_mapping: ClassVar[dict[type, str]] = {str: "STRING", int: "INT", bool: "BOOLEAN", float: "FLOAT"}

    @classmethod
    def _schema_for(cls, klass):
        fields = []
        for f in dataclasses.fields(klass):
            if f.type not in cls._builtin_type_mapping:
                msg = f"Cannot auto-convert {f.type}"
                raise SyntaxError(msg)
            not_null = " NOT NULL"
            if f.default is None:
                not_null = ""
            spark_type = cls._builtin_type_mapping[f.type]
            fields.append(f"{f.name} {spark_type}{not_null}")
        return ", ".join(fields)


class StatementExecutionBackend(SqlBackend):
    def __init__(self, ws: WorkspaceClient, warehouse_id, *, max_records_per_batch: int = 1000):
        self._sql = StatementExecutionExt(ws.api_client)
        self._warehouse_id = warehouse_id
        self._max_records_per_batch = max_records_per_batch

    def execute(self, sql):
        logger.debug(f"[api][execute] {sql}")
        self._sql.execute(self._warehouse_id, sql)

    def fetch(self, sql) -> Iterator[any]:
        logger.debug(f"[api][fetch] {sql}")
        return self._sql.execute_fetch_all(self._warehouse_id, sql)

    def save_table(self, full_name: str, rows: list[any], mode="append"):
        if mode == "overwrite":
            msg = "Overwrite mode is not yet supported"
            raise NotImplementedError(msg)

        if len(rows) == 0:
            return

        klass = rows[0].__class__
        ddl = f"CREATE TABLE IF NOT EXISTS {full_name} ({self._schema_for(klass)}) USING DELTA"
        self.execute(ddl)

        fields = dataclasses.fields(klass)
        field_names = [f.name for f in fields]
        for i in range(0, len(rows), self._max_records_per_batch):
            batch = rows[i : i + self._max_records_per_batch]
            vals = "), (".join(self._row_to_sql(r, fields) for r in batch)
            sql = f'INSERT INTO {full_name} ({", ".join(field_names)}) VALUES ({vals})'
            self.execute(sql)

    @staticmethod
    def _row_to_sql(row, fields):
        data = []
        for f in fields:
            value = getattr(row, f.name)
            if value is None:
                data.append("NULL")
            elif f.type == bool:
                data.append("TRUE" if value else "FALSE")
            elif f.type == str:
                value = str(value).replace("'", "''")
                data.append(f"'{value}'")
            elif f.type == int:
                data.append(f"{value}")
            else:
                msg = f"unknown type: {f.type}"
                raise ValueError(msg)
        return ", ".join(data)


class RuntimeBackend(SqlBackend):
    def __init__(self):
        from pyspark.sql.session import SparkSession

        if "DATABRICKS_RUNTIME_VERSION" not in os.environ:
            msg = "Not in the Databricks Runtime"
            raise RuntimeError(msg)

        self._spark = SparkSession.builder.getOrCreate()

    def execute(self, sql):
        logger.debug(f"[spark][execute] {sql}")
        self._spark.sql(sql)

    def fetch(self, sql) -> Iterator[any]:
        logger.debug(f"[spark][fetch] {sql}")
        return self._spark.sql(sql).collect()

    def save_table(self, full_name: str, rows: list[any], mode: str = "append"):
        if len(rows) == 0:
            return
        # pyspark deals well with lists of dataclass instances, as long as schema is provided
        df = self._spark.createDataFrame(rows, self._schema_for(rows[0]))
        df.write.saveAsTable(full_name, mode=mode)


class CrawlerBase:
    def __init__(self, backend: SqlBackend, catalog: str, schema: str, table: str):
        """
        Initializes a CrawlerBase instance.

        Args:
            backend (SqlBackend): The backend that executes SQL queries:
                Statement Execution API or Databricks Runtime.
            catalog (str): The catalog name for the inventory persistence.
            schema: The schema name for the inventory persistence.
            table: The table name for the inventory persistence.
        """
        self._catalog = self._valid(catalog)
        self._schema = self._valid(schema)
        self._table = self._valid(table)
        self._backend = backend
        self._fetch = backend.fetch
        self._exec = backend.execute

    @property
    def _full_name(self) -> str:
        """
        Generates the full name of the table.

        Returns:
            str: The full table name.
        """
        return f"{self._catalog}.{self._schema}.{self._table}"

    @staticmethod
    def _valid(name: str) -> str:
        """
        Validates that the provided name does not contain dots.

        Args:
            name (str): The name to be validated.

        Returns:
            str: The validated name.

        Raises:
            ValueError: If the name contains dots.
        """
        if "." in name:
            msg = f"no dots allowed in `{name}`"
            raise ValueError(msg)
        return name

    @classmethod
    def _try_valid(cls, name: str):
        """
        Tries to validate a name. If None, returns None.

        Args:
            name (str): The name to be validated.

        Returns:
            str or None: The validated name or None.
        """
        if name is None:
            return None
        return cls._valid(name)

    def _snapshot(self, fetcher, loader) -> list[any]:
        """
        Tries to load dataset of records with `fetcher` function, otherwise automatically creates
        a table with the schema defined in the class of the first row and executes `loader` function
        to populate the dataset.

        Args:
            fetcher: A function to fetch existing data.
            loader: A function to load new data.

        Exceptions:
        - If a runtime error occurs during fetching (other than "TABLE_OR_VIEW_NOT_FOUND"), the original error is
          re-raised.

        Returns:
        list[any]: A list of data records, either fetched or loaded.
        """
        logger.debug(f"[{self._full_name}] fetching {self._table} inventory")
        try:
            cached_results = list(fetcher())
            if len(cached_results) > 0:
                return cached_results
        except Exception as err:
            if "TABLE_OR_VIEW_NOT_FOUND" not in str(err):
                raise err
        logger.debug(f"[{self._full_name}] crawling new batch for {self._table}")
        loaded_records = list(loader())
        self._append_records(loaded_records)
        return loaded_records

    def _append_records(self, items):
        if len(items) == 0:
            return
        logger.debug(f"[{self._full_name}] found {len(items)} new records for {self._table}")
        self._backend.save_table(self._full_name, items, mode="append")

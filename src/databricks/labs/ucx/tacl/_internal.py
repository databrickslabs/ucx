import dataclasses
import logging
import os
from abc import ABC, abstractmethod
from collections.abc import Iterator

from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.providers.mixins.sql import StatementExecutionExt

logger = logging.getLogger(__name__)


class SqlBackend(ABC):
    @abstractmethod
    def execute(self, sql):
        raise NotImplementedError

    @abstractmethod
    def fetch(self, sql) -> Iterator[any]:
        raise NotImplementedError


class StatementExecutionBackend(SqlBackend):
    def __init__(self, ws: WorkspaceClient, warehouse_id):
        self._sql = StatementExecutionExt(ws.api_client)
        self._warehouse_id = warehouse_id

    def execute(self, sql):
        logger.debug(f"[api][execute] {sql}")
        self._sql.execute(self._warehouse_id, sql)

    def fetch(self, sql) -> Iterator[any]:
        logger.debug(f"[api][fetch] {sql}")
        return self._sql.execute_fetch_all(self._warehouse_id, sql)


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

    def _snapshot(self, klass, fetcher, loader) -> list[any]:
        """
        Tries to load dataset of records with the type `klass` with `fetcher` function,
        otherwise automatically creates a table with the schema defined in `klass` and
        executes `loader` function to populate the dataset.

        Args:
            klass: The class representing the data structure.
            fetcher: A function to fetch existing data.
            loader: A function to load new data.

        Behavior:
        - Initiates an infinite loop to attempt fetching existing data using the provided fetcher function.
        - If the fetcher function encounters a runtime error with the message "TABLE_OR_VIEW_NOT_FOUND",
          it indicates that the data does not exist in the table.
        - In this case, the method logs that the data is not found and triggers the loader function to load new data.
        - The new data loaded by the loader function is then appended to the existing table using the `_append_records`
          method.

        Note:
        - The method assumes that the provided fetcher and loader functions operate on the same data structure.
        - The fetcher function should return an iterator of data records.
        - The loader function should return an iterator of new data records to be added to the table.

        Exceptions:
        - If a runtime error occurs during fetching (other than "TABLE_OR_VIEW_NOT_FOUND"), the original error is
          re-raised.

        Returns:
        list[any]: A list of data records, either fetched or loaded.
        """
        loaded = False
        trigger_load = ValueError("trigger records load")
        while True:
            try:
                logger.debug(f"[{self._full_name}] fetching {self._table} inventory")
                cached_results = list(fetcher())
                if len(cached_results) == 0 and loaded:
                    return cached_results
                if len(cached_results) == 0 and not loaded:
                    raise trigger_load
                return cached_results
            except Exception as e:
                if not (e == trigger_load or "TABLE_OR_VIEW_NOT_FOUND" in str(e)):
                    raise e
            logger.debug(f"[{self._full_name}] crawling new batch for {self._table}")
            loaded_records = list(loader())
            if len(loaded_records) > 0:
                self._append_records(klass, loaded_records)
            loaded = True

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
                data.append(f"'{value}'")
            else:
                msg = f"unknown type: {f.type}"
                raise ValueError(msg)
        return ", ".join(data)

    @staticmethod
    def _field_type(f):
        if f.type == bool:
            return "BOOLEAN"
        elif f.type == str:
            return "STRING"
        else:
            msg = f"unknown type: {f.type}"
            raise ValueError(msg)

    def _append_records(self, klass, records: Iterator[any]):
        """
        Appends records to the table or creates the table if it does not exist.

        Args:
            klass: The class representing the data structure.
            records (Iterator[any]): An iterator of records to be appended.

        Behavior:
        - Retrieves the fields of the provided class representing the data.
        - Generates a comma-separated list of field names from the fields.
        - Converts each record into a formatted SQL representation using the `_row_to_sql` method.
        - Constructs an SQL INSERT statement with the formatted field names and values.
        - Attempts to execute the INSERT statement using the `_exec` function.
        - If the table does not exist (TABLE_OR_VIEW_NOT_FOUND), it creates the table using a CREATE TABLE statement.

        Note:
        - The method assumes that the target table exists in the database.
        - If the table does not exist, it will be created with the schema inferred from the class fields.
        - If the table already exists, the provided records will be appended to it.

        Exceptions:
        - If a runtime error occurs during execution, it checks if the error message contains "TABLE_OR_VIEW_NOT_FOUND".
        - If the table does not exist, a new table will be created using the schema inferred from the class fields.
        - If the error is different, the original error is re-raised.
        """
        fields = dataclasses.fields(klass)
        field_names = [f.name for f in fields]
        vals = "), (".join(self._row_to_sql(r, fields) for r in records)
        sql = f'INSERT INTO {self._full_name} ({", ".join(field_names)}) VALUES ({vals})'
        while True:
            try:
                logger.debug(f"[{self._full_name}] appending records")
                self._exec(sql)
                return
            except Exception as e:
                if "TABLE_OR_VIEW_NOT_FOUND" not in str(e):
                    raise e
                logger.debug(f"[{self._full_name}] not found. creating")
                schema = ", ".join(f"{f.name} {self._field_type(f)}" for f in fields)
                try:
                    self._exec(f"CREATE TABLE {self._full_name} ({schema}) USING DELTA")
                except Exception as e:
                    schema_not_found = "SCHEMA_NOT_FOUND" in str(e)
                    if not schema_not_found:
                        raise e
                    logger.debug(f"[{self._catalog}.{self._schema}] not found. creating")
                    self._exec(f"CREATE SCHEMA {self._catalog}.{self._schema}")

import dataclasses
import logging
import os
import pkgutil
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Iterator, Sequence
from types import UnionType
from typing import Any, ClassVar, Generic, Protocol, TypeVar

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

from databricks.labs.ucx.mixins.sql import Row, StatementExecutionExt

logger = logging.getLogger(__name__)


class DataclassInstance(Protocol):
    __dataclass_fields__: ClassVar[dict]


Result = TypeVar("Result", bound=DataclassInstance)
Dataclass = type[DataclassInstance]
ResultFn = Callable[[], Iterable[Result]]


class SqlBackend(ABC):
    @abstractmethod
    def execute(self, sql):
        raise NotImplementedError

    @abstractmethod
    def fetch(self, sql) -> Iterator[Any]:
        raise NotImplementedError

    @abstractmethod
    def save_table(self, full_name: str, rows: Sequence[DataclassInstance], klass: Dataclass, mode: str = "append"):
        raise NotImplementedError

    def create_table(self, full_name: str, klass: Dataclass):
        ddl = f"CREATE TABLE IF NOT EXISTS {full_name} ({self._schema_for(klass)}) USING DELTA"
        self.execute(ddl)

    _builtin_type_mapping: ClassVar[dict[type, str]] = {
        str: "STRING",
        int: "LONG",
        bool: "BOOLEAN",
        float: "FLOAT",
    }

    @classmethod
    def _schema_for(cls, klass: Dataclass):
        fields = []
        for f in dataclasses.fields(klass):
            field_type = f.type
            if isinstance(field_type, UnionType):
                field_type = field_type.__args__[0]
            if field_type not in cls._builtin_type_mapping:
                msg = f"Cannot auto-convert {field_type}"
                raise SyntaxError(msg)
            not_null = " NOT NULL"
            if f.default is None:
                not_null = ""
            spark_type = cls._builtin_type_mapping[field_type]
            fields.append(f"{f.name} {spark_type}{not_null}")
        return ", ".join(fields)

    @classmethod
    def _filter_none_rows(cls, rows, klass):
        if len(rows) == 0:
            return rows

        results = []
        class_fields = dataclasses.fields(klass)
        for row in rows:
            if row is None:
                continue
            for field in class_fields:
                if not hasattr(row, field.name):
                    logger.debug(f"Field {field.name} not present in row {dataclasses.asdict(row)}")
                    continue
                if field.default is not None and getattr(row, field.name) is None:
                    msg = f"Not null constraint violated for column {field.name}, row = {dataclasses.asdict(row)}"
                    raise ValueError(msg)
            results.append(row)
        return results


class StatementExecutionBackend(SqlBackend):
    def __init__(self, ws: WorkspaceClient, warehouse_id, *, max_records_per_batch: int = 1000):
        self._sql = StatementExecutionExt(ws.api_client)
        self._warehouse_id = warehouse_id
        self._max_records_per_batch = max_records_per_batch

    def execute(self, sql):
        logger.debug(f"[api][execute] {sql}")
        self._sql.execute(self._warehouse_id, sql)

    def fetch(self, sql) -> Iterator[Row]:
        logger.debug(f"[api][fetch] {sql}")
        return self._sql.execute_fetch_all(self._warehouse_id, sql)

    def save_table(self, full_name: str, rows: Sequence[DataclassInstance], klass: Dataclass, mode="append"):
        if mode == "overwrite":
            msg = "Overwrite mode is not yet supported"
            raise NotImplementedError(msg)
        rows = self._filter_none_rows(rows, klass)
        self.create_table(full_name, klass)
        if len(rows) == 0:
            return
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
            field_type = f.type
            if isinstance(field_type, UnionType):
                field_type = field_type.__args__[0]
            if value is None:
                data.append("NULL")
            elif field_type == bool:
                data.append("TRUE" if value else "FALSE")
            elif field_type == str:
                value = str(value).replace("'", "''")
                data.append(f"'{value}'")
            elif field_type == int:
                data.append(f"{value}")
            else:
                msg = f"unknown type: {field_type}"
                raise ValueError(msg)
        return ", ".join(data)


class RuntimeBackend(SqlBackend):
    def __init__(self):
        from pyspark.sql.session import SparkSession  # type: ignore[import-not-found]

        if "DATABRICKS_RUNTIME_VERSION" not in os.environ:
            msg = "Not in the Databricks Runtime"
            raise RuntimeError(msg)

        self.spark = SparkSession.builder.getOrCreate()

    def execute(self, sql):
        logger.debug(f"[spark][execute] {sql}")
        self.spark.sql(sql)

    def fetch(self, sql) -> Iterator[Row]:
        logger.debug(f"[spark][fetch] {sql}")
        return self.spark.sql(sql).collect()

    def save_table(self, full_name: str, rows: Sequence[DataclassInstance], klass: Dataclass, mode: str = "append"):
        rows = self._filter_none_rows(rows, klass)

        if len(rows) == 0:
            self.create_table(full_name, klass)
            return
        # pyspark deals well with lists of dataclass instances, as long as schema is provided
        df = self.spark.createDataFrame(rows, self._schema_for(klass))
        df.write.saveAsTable(full_name, mode=mode)


class CrawlerBase(Generic[Result]):
    def __init__(self, backend: SqlBackend, catalog: str, schema: str, table: str, klass: type[Result]):
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
        self._klass = klass

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
    def _try_valid(cls, name: str | None):
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

    def _snapshot(self, fetcher: ResultFn, loader: ResultFn) -> list[Result]:
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
        except NotFound:
            pass
        logger.debug(f"[{self._full_name}] crawling new batch for {self._table}")
        loaded_records = list(loader())
        self._append_records(loaded_records)
        return loaded_records

    def _append_records(self, items: Sequence[Result]):
        logger.debug(f"[{self._full_name}] found {len(items)} new records for {self._table}")
        self._backend.save_table(self._full_name, items, self._klass, mode="append")


class SchemaDeployer:
    def __init__(self, sql_backend: SqlBackend, inventory_schema: str, mod: Any):
        self._sql_backend = sql_backend
        self._inventory_schema = inventory_schema
        self._module = mod

    def deploy_schema(self):
        logger.info(f"Ensuring {self._inventory_schema} database exists")
        self._sql_backend.execute(f"CREATE SCHEMA IF NOT EXISTS hive_metastore.{self._inventory_schema}")

    def delete_schema(self):
        logger.info(f"deleting {self._inventory_schema} database")

        self._sql_backend.execute(f"DROP SCHEMA IF EXISTS hive_metastore.{self._inventory_schema} CASCADE")

    def deploy_table(self, name: str, klass: Dataclass):
        logger.info(f"Ensuring {self._inventory_schema}.{name} table exists")
        self._sql_backend.create_table(f"hive_metastore.{self._inventory_schema}.{name}", klass)

    def deploy_view(self, name: str, relative_filename: str):
        query = self._load(relative_filename)
        logger.info(f"Ensuring {self._inventory_schema}.{name} view matches {relative_filename} contents")
        ddl = f"CREATE OR REPLACE VIEW hive_metastore.{self._inventory_schema}.{name} AS {query}"
        self._sql_backend.execute(ddl)

    def _load(self, relative_filename: str) -> str:
        data = pkgutil.get_data(self._module.__name__, relative_filename)
        assert data is not None
        sql = data.decode("utf-8")
        sql = sql.replace("$inventory", f"hive_metastore.{self._inventory_schema}")
        return sql

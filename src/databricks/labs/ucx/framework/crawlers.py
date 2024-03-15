import logging
from collections.abc import Callable, Iterable, Sequence
from typing import ClassVar, Generic, Protocol, TypeVar

from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk.errors import NotFound

logger = logging.getLogger(__name__)


class DataclassInstance(Protocol):
    __dataclass_fields__: ClassVar[dict]


Result = TypeVar("Result", bound=DataclassInstance)
Dataclass = type[DataclassInstance]
ResultFn = Callable[[], Iterable[Result]]


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
    def full_name(self) -> str:
        """
        Generates the full name of the table.

        Returns:
            str: The full table name.
        """
        return f"{self._catalog}.{self._schema}.{self._table}"

    def reset(self):
        """
        Delete the content of the inventory table.
        The next call to `snapshot` will re-populate the table.
        """
        self._exec(f"DELETE FROM {self.full_name}")

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
        logger.debug(f"[{self.full_name}] fetching {self._table} inventory")
        try:
            cached_results = list(fetcher())
            if len(cached_results) > 0:
                return cached_results
        except NotFound:
            pass
        logger.debug(f"[{self.full_name}] crawling new batch for {self._table}")
        loaded_records = list(loader())
        self._append_records(loaded_records)
        return loaded_records

    def _append_records(self, items: Sequence[Result]):
        logger.debug(f"[{self.full_name}] found {len(items)} new records for {self._table}")
        self._backend.save_table(self.full_name, items, self._klass, mode="append")

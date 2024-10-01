import logging
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Sequence
from functools import cached_property
from typing import ClassVar, Generic, Literal, Protocol, TypeVar, final

from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

from databricks.labs.ucx.framework.utils import escape_sql_identifier, find_an_admin

logger = logging.getLogger(__name__)


class DataclassInstance(Protocol):
    __dataclass_fields__: ClassVar[dict]


Result = TypeVar("Result", bound=DataclassInstance)
Dataclass = type[DataclassInstance]
ResultFn = Callable[[], Iterable[Result]]


class CrawlerBase(ABC, Generic[Result]):

    _cached_workspace_admins: dict[int, str | RuntimeError] = {}
    """Cached user names of workspace administrators, keyed by workspace id."""

    def __init__(
        self, ws: WorkspaceClient, backend: SqlBackend, catalog: str, schema: str, table: str, klass: type[Result]
    ):
        """
        Initializes a CrawlerBase instance.

        Args:
            ws (WorkspaceClient): A client for the current workspace.
            backend (SqlBackend): The backend that executes SQL queries:
                Statement Execution API or Databricks Runtime.
            catalog (str): The catalog name for the inventory persistence.
            schema: The schema name for the inventory persistence.
            table: The table name for the inventory persistence.
        """
        self._ws = ws
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
        self._exec(f"TRUNCATE TABLE {escape_sql_identifier(self.full_name)}")

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

    def snapshot(self, *, force_refresh: bool = False) -> Iterable[Result]:
        """Obtain a snapshot of the data that is captured by this crawler.

        If this crawler has already captured data, by default this previously-captured data is returned.
        However if there is no captured data or the `force_refresh` argument is true a (potentially expensive)
        crawl is performed to obtain a fresh snapshot.

        Args:
            force_refresh (bool, optional): If true, the crawler will capture a new snapshot previously-captured
                data is available. If this crawler depends on other crawlers, this argument is _not_ passed on:
                a forced refresh is shallow in nature.
        Returns:
            Iterable[Result]: A snapshot of the data that is captured by this crawler.
        """
        return self._snapshot(self._try_fetch, self._crawl, force_refresh=force_refresh)

    @final
    def owner_of(self, result: Result) -> str:
        """Obtain the user-name of a user that is responsible for the given record.

        This is intended to be a point of contact, and is either:

         - The user that originally created the resource associated with the result; or
         - An active administrator for the current workspace.

        Args:
            result (Result): The record for which an associated user-name is sought.
        Returns:
            A string containing the user-name attribute of the user considered to own the resource.
        Raises:
            RuntimeError if there are no active administrators for the current workspace.
        """
        return self._result_owner(result) or self._workspace_admin

    @cached_property
    def _workspace_admin(self) -> str:
        # Avoid repeatedly hitting the shared cache.
        return self._find_administrator_for(self._ws)

    @classmethod
    @final
    def _find_administrator_for(cls, ws: WorkspaceClient) -> str:
        # Finding an administrator is quite expensive, so we ensure that for a given workspace we only
        # do it once.
        workspace_id = ws.get_workspace_id()
        found_admin_or_error = cls._cached_workspace_admins.get(workspace_id, None)
        if isinstance(found_admin_or_error, str):
            return found_admin_or_error
        if isinstance(found_admin_or_error, RuntimeError):
            raise found_admin_or_error

        found_admin = find_an_admin(ws)
        if found_admin is None or not found_admin.user_name:
            msg = f"No active workspace or account administrator can be found for workspace: {workspace_id}"
            error = RuntimeError(msg)
            cls._cached_workspace_admins[workspace_id] = error
            raise error
        user_name = found_admin.user_name
        cls._cached_workspace_admins[workspace_id] = user_name
        return user_name

    @classmethod
    def _result_owner(cls, result: Result) -> str | None:  # pylint: disable=unused-argument
        """Obtain the record-specific user-name associated with the given result, if any."""
        return None

    @abstractmethod
    def _try_fetch(self) -> Iterable[Result]:
        """Fetch existing data that has (previously) been crawled by this crawler.

        Returns:
            Iterable[Result]: The data that has already been crawled.
        """

    @abstractmethod
    def _crawl(self) -> Iterable[Result]:
        """Perform the (potentially slow) crawling necessary to capture the current state of the environment.

        If this operation depends on the results of other crawlers these MUST NOT force a refresh of the subordinate
        crawler.

        Returns:
            Iterable[Result]: Records that capture the results of crawling the environment.
        """

    def _snapshot(self, fetcher: ResultFn, loader: ResultFn, *, force_refresh: bool) -> list[Result]:
        """
        Tries to load dataset of records with `fetcher` function, otherwise automatically creates
        a table with the schema defined in the class of the first row and executes `loader` function
        to populate the dataset.

        Args:
            fetcher: A function to fetch existing data.
            loader: A function to load new data.
            force_refresh: Whether existing data should be ignored and forcibly replaced with data from the loader.

        Exceptions:
        - Any errors raised by the fetcher are passed through, except for NotFound (usually due to
          TABLE_OR_VIEW_NOT_FOUND) which is suppressed. All errors raised by the loader are passed through.

        Returns:
            list[Result]: A list of data records, either fetched or loaded.
        """
        if force_refresh:
            logger.debug(f"[{self.full_name}] ignoring any existing {self._table} inventory; refresh is forced.")
        else:
            logger.debug(f"[{self.full_name}] fetching {self._table} inventory")
            try:
                cached_results = list(fetcher())
                if cached_results:
                    return cached_results
            except NotFound as e:
                logger.debug("Inventory table not found", exc_info=e)
        logger.debug(f"[{self.full_name}] crawling new set of snapshot data for {self._table}")
        loaded_records = list(loader())
        self._update_snapshot(loaded_records, mode="overwrite")
        return loaded_records

    def _update_snapshot(self, items: Sequence[Result], mode: Literal["append", "overwrite"] = "append") -> None:
        logger.debug(f"[{self.full_name}] found {len(items)} new records for {self._table}")
        self._backend.save_table(self.full_name, items, self._klass, mode=mode)

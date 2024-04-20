import logging
from collections.abc import Iterable
from dataclasses import dataclass
from functools import partial

from databricks.labs.blueprint.parallel import Threads
from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk.errors import Unknown, NotFound

from databricks.labs.ucx.framework.crawlers import CrawlerBase
from databricks.labs.ucx.framework.utils import escape_sql_identifier

logger = logging.getLogger(__name__)


@dataclass
class Udf:
    catalog: str
    database: str
    name: str
    func_type: str
    func_input: str
    func_returns: str
    deterministic: bool
    data_access: str
    body: str
    comment: str = ""

    @property
    def key(self) -> str:
        return f"{self.catalog}.{self.database}.{self.name}".lower()


class UdfsCrawler(CrawlerBase):
    def __init__(self, backend: SqlBackend, schema: str, include_databases: list[str] | None = None):
        """
        Initializes a UdfsCrawler instance.

        Args:
            backend (SqlBackend): The SQL Execution Backend abstraction (either REST API or Spark)
            schema: The schema name for the inventory persistence.
        """
        super().__init__(backend, "hive_metastore", schema, "udfs", Udf)
        self._include_database = include_databases

    def _all_databases(self) -> list[str]:
        if not self._include_database:
            return [row[0] for row in self._fetch("SHOW DATABASES")]
        return self._include_database

    def snapshot(self) -> list[Udf]:
        """
        Takes a snapshot of tables in the specified catalog and database.

        Returns:
            list[Udf]: A list of Udf objects representing the snapshot of tables.
        """
        return self._snapshot(self._try_load, self._crawl)

    def _try_load(self) -> Iterable[Udf]:
        """Tries to load udf information from the database or throws TABLE_OR_VIEW_NOT_FOUND error"""
        for row in self._fetch(f"SELECT * FROM {escape_sql_identifier(self.full_name)}"):
            yield Udf(*row)

    def _crawl(self) -> Iterable[Udf]:
        """Crawls and lists udfs within the specified catalog and database."""
        tasks = []
        catalog = "hive_metastore"
        # need to set the current catalog otherwise "SHOW USER FUNCTIONS FROM" is raising error:
        # "target schema <database> is not in the current catalog"
        self._exec(f"USE CATALOG {escape_sql_identifier(catalog)};")
        for database in self._all_databases():
            for task in self._collect_tasks(catalog, database):
                tasks.append(task)
        catalog_tables, errors = Threads.gather(f"listing udfs in {catalog}", tasks)
        if len(errors) > 0:
            logger.error(f"Detected {len(errors)} while scanning udfs in {catalog}")
        return catalog_tables

    def _collect_tasks(self, catalog, database) -> Iterable[partial[Udf | None]]:
        try:
            logger.debug(f"[{catalog}.{database}] listing udfs")
            for (udf,) in self._fetch(
                f"SHOW USER FUNCTIONS FROM {escape_sql_identifier(catalog)}.{escape_sql_identifier(database)};"
            ):
                if not udf.startswith(f"{catalog}.{database}"):
                    continue
                udf_name = udf[udf.rfind(".") + 1 :]  # remove catalog and database info from the name
                yield partial(self._describe, catalog, database, udf_name)
        except NotFound:
            # This make the integration test more robust as many test schemas are being created and deleted quickly.
            logger.warning(f"Schema {catalog}.{database} no longer existed")
        except Unknown as err:
            logger.error(f"Problem with {database}: {err}")

    def _describe(self, catalog: str, database: str, udf: str) -> Udf | None:
        """Fetches metadata like udf type, input, returns, data access and body
        if specified for a specific udf within the given catalog and database.
        """
        full_name = f"{catalog}.{database}.{udf}"
        try:
            logger.debug(f"[{full_name}] fetching udf metadata")
            describe = {}
            for key_value in self._fetch(f"DESCRIBE FUNCTION EXTENDED {escape_sql_identifier(full_name)}"):
                if ":" in key_value:  # skip free text configs that don't have a key
                    key, value = key_value.split(":")
                    describe[key] = value.strip()
            return Udf(
                catalog=catalog.lower(),
                database=database.lower(),
                name=udf.lower(),
                func_type=describe.get("Type", "UNKNOWN"),
                func_input=describe.get("Input", "UNKNOWN"),
                func_returns=describe.get("Returns", "UNKNOWN"),
                deterministic=describe.get("Deterministic", False),
                data_access=describe.get("Type", "UNKNOWN"),
                comment=describe.get("Comment", "UNKNOWN"),
                body=describe.get("Body", "UNKNOWN"),
            )
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error(f"Couldn't fetch information for udf {full_name} : {e}")
            return None

import logging
from collections.abc import Iterable
from dataclasses import dataclass, replace
from functools import partial

from databricks.labs.blueprint.parallel import Threads
from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk.errors import Unknown, NotFound

from databricks.labs.ucx.framework.crawlers import CrawlerBase
from databricks.labs.ucx.framework.utils import escape_sql_identifier

logger = logging.getLogger(__name__)


@dataclass
class Udf:  # pylint: disable=too-many-instance-attributes
    catalog: str
    database: str
    name: str
    func_type: str
    func_input: str
    func_returns: str
    deterministic: bool
    data_access: str
    body: str
    comment: str
    success: int = 1
    failures: str = ""

    @property
    def key(self) -> str:
        return f"{self.catalog}.{self.database}.{self.name}".lower()


class UdfsCrawler(CrawlerBase[Udf]):
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

    def _try_fetch(self) -> Iterable[Udf]:
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
        udfs, errors = Threads.gather(f"listing udfs in {catalog}", tasks)
        if len(errors) > 0:
            logger.error(f"Detected {len(errors)} while scanning udfs in {catalog}")
        return self._assess_udfs(udfs)

    def _collect_tasks(self, catalog, database) -> Iterable[partial[Udf | None]]:
        """
        Hive metastore supports 2 type of UDFs, SQL UDFs & Hive UDFs, both are created using CREATE FUNCTION
        - https://docs.databricks.com/en/udf/index.html
        - https://docs.databricks.com/en/sql/language-manual/sql-ref-functions-udf-hive.html
        We do not have to worry about Python or Scala UDF/UDAFs, as they cannot be permanent registered in HMS. Those
        UDFs when defined are only valid within the Spark session.
        Unity Catalog supports SQL UDFs, Python UDFs, which means SQL UDFs can be migrated as is. Hive UDFs cannot be
        migrated
        """
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
        The output is different between SQL UDFs and Hive UDFs. Hive UDFs only returns the class & usage.
        """
        full_name = f"{catalog}.{database}.{udf}"
        logger.debug(f"[{full_name}] fetching udf metadata")
        describe = {}
        current_key = ""
        try:
            for row in self._fetch(f"DESCRIBE FUNCTION EXTENDED {escape_sql_identifier(full_name)}"):
                key_value = row["function_desc"]
                if ":" in key_value:
                    current_key, value = key_value.split(":", 1)
                    describe[current_key] = value.strip()
                elif current_key != "":  # append multiline returns, e.g. Input
                    describe[current_key] += f"\n{key_value.strip()}"
            return Udf(
                catalog=catalog.lower(),
                database=database.lower(),
                name=udf.lower(),
                func_type=describe.get("Type", describe.get("Class", "UNKNOWN")),
                func_input=describe.get("Input", "UNKNOWN"),
                func_returns=describe.get("Returns", "UNKNOWN"),
                deterministic=bool(describe.get("Deterministic", False)),
                data_access=describe.get("Data Access", "UNKNOWN"),
                comment=describe.get("Comment", "UNKNOWN"),
                body=describe.get("Body", "UNKNOWN"),
            )
        except Exception as e:  # pylint: disable=broad-exception-caught
            logger.error(f"Couldn't fetch information for udf {full_name} : {e}")
            return None

    @staticmethod
    def _assess_udfs(udfs: Iterable[Udf]) -> Iterable[Udf]:
        for udf in udfs:
            if udf.func_type != "SCALAR":
                yield replace(udf, success=0, failures="Only SCALAR functions are supported")
            else:
                yield replace(udf, success=1)

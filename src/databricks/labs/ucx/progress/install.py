import datetime as dt
import logging

from databricks.labs.lsql.backends import Dataclass, SqlBackend
from databricks.sdk.errors import InternalError
from databricks.sdk.retries import retried

from databricks.labs.ucx.framework.utils import escape_sql_identifier


logger = logging.getLogger(__name__)


class HistoryInstallation:
    """Install resources for UCX's artifacts history.

    `InternalError` are retried on create statements for resilience on sporadic Databricks issues.
    """

    _SCHEMA = "history"

    def __init__(self, sql_backend: SqlBackend, ucx_catalog: str) -> None:
        self._backend = sql_backend
        self._ucx_catalog = ucx_catalog

    def run(self) -> None:
        self._create_schema()
        logger.info("Installation completed successfully!")

    @retried(on=[InternalError], timeout=dt.timedelta(minutes=1))
    def _create_schema(self) -> None:
        schema = f"{self._ucx_catalog}.{self._SCHEMA}"
        logger.info(f"Creating {schema} database...")
        self._backend.execute(f"CREATE SCHEMA IF NOT EXISTS {escape_sql_identifier(schema, maxsplit=1)}")

    @retried(on=[InternalError], timeout=dt.timedelta(minutes=1))
    def _create_table(self, name: str, klass: Dataclass) -> None:
        full_name = f"{self._ucx_catalog}.{self._SCHEMA}.{name}"
        logger.info(f"Create {full_name} table ...")
        self._backend.create_table(escape_sql_identifier(full_name), klass)

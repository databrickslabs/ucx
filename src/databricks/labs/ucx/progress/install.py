import datetime as dt
import logging
from dataclasses import dataclass

from databricks.labs.lsql.backends import Dataclass, SqlBackend
from databricks.sdk.errors import InternalError
from databricks.sdk.retries import retried

from databricks.labs.ucx.framework.utils import escape_sql_identifier


logger = logging.getLogger(__name__)


@dataclass
class Record:
    workspace_id: int  # The workspace id
    run_id: int  # The workflow run id that crawled the objects
    run_start_time: dt.datetime  # The workflow run timestamp that crawled the objects
    object_type: str  # The object type, e.g. TABLE, VIEW. Forms a composite key together with object_id
    object_id: str  # The object id, e.g. hive_metastore.database.table. Forms a composite key together with object_id
    object_data: str  # The object data; the attributes of the corresponding ucx data class, e.g. table name, table ...
    failures: list  # The failures indicating the object is not UC compatible
    owner: str  # The object owner
    ucx_version: str  # The ucx semantic version
    snapshot_id: int  # An identifier for the snapshot


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
        self._create_table("records", Record)
        logger.info(f"Installation completed successfully!")

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

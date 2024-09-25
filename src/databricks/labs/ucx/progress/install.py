import datetime as dt
import logging
from dataclasses import dataclass

from databricks.labs.lsql.backends import Dataclass, SqlBackend
from databricks.sdk.errors import InternalError
from databricks.sdk.retries import retried

from databricks.labs.ucx.framework.utils import escape_sql_identifier

from databricks.labs.lsql.backends import SqlBackend
from databricks.labs.lsql.deployment import SchemaDeployer
from databricks.labs.ucx.progress.workflow_runs import WorkflowRun

logger = logging.getLogger(__name__)


@dataclass
class Record:
    workspace_id: int  # The workspace id
    run_id: int  # The workflow run id that crawled the objects
    run_start_time: dt.datetime  # The workflow run timestamp that crawled the objects
    object_type: str  # The object type, e.g. TABLE, VIEW. Forms a composite key together with object_id
    object_id: str  # The object id, e.g. hive_metastore.database.table. Forms a composite key together with object_id
    object_data: str  # The object data; the attributes of the corresponding ucx data class, e.g. table name, table ...
    failures: list[str]  # The failures indicating the object is not UC compatible
    owner: str  # The object owner
    ucx_version: str  # The ucx semantic version
    snapshot_id: int  # An identifier for the snapshot


class ProgressTrackingInstallation:
    """Install resources for UCX's progress tracking."""

    _SCHEMA = "multiworkspace"

    def __init__(self, sql_backend: SqlBackend, ucx_catalog: str) -> None:
        # `mod` is a required parameter, though, it's not used in this context without views.
        self._schema_deployer = SchemaDeployer(sql_backend, self._SCHEMA, mod=None, catalog=ucx_catalog)

    def run(self) -> None:
        self._schema_deployer.deploy_schema()
        self._schema_deployer.deploy_table("workflow_runs", WorkflowRun)
        self._schema_deployer.deploy_table("history_records", Record)
        logger.info("Installation completed successfully!")

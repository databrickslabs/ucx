from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.marketplace import Installation


class DirectFsMapping:
    FILENAME = 'directfs_mapping.csv'
    UCX_SKIP_PROPERTY = "databricks.labs.ucx.skip"


    def __init__(
        self,
        installation: Installation,
        workspace_client: WorkspaceClient,
        sql_backend: SqlBackend,
    ) -> None:
        self.installation = installation
        self.workspace_client = workspace_client
        self.sql_backend = sql_backend

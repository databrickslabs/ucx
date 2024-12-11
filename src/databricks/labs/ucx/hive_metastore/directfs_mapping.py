from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.marketplace import Installation

from databricks.labs.ucx.source_code.directfs_access import DirectFsAccessCrawler


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


    def directfs_list(self, directfs_crawler: DirectFsAccessCrawler):
        """
        List all direct filesystem access records.
        """
        return directfs_crawler.snapshot()

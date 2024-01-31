import logging

from databricks.sdk import WorkspaceClient

logger = logging.getLogger(__name__)


class AzureServicePrincipalMigration:
    def __init__(self, ws: WorkspaceClient,):
        self._ws = ws
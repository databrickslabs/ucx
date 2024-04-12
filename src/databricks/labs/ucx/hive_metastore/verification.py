import logging

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import PermissionDenied

logger = logging.getLogger(__name__)


class VerifyHasMetastore:
    def __init__(self, ws: WorkspaceClient):
        self.metastore_id: str | None = None
        self.default_catalog_name: str | None = None
        self.workspace_id: int | None = None
        self._ws = ws

    def verify_metastore(self):
        """
        Verifies if a metastore exists for a metastore
        :param :
        :return:
        """

        try:
            current_metastore = self._ws.metastores.current()
            if current_metastore:
                self.default_catalog_name = current_metastore.default_catalog_name
                self.metastore_id = current_metastore.metastore_id
                self.workspace_id = current_metastore.workspace_id
                return True
            raise MetastoreNotFoundError
        except PermissionDenied as err:
            logger.error(f"Cannot access metastore: {err}")
            return False


class MetastoreNotFoundError(Exception):
    def __init__(self, message="Metastore not found in the workspace"):
        self.message = message
        super().__init__(self.message)

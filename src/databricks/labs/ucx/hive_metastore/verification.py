import logging

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound, PermissionDenied

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


class VerifyHasCatalog:
    """Verify a UC catalog exists."""

    def __init__(self, ws: WorkspaceClient, catalog_name: str) -> None:
        self._ws = ws
        self._catalog_name = catalog_name

    def verify(self) -> bool:
        """Verifies if the UC catalog exists."""
        try:
            catalog = self._ws.catalogs.get(self._catalog_name)
            if catalog:
                return True
            return False
        except (NotFound, PermissionDenied) as e:
            logger.error(f"Cannot access catalog: {self._catalog_name}", exc_info=e)
            return False

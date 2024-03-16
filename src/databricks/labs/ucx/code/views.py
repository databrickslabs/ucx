from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.code.base import Fixer
from databricks.labs.ucx.hive_metastore.tables import Table


class Views:
    def __init__(self, fixer: Fixer, ws: WorkspaceClient):
        self._fixer = fixer
        self._ws = ws

    def fix(self, view: Table):
        if self._fixer.match(view.view):
            view.view = self._fixer.apply(view.view)
            # it doesn't belong to this package - it should be in the hive_metastore pacakge
            pass

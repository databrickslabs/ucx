import logging
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.framework.crawlers import CrawlerBase, SqlBackend

logger = logging.getLogger(__name__)


@dataclass
class Mount:
    name: str
    source: str


class Mounts(CrawlerBase):
    def __init__(self, backend: SqlBackend, ws: WorkspaceClient, inventory_database: str):
        super().__init__(backend, "hive_metastore", inventory_database, "mounts")
        self._dbutils = ws.dbutils

    def inventorize_mounts(self):
        self._append_records(self._list_mounts())

    def _list_mounts(self):
        mounts = []
        for mount_point, source, _ in self._dbutils.fs.mounts():
            mounts.append(Mount(mount_point, source))
        return mounts

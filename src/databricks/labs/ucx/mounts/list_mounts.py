import logging
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient

# Added a * import otherwise it won't be possible to import spark directly
from databricks.sdk.runtime import *  # noqa: F403

logger = logging.getLogger(__name__)


@dataclass
class MountResult:
    name: str
    source: str
    instance_profile: str | None = None


class MountLister:
    def __init__(self, ws: WorkspaceClient, inventory_database: str):
        self._ws = ws
        self._inventory_database = inventory_database

    def inventorize_mounts(self):
        mounts = dbutils.fs.mounts()  # noqa: F405
        print(f"found {len(mounts)} mount points in this workspace")
        df = spark.createDataFrame(dbutils.fs.mounts()).selectExpr("mountPoint as name", "source")  # noqa: F405

        target_table = f"{self._inventory_database}.mounts"
        df.writeTo(target_table).replace()
        print(f"All workspace mounts points saved to {target_table}")

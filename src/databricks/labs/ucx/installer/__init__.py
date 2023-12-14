import functools
import logging
from dataclasses import dataclass
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.iam import User

from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.framework.parallel import ManyError, Threads

logger = logging.getLogger(__name__)


@dataclass
class Installation:
    config: WorkspaceConfig
    user: User
    path: str

    def as_summary(self) -> dict:
        return {
            "user_name": self.user.user_name,
            "database": self.config.inventory_database,
            "warehouse_id": self.config.warehouse_id,
        }


class InstallationManager:
    def __init__(self, ws: WorkspaceClient, prefix: str = ".ucx"):
        self._ws = ws
        self._root = Path("/Users")
        self._prefix = prefix

    def for_user(self, user: User) -> Installation | None:
        try:
            config_file = self._root / user.user_name / self._prefix / "config.yml"
            with self._ws.workspace.download(config_file) as f:
                cfg = WorkspaceConfig.from_bytes(f.read())
                return Installation(cfg, user, config_file.parent.as_posix())
        except NotFound:
            return None
        except TypeError:
            logger.warning(f"{user.user_name} installation is corrupt")
            return None

    def user_installations(self) -> list[Installation]:
        tasks = []
        for user in self._ws.users.list(attributes="userName", count=500):
            tasks.append(functools.partial(self.for_user, user))
        installations, errors = Threads.gather("detecting installations", tasks)
        if errors:
            raise ManyError(errors)
        return sorted(installations, key=lambda i: (i.config.inventory_database, i.user.user_name))

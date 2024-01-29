import functools
import logging
from dataclasses import dataclass
from pathlib import Path

import yaml
from databricks.labs.blueprint.installer import IllegalState
from databricks.labs.blueprint.parallel import ManyError, Threads
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.iam import User

from databricks.labs.ucx.config import WorkspaceConfig

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

    def for_user(self, user: User) -> Installation:
        try:
            assert user.user_name is not None
            config_file = self._root / user.user_name / self._prefix / "config.yml"
            with self._ws.workspace.download(config_file.as_posix()) as f:
                cfg = WorkspaceConfig.from_bytes(f.read())
                return Installation(cfg, user, config_file.parent.as_posix())
        except NotFound:
            msg = f"Not installed for {user.user_name}"
            raise NotFound(msg) from None
        except TypeError:
            msg = f"Installation is corrupt for {user.user_name}"
            raise IllegalState(msg) from None
        except yaml.YAMLError:
            msg = f"Config file {config_file} is corrupted, check if it is in correct yaml format"
            raise IllegalState(msg) from None

    def _maybe_for_user(self, user: User) -> Installation | None:
        try:
            return self.for_user(user)
        except NotFound:
            return None
        except IllegalState:
            return None

    def user_installations(self) -> list[Installation]:
        tasks = []
        for user in self._ws.users.list(attributes="userName", count=500):
            tasks.append(functools.partial(self._maybe_for_user, user))
        installations, errors = Threads.gather("detecting installations", tasks)
        if errors:
            raise ManyError(errors)
        return sorted(installations, key=lambda i: (i.config.inventory_database, i.user.user_name))

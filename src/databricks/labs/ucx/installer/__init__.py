import logging
from dataclasses import dataclass
from pathlib import Path

import yaml
from databricks.labs.blueprint.installation import Installation, SerdeError
from databricks.labs.blueprint.installer import IllegalState
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.iam import User

from databricks.labs.ucx.config import WorkspaceConfig

logger = logging.getLogger(__name__)


@dataclass
class InstallationUCX:
    config: WorkspaceConfig
    username: str
    path: str

    def as_summary(self) -> dict:
        return {
            "user_name": self.username,
            "database": self.config.inventory_database,
            "warehouse_id": self.config.warehouse_id,
        }


class InstallationManager:
    def __init__(self, ws: WorkspaceClient, prefix: str = ".ucx"):
        self._ws = ws
        self._root = Path("/Users")
        self._prefix = prefix

    def for_user(self, user: User) -> InstallationUCX:
        try:
            assert user.user_name is not None
            config_file = self._root / user.user_name / self._prefix / "config.yml"
            with self._ws.workspace.download(config_file.as_posix()) as f:
                cfg = WorkspaceConfig.from_bytes(f.read())
                return InstallationUCX(cfg, user, config_file.parent.as_posix())
        except NotFound:
            msg = f"Not installed for {user.user_name}"
            raise NotFound(msg) from None
        except TypeError:
            msg = f"Installation is corrupt for {user.user_name}"
            raise IllegalState(msg) from None
        except yaml.YAMLError:
            msg = f"Config file {config_file} is corrupted, check if it is in correct yaml format"
            raise IllegalState(msg) from None

    def user_installations(self) -> list[InstallationUCX]:
        installations = []
        for inst in Installation.existing(self._ws, 'ucx'):
            try:
                installations.append(
                    InstallationUCX(
                        config=inst.load(WorkspaceConfig), path=inst.install_folder(), username=inst.username()
                    )
                )
            except NotFound:
                continue
            except SerdeError:
                continue
        return sorted(installations, key=lambda i: (i.config.inventory_database, i.username))

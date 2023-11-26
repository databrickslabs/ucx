import json
import logging
from json import JSONDecodeError

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.workspace import ImportFormat

logger = logging.getLogger(__name__)


class InstallState:
    def __init__(self, ws: WorkspaceClient, install_folder: str, version: int = 1):
        self._ws = ws
        self._state_file = f"{install_folder}/state.json"
        self._version = version
        self._state = {}

    def __getattr__(self, item):
        if not self._state:
            self._state = self._load()
        if item not in self._state["resources"]:
            self._state["resources"][item] = {}
        return self._state["resources"][item]

    def _load(self):
        default_state = {"$version": self._version, "resources": {}}
        try:
            raw = json.load(self._ws.workspace.download(self._state_file))
            version = raw.get("$version", None)
            if version != self._version:
                msg = f"expected state $version={self._version}, got={version}"
                raise ValueError(msg)
            return raw
        except NotFound:
            return default_state
        except JSONDecodeError:
            logger.warning(f"JSON state file corrupt: {self._state_file}")
            return default_state

    def save(self):
        state_dump = json.dumps(self._state, indent=2).encode("utf8")
        self._ws.workspace.upload(self._state_file, state_dump, format=ImportFormat.AUTO, overwrite=True)

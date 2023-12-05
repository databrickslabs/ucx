import json
import logging
from pathlib import Path
from typing import ClassVar

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import DatabricksError
from databricks.sdk.errors import NotFound
from databricks.sdk.service.provisioning import Workspace
from databricks.sdk.service.workspace import ImportFormat
from requests.exceptions import ConnectionError

from databricks.labs.ucx.__about__ import __version__
from databricks.labs.ucx.config import AccountConfig

logger = logging.getLogger(__name__)


class Workspaces:
    _tlds: ClassVar[dict[str, str]] = {
        "aws": "cloud.databricks.com",
        "azure": "azuredatabricks.net",
        "gcp": "gcp.databricks.com",
    }

    def __init__(self, cfg: AccountConfig):
        self._ac = cfg.to_account_client()
        self._cfg = cfg

    def configured_workspaces(self):
        for workspace in self._all_workspaces():
            if self._cfg.include_workspace_names:
                if workspace.workspace_name not in self._cfg.include_workspace_names:
                    logger.debug(
                        f"skipping {workspace.workspace_name} ({workspace.workspace_id} because "
                        f"its not explicitly included"
                    )
                    continue
            yield workspace

    def _get_cloud(self) -> str:
        if self._ac.config.is_azure:
            return "azure"
        elif self._ac.config.is_gcp:
            return "gcp"
        return "aws"

    def client_for(self, workspace: Workspace) -> WorkspaceClient:
        config = self._ac.config.as_dict()
        if "databricks_cli_path" in config:
            del config["databricks_cli_path"]
        # copy current config and swap with a host relevant to a workspace
        config["host"] = f"https://{workspace.deployment_name}.{self._tlds[self._get_cloud()]}"
        return WorkspaceClient(**config, product="ucx", product_version=__version__)

    def workspace_clients(self) -> list[WorkspaceClient]:
        """
        Return a list of WorkspaceClient for each configured workspace in the account
        :return: list[WorkspaceClient]
        """
        clients = []
        for workspace in self.configured_workspaces():
            ws = self.client_for(workspace)
            clients.append(ws)
        return clients

    def sync_workspace_info(self):
        """
        Create a json dump for each Workspace in account
        For each user that has ucx installed in their workspace,
        upload the json dump of workspace info in the .ucx folder
        :return:
        """
        workspaces = []
        for workspace in self._ac.workspaces.list():
            workspaces.append(workspace.as_dict())
        workspaces_json = json.dumps(workspaces, indent=2).encode("utf8")

        workspaces_in_account = Workspaces(self._cfg)
        for ws in workspaces_in_account.workspace_clients():
            for user in ws.users.list(attributes="userName"):
                try:
                    potential_install = f"/Users/{user.user_name}/.ucx"
                    ws.workspace.upload(
                        f"{potential_install}/workspaces.json",
                        workspaces_json,
                        overwrite=True,
                        format=ImportFormat.AUTO,
                    )
                except NotFound:
                    continue

    def _all_workspaces(self):
        return self._ac.workspaces.list()


if __name__ == "__main__":
    logger.setLevel("INFO")

    config_file = Path.home() / ".ucx/config.yml"
    cfg = AccountConfig.from_file(config_file)
    wss = Workspaces(cfg)

    for workspace in wss.configured_workspaces():
        ws = wss.client_for(workspace)

        metastore_id = "NOT ASSIGNED"
        default_catalog = "hive_metastore"
        try:
            metastore = ws.metastores.current()
            default_catalog = metastore.default_catalog_name
            metastore_id = metastore.metastore_id
        except DatabricksError:
            pass
        except ConnectionError:
            logger.warning(f"Private DNS for {workspace.workspace_name} is not yet supported?..")

        logger.info(
            f"workspace: {workspace.workspace_name}: "
            f"metastore {metastore_id}, "
            f"default catalog: {default_catalog}"
        )

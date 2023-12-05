import json
import logging
import sys
import webbrowser

from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.account.workspaces import Workspaces
from databricks.labs.ucx.config import AccountConfig
from databricks.labs.ucx.install import WorkspaceInstaller
from databricks.labs.ucx.installer import InstallationManager

logger = logging.getLogger("databricks.labs.ucx")


def workflows():
    ws = WorkspaceClient()
    installer = WorkspaceInstaller(ws)
    logger.info("Fetching deployed jobs...")
    print(json.dumps(installer.latest_job_status()))


def open_remote_config():
    ws = WorkspaceClient()
    installer = WorkspaceInstaller(ws)

    ws_file_url = installer.notebook_link(installer.config_file)
    webbrowser.open(ws_file_url)

def list_installations():
    ws = WorkspaceClient()
    installation_manager = InstallationManager(ws)
    logger.info("Fetching installations...")
    all_users = [_.as_summary() for _ in installation_manager.user_installations()]
    print(json.dumps(all_users))


def sync_workspace_info():
    """
    Cli function to upload a mapping file to each ucx installation folder
    :return:
    """
    workspaces = Workspaces(AccountConfig())
    workspaces.sync_workspace_info()

def workspace_mapping():
    """
    Cli function to upload a mapping file to each ucx installation folder
    :return:
    """
    workspaces = Workspaces(AccountConfig())
    workspaces.workspace_mapping()


MAPPING = {
    "open-remote-config": open_remote_config,
    "installations": list_installations,
    "workflows": workflows,
    "workspace-mapping": workspace_mapping,
}


def main(raw):
    payload = json.loads(raw)
    command = payload["command"]
    if command not in MAPPING:
        msg = f"cannot find command: {command}"
        raise KeyError(msg)
    flags = payload["flags"]
    log_level = flags.pop("log_level")
    if log_level != "disabled":
        databricks_logger = logging.getLogger("databricks")
        databricks_logger.setLevel(log_level.upper())

    kwargs = {k.replace("-", "_"): v for k, v in flags.items()}
    MAPPING[command](**kwargs)


if __name__ == "__main__":
    main(*sys.argv[1:])

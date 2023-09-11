import logging
import subprocess
import sys
import tempfile
import webbrowser
from io import BytesIO
from pathlib import Path

import yaml
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import DatabricksError
from databricks.sdk.service.workspace import ImportFormat

from databricks.labs.ucx.config import GroupsConfig, MigrationConfig, TaclConfig

INSTALL_NOTEBOOK = """
# Databricks notebook source
# MAGIC %md
# MAGIC # UCX - The UC Migration Toolkit
# MAGIC
# MAGIC This notebook installs `ucx` as a wheel package locally
# MAGIC and then restarts the Python interpreter.

# COMMAND ----------

# MAGIC %pip install /Workspace{remote_wheel_file}
dbutils.library.restartPython()

"""

logger = logging.getLogger(__name__)


def main(ws: WorkspaceClient, *, verbose: bool = False):
    folder_base = f"/Users/{ws.current_user.me().user_name}/.ucx"
    # create configuration file only if this installer is called for the first time,
    # otherwise just open file in the browser
    save_config(ws, folder_base)
    with tempfile.TemporaryDirectory() as tmp_dir:
        logger.debug(f"Created temporary directory: {tmp_dir}")
        # build wheel in temp directory
        wheel_file = build_wheel(tmp_dir, verbose=verbose)
        logger.info(f"Wheel file: {wheel_file}")
        # (re)upload wheel and starer notebook to workspace
        upload_artifacts(ws, folder_base, wheel_file)
    logger.info("DONE.")


def save_config(ws: WorkspaceClient, folder_base: str):
    config_path = f"{folder_base}/config.yml"
    ws_file_url = f"{ws.config.host}/#workspace{config_path}"
    try:
        ws.workspace.get_status(config_path)
        logger.info(f"UCX is already configured. See {ws_file_url}")
        if question("Open config file in the browser and continue installing?", default="yes") == "yes":
            webbrowser.open(ws_file_url)
        return config_path
    except DatabricksError as err:
        if err.error_code != "RESOURCE_DOES_NOT_EXIST":
            raise err

    logger.info("Please answer a couple of questions to configure Unity Catalog migration")

    config = MigrationConfig(
        inventory_database=question("Inventory Database", default="ucx"),
        groups=GroupsConfig(
            selected=question("Comma-separated list of workspace group names to migrate").split(","),
            backup_group_prefix=question("Backup prefix", default="db-temp-"),
        ),
        tacl=TaclConfig(auto=True),
        log_level=question("Log level", default="INFO"),
        num_threads=int(question("Number of threads", default="8")),
    )
    ws.workspace.upload(config_path, yaml.dump(config.as_dict()).encode("utf8"), format=ImportFormat.AUTO)
    logger.info(f"Created configuration file: {config_path}")
    if question("Open config file in the browser and continue installing?", default="yes") == "yes":
        webbrowser.open(ws_file_url)


def build_wheel(tmp_dir: str, *, verbose: bool = False):
    """Helper to build the wheel package"""
    streams = {}
    if not verbose:
        streams = {
            "stdout": subprocess.DEVNULL,
            "stderr": subprocess.DEVNULL,
        }
    project_root = find_project_root(Path(__file__))
    if not project_root:
        msg = "Cannot find project root"
        raise NotADirectoryError(msg)
    subprocess.run(
        [sys.executable, "-m", "pip", "wheel", "--no-deps", "--wheel-dir", tmp_dir, project_root], **streams, check=True
    )
    # get wheel name as first file in the temp directory
    return next(Path(tmp_dir).glob("*.whl"))


def upload_artifacts(ws: WorkspaceClient, folder_base, local_wheel: Path):
    """Helper to upload artifacts into a workspace folder"""
    remote_wheel_file = f"{folder_base}/{local_wheel.name}"
    remote_notebook_file = f"{folder_base}/install_ucx.py"
    logger.info(f"Remote wheel file: {remote_wheel_file}")
    logger.info(f"Remote notebook file: {remote_notebook_file}")
    logger.info("Uploading...")
    ws.workspace.mkdirs(folder_base)
    with local_wheel.open("rb") as fh:
        ws.workspace.upload(remote_wheel_file, fh, format=ImportFormat.AUTO, overwrite=True)
    buf = BytesIO(INSTALL_NOTEBOOK.format(remote_wheel_file=remote_wheel_file).encode())
    ws.workspace.upload(remote_notebook_file, buf, overwrite=True)


def find_dir_with_leaf(folder: Path, leaf: str) -> Path | None:
    root = folder.root
    while str(folder.absolute()) != root:
        if (folder / leaf).exists():
            return folder
        folder = folder.parent
    return None


def find_project_root(folder: Path) -> Path | None:
    for leaf in ["pyproject.toml", "setup.py"]:
        root = find_dir_with_leaf(folder, leaf)
        if root is not None:
            return root
    return None


def question(text: str, *, default: str | None = None) -> str:
    default_help = "" if default is None else f"\033[36m (default: {default})\033[0m"
    prompt = f"\033[1m{text}{default_help}: \033[0m"
    res = None
    while not res:
        res = input(prompt)
        if not res and default is not None:
            return default
    return res

import argparse
import logging
import os
import shutil
import subprocess
import sys
import tempfile
from io import BytesIO

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import DatabricksError
from databricks.sdk.service.workspace import ImportFormat

from databricks.labs.ucx.logger import _install

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

CONFIG_FILE = """
inventory:
  table:
    catalog: main
    database: default
    name: uc_migration_inventory

tacl:
  databases: [ "default" ]

warehouse_id: None

groups:
  selected: [ "analyst" ]

num_threads: 80
"""

# install logging backend
_install()
logger = logging.getLogger(__name__)

# parse command line parameters
parser = argparse.ArgumentParser(prog="ucx", description="Builds and installs ucx.")
parser.add_argument("--folder", "-f", default="ucx", help="name of folder in workspace, default: ucx")
parser.add_argument("--quiet", action="store_true", help="suppress extraneous information")
parser.add_argument("--debug", action="store_true", help="enable debug mode")
args = parser.parse_args()

# adjust logging levels as needed
if args.debug:
    logging.getLogger("databricks").setLevel("DEBUG")


def delete_local_dir(dir_name):
    """Helper to delete a directory"""
    try:
        shutil.rmtree(dir_name)
    except OSError as e:
        logger.error(f"Error: {e.filename} - {e.strerror}.")


def folder_exists(folder_base, ws):
    """Helper to check if a workspace folder exists"""
    folder_files = []
    try:
        for f in ws.workspace.list(folder_base):
            folder_files.append(f.path)
        logger.debug(f"Folder files: {folder_files}")
        return True
    except DatabricksError:
        return False


def build_wheel():
    """Helper to build the wheel package"""
    tmp_dir = tempfile.TemporaryDirectory()
    logger.debug(f"Created temporary directory: {tmp_dir.name}")
    streams = {}
    if args.quiet:
        streams = {
            "stdout": subprocess.DEVNULL,
            "stderr": subprocess.DEVNULL,
        }
    subprocess.run(
        [sys.executable, "-m", "pip", "wheel", "--no-deps", "--wheel-dir", tmp_dir.name, ".."], **streams, check=True
    )
    return tmp_dir.name


def upload_artifacts(folder_base, local_wheel_file, wheel_file_name, ws):
    """Helper to upload artifacts into a workspace folder"""
    remote_wheel_file = f"{folder_base}/{wheel_file_name}"
    remote_dbfs_wheel_file = f"/FileStore/jars/{folder_base}/{wheel_file_name}"
    remote_notebook_file = f"{folder_base}/install_ucx.py"
    remote_configuration_file = f"{folder_base}/ucx_config.yaml"
    logger.info(f"Remote wheel file: {remote_wheel_file}")
    logger.info(f"Remote notebook file: {remote_notebook_file}")
    logger.info(f"Remote configuration file: {remote_configuration_file}")
    logger.info("Uploading...")
    ws.workspace.mkdirs(folder_base)
    with open(local_wheel_file, "rb") as fh:
        ws.workspace.upload(path=remote_wheel_file, content=fh.read(), format=ImportFormat.AUTO)
    buf = BytesIO(INSTALL_NOTEBOOK.format(remote_wheel_file=remote_wheel_file).encode())
    configs = BytesIO(CONFIG_FILE.format(remote_wheel_file=remote_wheel_file).encode())
    ws.workspace.upload(path=remote_notebook_file, content=buf)
    ws.workspace.upload(path=remote_configuration_file, content=configs)
    with open(local_wheel_file, "rb") as fh:
        ws.dbfs.upload(path=remote_dbfs_wheel_file, content=fh.read(), format=ImportFormat.AUTO)


def main():
    # preflight check
    ws = WorkspaceClient()
    folder_base = f"/Users/{ws.current_user.me().user_name}/{args.folder}"
    if folder_exists(folder_base, ws):
        logger.error(f"ERROR: Remote folder '{folder_base}' already exists, aborting!")
        sys.exit(-1)
    # build wheel in temp directory
    tmp_dir = build_wheel()
    # get wheel name as first file in the temp directory
    files = os.listdir(tmp_dir)
    wheel_file_name = files[0]
    local_wheel_file = tmp_dir + "/" + wheel_file_name
    logger.info(f"Wheel file: {wheel_file_name}")
    # upload wheel, config and starter notebook to workspace
    upload_artifacts(folder_base, local_wheel_file, wheel_file_name, ws)
    # cleanup
    delete_local_dir(tmp_dir)
    logger.info("DONE.")


if __name__ == "__main__":
    main()

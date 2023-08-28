import os
import sys
from io import BytesIO
import shutil
import argparse
import subprocess
import tempfile

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat

INSTALL_NOTEBOOK = """
# Databricks notebook source
# MAGIC %md
# MAGIC # UCX - The UC Migration Toolkit
# MAGIC
# MAGIC This notebook installs `ucx` as a Wheel package locally.
# MAGIC and restart the Python interpreter.

# COMMAND ----------

# MAGIC %pip install {remote_wheel_file}
dbutils.library.restartPython()

"""

parser = argparse.ArgumentParser(prog="ucx",
                                 description="Builds and installs ucx.")
parser.add_argument("--folder", "-f", default="ucx",
                    help="name of folder in workspace, default: ucx")
parser.add_argument("--verbose", "-v", action="store_true",
                    help="increase output verbosity")
parser.add_argument("--debug", action="store_true",
                    help="enable debug mode")
args = parser.parse_args()


def delete_local_dir(dir):
    try:
        shutil.rmtree(dir)
    except OSError as e:
        if args.verbose:
            print(f"Error: {e.filename} - {e.strerror}.")


def main():
    # build wheel in temp directory
    tmp_dir = tempfile.TemporaryDirectory()
    if args.verbose:
        print(f"Created temporary directory: {tmp_dir.name}")
    if args.verbose:
        subprocess.run([
            "python3", "-m", "pip",
            "wheel", "--no-deps",
            "--wheel-dir", tmp_dir.name,
            ".."],
            check=True)
    else:
        subprocess.run([
            "python3", "-m", "pip",
            "wheel", "--no-deps", "--quiet",
            "--wheel-dir", tmp_dir.name,
            ".."],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            check=True)
    # get wheel name as first file in the temp directory
    files = os.listdir(tmp_dir.name)
    wheel_file_name = files[0]
    local_wheel_file = tmp_dir.name + '/' + wheel_file_name
    if args.verbose:
        print(f"Wheel file: {wheel_file_name}")
    # upload wheel and starer notebook to workspace
    ws = WorkspaceClient()
    folder_base = f"/Users/{ws.current_user.me().user_name}/{args.folder}"
    remote_wheel_file = f"{folder_base}/{wheel_file_name}"
    remote_notebook_file = f"{folder_base}/install_ucx.py"
    if args.verbose:
        print(f"Remote wheel file: {remote_wheel_file}")
        print(f"Remote notebook file: {remote_notebook_file}")
        print("Uploading...")
    try:
        folder_files = []
        for f in ws.workspace.list(folder_base):
            folder_files.append(f.path)
        print(f"ERROR: Remote folder '{folder_base}' already exists!")
        print(f"Found: {folder_files} - ABORTING!")
        sys.exit(-1)
    except:
        pass
    ws.workspace.mkdirs(folder_base)
    with open(local_wheel_file, "rb") as fh:
        buf = BytesIO(fh.read())
        ws.workspace.upload(
            path=remote_wheel_file,
            content=buf,
            format=ImportFormat.AUTO
        )
    buf = BytesIO(INSTALL_NOTEBOOK.format(
        remote_wheel_file=remote_wheel_file).encode())
    ws.workspace.upload(
        path=remote_notebook_file,
        content=buf
    )
    # cleanup
    delete_local_dir(tmp_dir.name)
    if args.verbose:
        print("DONE.")


if __name__ == "__main__":
    main()
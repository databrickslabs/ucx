import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from databricks.sdk.service.workspace import ImportFormat

from databricks.labs.ucx.providers.mixins.compute import CommandExecutor


@pytest.fixture
def fresh_wheel_file(tmp_path) -> Path:
    this_file = Path(__file__)
    project_root = this_file.parent.parent.parent.absolute()
    # TODO: we can dynamically determine this with python -m build .
    wheel_name = "databricks_labs_ucx"

    build_root = tmp_path / fresh_wheel_file.__name__
    shutil.copytree(project_root, build_root)
    try:
        completed_process = subprocess.run(
            [sys.executable, "-m", "pip", "wheel", "."],
            capture_output=True,
            cwd=build_root,
            check=True,
        )
        if completed_process.returncode != 0:
            raise RuntimeError(completed_process.stderr)

        found_wheels = list(build_root.glob(f"{wheel_name}-*.whl"))
        if not found_wheels:
            msg = f"cannot find {wheel_name}-*.whl"
            raise RuntimeError(msg)
        if len(found_wheels) > 1:
            msg = f'more than one wheel match: {", ".join(found_wheels)}'
            raise RuntimeError(msg)
        wheel_file = found_wheels[0]

        return wheel_file
    except subprocess.CalledProcessError as e:
        raise RuntimeError(e.stderr) from None


@pytest.fixture
def wsfs_wheel(ws, fresh_wheel_file, make_random):
    my_user = ws.current_user.me().user_name
    workspace_location = f"/Users/{my_user}/wheels/{make_random(10)}"
    ws.workspace.mkdirs(workspace_location)

    wsfs_wheel = f"{workspace_location}/{fresh_wheel_file.name}"
    with fresh_wheel_file.open("rb") as f:
        ws.workspace.upload(wsfs_wheel, f, format=ImportFormat.AUTO)

    yield wsfs_wheel

    ws.workspace.delete(workspace_location, recursive=True)


def test_this_wheel_installs(ws, wsfs_wheel):
    commands = CommandExecutor(ws)

    commands.install_notebook_library(f"/Workspace{wsfs_wheel}")
    installed_version = commands.run(
        """
    from databricks.labs.ucx.__about__ import __version__
    print(__version__)
    """
    )

    assert installed_version is not None


def test_sql_backend_works(ws, wsfs_wheel):
    commands = CommandExecutor(ws)

    commands.install_notebook_library(f"/Workspace{wsfs_wheel}")
    installed_version = commands.run(
        """
    from databricks.labs.ucx.tacl._internal import SqlBackend
    backend = SqlBackend()
    print(list(backend.fetch("SHOW DATABASES")))
    """
    )

    assert installed_version is not None

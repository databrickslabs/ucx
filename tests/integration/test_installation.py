import logging
import os
import shutil
import subprocess
import sys
from pathlib import Path

import pytest
from databricks.sdk.service.workspace import ImportFormat

from databricks.labs.ucx.providers.mixins.compute import CommandExecutor

logging.getLogger('databricks.sdk').setLevel('DEBUG')


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
            conflicts = ", ".join(str(whl) for whl in found_wheels)
            msg = f"more than one wheel match: {conflicts}"
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

@pytest.fixture
def dbfs_wheel(ws, fresh_wheel_file, make_random):
    my_user = ws.current_user.me().user_name
    workspace_location = f"/FileStore/jars/{make_random(10)}"
    ws.workspace.mkdirs(workspace_location)

    dbfs_wheel = f"{workspace_location}/{fresh_wheel_file.name}"
    with fresh_wheel_file.open("rb") as f:
        ws.dbfs.upload(dbfs_wheel, f)

    yield dbfs_wheel

    ws.dbfs.delete(workspace_location, recursive=True)


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
    database_names = commands.run(
        """
        from databricks.labs.ucx.tacl._internal import RuntimeBackend
        backend = RuntimeBackend()
        return backend.fetch("SHOW DATABASES")
        """
    )

    assert len(database_names) > 0


def test_wheel_job(ws, wsfs_wheel, sql_exec, make_catalog, make_schema, make_table, make_group):
    commands = CommandExecutor(ws)
    commands.install_notebook_library(f"/Workspace{wsfs_wheel}")

    cluster_id = os.environ["DATABRICKS_CLUSTER_ID"]

    inventory_schema = make_schema(catalog=make_catalog())
    inventory_catalog, inventory_schema = inventory_schema.split(".")

    crawl_tacl(cluster_id, ws, inventory_catalog, inventory_schema, wsfs_wheel)


from databricks.sdk.service.jobs import Task
from databricks.sdk.service.compute import ClusterSpec
from dataclasses import dataclass
from typing import Optional

@dataclass
class TaskExt(Task):
    new_cluster: Optional['jobs.ClusterSpec'] = None


@dataclass
class ClusterSpecExt(ClusterSpec):
    libraries: Optional['List[compute.Library]'] = None






def test_job_creation(ws, wsfs_wheel):
    logging.getLogger('databricks').setLevel('DEBUG')

    from databricks.sdk.service import jobs, compute
    #notebook_path = f'{os.path.dirname(wsfs_wheel)}/wrapper.py'
    #ws.workspace.upload(notebook_path, io.BytesIO(b'from databricks.labs.ucx.toolkits.table_acls import main; main()'))
    created_job = ws.jobs.create(
        tasks=[
            Task(
                task_key='crawl',
                python_wheel_task=jobs.PythonWheelTask(
                    package_name='databricks.labs.ucx.toolkits.table_acls',
                    entry_point='main',
                    parameters=['inventory_catalog', 'inventory_schema'],
                ),
                libraries=[compute.Library(whl=f"dbfs:{wsfs_wheel}")],
                new_cluster=compute.ClusterSpec(
                    node_type_id=ws.clusters.select_node_type(local_disk=True),
                    spark_version=ws.clusters.select_spark_version(latest=True),
                    num_workers=1,
                    ),
                )
        ],
        name='[UCX] Crawl Tables',
    )
    ws.jobs.run_now(created_job.job_id).result()
    print(created_job)


import logging
import shutil
import subprocess
import sys
from io import BytesIO
from pathlib import Path

import pytest
from databricks.sdk.service import compute, jobs
from databricks.sdk.service.workspace import ImportFormat

from databricks.labs.ucx.providers.mixins.compute import CommandExecutor

logger = logging.getLogger(__name__)


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


def test_toolkit_notebook(
    acc,
    ws,
    sql_exec,
    wsfs_wheel,
    make_acc_group,
    make_catalog,
    make_cluster,
    make_cluster_policy,
    make_directory,
    make_group,
    make_instance_pool,
    make_job,
    make_notebook,
    make_pipeline,
    make_random,
    make_repo,
    make_secret_scope,
    make_schema,
    make_table,
    make_user,
):
    logger.info("setting up fixtures")

    user_a = make_user()
    user_b = make_user()
    user_c = make_user()

    logger.info(f"user_a={user_a}, user_b={user_b}, user_c={user_c}, ")

    # TODO acc_group
    # TODO add users to groups
    group_a = make_group()
    group_b = make_group()
    group_c = make_group()

    selected_groups = ",".join([group_a.display_name, group_b.display_name, group_c.display_name])

    logger.info(
        f"group_a={group_a}, " 
        f"group_b={group_b}, "
        f"group_c={group_c}, ")

    cluster = make_cluster(num_workers=1)
    cluster_policy = make_cluster_policy()
    directory = make_directory()
    instance_pool = make_instance_pool()
    job = make_job()
    notebook = make_notebook()
    pipeline = make_pipeline()
    repo = make_repo()
    secret_scope = make_secret_scope()

    logger.info(
        f"cluster={cluster}, "
        f"cluster_policy={cluster_policy}, "
        f"directory={directory}, "
        f"instance_pool={instance_pool}, "
        f"job={job}, "
        f"notebook={notebook}, "
        f"pipeline={pipeline}"
        f"repo={repo}, "
        f"secret_scope={secret_scope}, "
    )

    # TODO create fixtures for DBSQL assets
    # TODO set permissions

    schema_a = make_schema()
    schema_b = make_schema()
    schema_c = make_schema()
    table_a = make_table(schema=schema_a)
    table_b = make_table(schema=schema_b)

    logger.info(
        f"schema_a={schema_a}, "
        f"schema_b={schema_b}, "
        f"schema_c={schema_c}, "
        f"table_a={table_a}, "
        f"table_b={table_b}, "
    )

    databases = ",".join([schema_a.split(".")[1], schema_b.split(".")[1], schema_c.split(".")[1]])

    sql_exec(f"GRANT USAGE ON SCHEMA default TO `{group_a.display_name}`")
    sql_exec(f"GRANT USAGE ON SCHEMA default TO `{group_b.display_name}`")
    sql_exec(f"GRANT SELECT ON TABLE {table_a} TO `{group_a.display_name}`")
    sql_exec(f"GRANT SELECT ON TABLE {table_b} TO `{group_b.display_name}`")
    sql_exec(f"GRANT MODIFY ON SCHEMA {schema_b} TO `{group_b.display_name}`")

    inventory_table = make_table(schema=make_schema(catalog=make_catalog()))
    inventory_catalog, inventory_schema, inventory_table = inventory_table.split(".")

    logger.info(
        f"inventory_catalog={inventory_catalog}, "
        f"inventory_schema={inventory_schema}, "
        f"inventory_table={inventory_table}, "
    )

    logger.info("uploading notebook")

    ucx_notebook_path = Path("./test_toolkit_notebook.py").absolute()
    my_user = ws.current_user.me().user_name
    remote_ucx_notebook_location = f"/Users/{my_user}/notebooks/{make_random(10)}"
    ws.workspace.mkdirs(remote_ucx_notebook_location)
    ws_notebook = f"{remote_ucx_notebook_location}/test_notebook.py"

    with open(ucx_notebook_path, "rb") as fh:
        buf_notebook = BytesIO(fh.read())
    ws.workspace.upload(ws_notebook, buf_notebook, format=ImportFormat.AUTO)

    logger.info("creating job")

    created_job = ws.jobs.create(
        tasks=[
            jobs.Task(
                task_key="uc-migrate",
                notebook_task=jobs.NotebookTask(
                    notebook_path=f"{remote_ucx_notebook_location}/test_notebook",
                    base_parameters={
                        "inventory_catalog": inventory_catalog,
                        "inventory_schema": inventory_schema,
                        "selected_groups": selected_groups,
                        "databases": databases,
                    },
                ),
                libraries=[compute.Library(whl=f"/Workspace{wsfs_wheel}")],
                new_cluster=compute.ClusterSpec(
                    node_type_id=ws.clusters.select_node_type(local_disk=True),
                    spark_version=ws.clusters.select_spark_version(latest=True),
                    num_workers=1,
                    spark_conf={"spark.databricks.acl.sqlOnly": True},
                ),
            )
        ],
        name="[UCX] Run Migration",
    )

    logger.info("running job")

    ws.jobs.run_now(created_job.job_id).result()

    # TODO Validate migration, tacl

    logger.info("deleting workbook")

    ws.workspace.delete(remote_ucx_notebook_location, recursive=True)

    logger.info("deleting job")

    ws.jobs.delete(created_job.job_id)

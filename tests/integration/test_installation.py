import logging
import os
import random
import shutil
import subprocess
import sys
from io import BytesIO
from pathlib import Path

import pytest
from databricks.sdk.service import compute, jobs
from databricks.sdk.service.iam import PermissionLevel
from databricks.sdk.service.workspace import ImportFormat

from databricks.labs.ucx.inventory.types import RequestObjectType
from databricks.labs.ucx.providers.mixins.compute import CommandExecutor
from databricks.labs.ucx.tacl.grants import Grant
from databricks.labs.ucx.tacl.tables import Table

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


def test_creating_workflows(ws):
    from databricks.labs.ucx.install import Installer

    inst = Installer(ws)

    inst._create_jobs()


def test_toolkit_notebook(
    ws,
    sql_exec,
    sql_fetch_all,
    wsfs_wheel,
    make_cluster_policy,
    make_cluster_policy_permissions,
    make_ucx_group,
    make_job,
    make_job_permissions,
    make_random,
    make_schema,
    make_table,
):
    logger.info("setting up fixtures")

    ws_group_a, acc_group_a = make_ucx_group()
    members_src_a = sorted([_.display for _ in ws.groups.get(id=ws_group_a.id).members])
    ws_group_b, acc_group_b = make_ucx_group()
    members_src_b = sorted([_.display for _ in ws.groups.get(id=ws_group_b.id).members])
    ws_group_c, acc_group_c = make_ucx_group()
    members_src_c = sorted([_.display for _ in ws.groups.get(id=ws_group_c.id).members])

    selected_groups = ",".join([ws_group_a.display_name, ws_group_b.display_name, ws_group_c.display_name])

    logger.info(f"group_a={ws_group_a}, group_b={ws_group_b}, group_c={ws_group_c}, ")

    cluster_policy = make_cluster_policy()
    make_cluster_policy_permissions(
        object_id=cluster_policy.policy_id,
        permission_level=random.choice([PermissionLevel.CAN_USE]),
        group_name=ws_group_a.display_name,
    )
    cpp_src = ws.permissions.get(RequestObjectType.CLUSTER_POLICIES, cluster_policy.policy_id)
    cluster_policy_src_permissions = sorted(
        [_ for _ in cpp_src.access_control_list if _.group_name == ws_group_a.display_name],
        key=lambda p: p.group_name,
    )
    job = make_job()
    make_job_permissions(
        object_id=job.job_id,
        permission_level=random.choice(
            [PermissionLevel.CAN_VIEW, PermissionLevel.CAN_MANAGE_RUN, PermissionLevel.CAN_MANAGE]
        ),
        group_name=ws_group_b.display_name,
    )
    jp_src = ws.permissions.get(RequestObjectType.JOBS, job.job_id)
    job_src_permissions = sorted(
        [_ for _ in jp_src.access_control_list if _.group_name == ws_group_b.display_name],
        key=lambda p: p.group_name,
    )
    logger.info(f"cluster_policy={cluster_policy}, job={job}, ")

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

    sql_exec(f"GRANT USAGE ON SCHEMA default TO `{ws_group_a.display_name}`")
    sql_exec(f"GRANT USAGE ON SCHEMA default TO `{ws_group_b.display_name}`")
    sql_exec(f"GRANT SELECT ON TABLE {table_a} TO `{ws_group_a.display_name}`")
    sql_exec(f"GRANT SELECT ON TABLE {table_b} TO `{ws_group_b.display_name}`")
    sql_exec(f"GRANT MODIFY ON SCHEMA {schema_b} TO `{ws_group_b.display_name}`")

    _, inventory_database = make_schema(catalog="hive_metastore").split(".")

    logger.info(f"inventory_schema={inventory_database}")

    logger.info("uploading notebook")

    ucx_notebook_path = Path(__file__).parent.parent.parent / "notebooks" / "toolkit.py"
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
                        "inventory_database": inventory_database,
                        "selected_groups": selected_groups,
                        "databases": databases,
                    },
                ),
                libraries=[compute.Library(whl=f"/Workspace{wsfs_wheel}")],
                new_cluster=compute.ClusterSpec(
                    instance_pool_id=os.environ["TEST_INSTANCE_POOL_ID"],
                    spark_version=ws.clusters.select_spark_version(latest=True),
                    num_workers=1,
                    spark_conf={"spark.databricks.acl.sqlOnly": "true"},
                ),
            )
        ],
        name="[UCX] Run Migration",
    )

    logger.info("running job")

    try:
        ws.jobs.run_now(created_job.job_id).result()

        logger.info("validating group ids")

        dst_ws_group_a = ws.groups.list(filter=f"displayName eq {ws_group_a.display_name}")[0]
        assert (
            ws_group_a.id != dst_ws_group_a.id
        ), f"Group id for target group {ws_group_a.display_name} should differ from group id of source group"

        dst_ws_group_b = ws.groups.list(filter=f"displayName eq {ws_group_b.display_name}")[0]
        assert (
            ws_group_b.id != dst_ws_group_b.id
        ), f"Group id for target group {ws_group_b.display_name} should differ from group id of source group"

        dst_ws_group_c = ws.groups.list(filter=f"displayName eq {ws_group_c.display_name}")[0]
        assert (
            ws_group_c.id != dst_ws_group_c.id
        ), f"Group id for target group {ws_group_c.display_name} should differ from group id of source group"

        logger.info("validating group members")

        members_dst_a = sorted([_.display for _ in ws.groups.get(id=dst_ws_group_a.id).members])
        assert members_dst_a == members_src_a, f"Members from {ws_group_a.display_name} were not migrated correctly"

        members_dst_b = sorted([_.display for _ in ws.groups.get(id=dst_ws_group_b.id).members])
        assert members_dst_b == members_src_b, f"Members in {ws_group_b.display_name} were not migrated correctly"

        members_dst_c = sorted([_.display for _ in ws.groups.get(id=dst_ws_group_c.id).members])
        assert members_dst_c == members_src_c, f"Members in {ws_group_c.display_name} were not migrated correctly"

        logger.info("validating permissions")

        cp_dst = ws.permissions.get(RequestObjectType.CLUSTER_POLICIES, cluster_policy.policy_id)
        cluster_policy_dst_permissions = sorted(
            [_ for _ in cp_dst.access_control_list if _.group_name == ws_group_a.display_name],
            key=lambda p: p.group_name,
        )
        assert len(cluster_policy_dst_permissions) == len(
            cluster_policy_src_permissions
        ), "Target permissions were not applied correctly for cluster policies"
        assert [t.all_permissions for t in cluster_policy_dst_permissions] == [
            s.all_permissions for s in cluster_policy_src_permissions
        ], "Target permissions were not applied correctly for cluster policies"

        jp_dst = ws.permissions.get(RequestObjectType.JOBS, job.job_id)
        job_dst_permissions = sorted(
            [_ for _ in jp_dst.access_control_list if _.group_name == ws_group_b.display_name],
            key=lambda p: p.group_name,
        )
        assert len(job_dst_permissions) == len(
            job_src_permissions
        ), f"Target permissions were not applied correctly for {RequestObjectType.JOBS}/{job.job_id}"
        assert [t.all_permissions for t in job_dst_permissions] == [
            s.all_permissions for s in job_src_permissions
        ], f"Target permissions were not applied correctly for {RequestObjectType.JOBS}/{job.job_id}"

        logger.info("validating tacl")

        tables = sql_fetch_all(f"SELECT * FROM hive_metastore.{inventory_database}.tables")
        print(list(sql_fetch_all(f"SELECT * FROM hive_metastore.{inventory_database}.tables")))

        all_tables = {}
        for t_row in tables:
            table = Table(*t_row)
            all_tables[table.key] = table

        assert len(all_tables) >= 2, "must have at least two tables"

        logger.debug(f"all tables={all_tables}, ")

        grants = sql_fetch_all(f"SELECT * FROM hive_metastore.{inventory_database}.grants")
        all_grants = {}
        for g_row in grants:
            grant = Grant(*g_row)
            if grant.table:
                all_grants[f"{grant.principal}.{grant.catalog}.{grant.database}.{grant.table}"] = grant.action_type
            else:
                all_grants[f"{grant.principal}.{grant.catalog}.{grant.database}"] = grant.action_type

        logger.debug(f"all grants={all_grants}, ")

        assert len(all_grants) >= 3, "must have at least three grants"
        assert all_grants[f"{ws_group_a.display_name}.{table_a}"] == "SELECT"
        assert all_grants[f"{ws_group_b.display_name}.{table_b}"] == "SELECT"
        assert all_grants[f"{ws_group_b.display_name}.{schema_b}"] == "MODIFY"

    finally:
        logger.info("deleting workbook")

        ws.workspace.delete(remote_ucx_notebook_location, recursive=True)

        logger.info("deleting job")

        ws.jobs.delete(created_job.job_id)

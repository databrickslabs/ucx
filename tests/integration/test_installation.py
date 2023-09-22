import logging
import os
import random
from io import BytesIO
from pathlib import Path

from databricks.sdk.service import compute, jobs
from databricks.sdk.service.iam import PermissionLevel
from databricks.sdk.service.workspace import ImportFormat

from databricks.labs.ucx.config import GroupsConfig, WorkspaceConfig
from databricks.labs.ucx.hive_metastore.grants import Grant
from databricks.labs.ucx.hive_metastore.tables import Table
from databricks.labs.ucx.install import WorkspaceInstaller

logger = logging.getLogger(__name__)


def test_assessment_job_with_no_inventory_database(
    request,
    ws,
    sql_exec,
    sql_fetch_all,
    make_cluster_policy,
    make_cluster_policy_permissions,
    make_ucx_group,
    make_job,
    make_job_permissions,
    make_random,
    make_schema,
    make_table,
):
    ws_group_a, acc_group_a = make_ucx_group()
    ws_group_b, acc_group_b = make_ucx_group()
    ws_group_c, acc_group_c = make_ucx_group()

    schema_a = make_schema()
    schema_b = make_schema()
    make_schema()
    table_a = make_table(schema=schema_a)
    table_b = make_table(schema=schema_b)

    sql_exec(f"GRANT USAGE ON SCHEMA default TO `{ws_group_a.display_name}`")
    sql_exec(f"GRANT USAGE ON SCHEMA default TO `{ws_group_b.display_name}`")
    sql_exec(f"GRANT SELECT ON TABLE {table_a} TO `{ws_group_a.display_name}`")
    sql_exec(f"GRANT SELECT ON TABLE {table_b} TO `{ws_group_b.display_name}`")
    sql_exec(f"GRANT MODIFY ON SCHEMA {schema_b} TO `{ws_group_b.display_name}`")

    cluster_policy = make_cluster_policy()
    make_cluster_policy_permissions(
        object_id=cluster_policy.policy_id,
        permission_level=random.choice([PermissionLevel.CAN_USE]),
        group_name=ws_group_a.display_name,
    )

    job = make_job()
    make_job_permissions(
        object_id=job.job_id,
        permission_level=random.choice(
            [PermissionLevel.CAN_VIEW, PermissionLevel.CAN_MANAGE_RUN, PermissionLevel.CAN_MANAGE]
        ),
        group_name=ws_group_b.display_name,
    )

    install = WorkspaceInstaller(ws, prefix=make_random(4), promtps=False)
    install._config = WorkspaceConfig(
        inventory_database=f"ucx_{make_random(4)}",
        instance_pool_id=os.environ["TEST_INSTANCE_POOL_ID"],
        groups=GroupsConfig(selected=[ws_group_a.display_name, ws_group_b.display_name, ws_group_c.display_name]),
        log_level="DEBUG",
    )
    install._write_config()
    install.run()

    def cleanup_created_resources():
        logger.debug(f"cleaning up install folder: {install._install_folder}")
        ws.workspace.delete(install._install_folder, recursive=True)

        for step, job_id in install._deployed_steps.items():
            logger.debug(f"cleaning up {step} job_id={job_id}")
            ws.jobs.delete(job_id)

        logger.debug(f"cleaning up inventory_database={install._config.inventory_database}")
        sql_exec(f"DROP SCHEMA IF EXISTS `{install._config.inventory_database}` CASCADE")

    request.addfinalizer(cleanup_created_resources)

    logger.debug(f'starting job: {ws.config.host}#job/{install._deployed_steps["assessment"]}')
    ws.jobs.run_now(install._deployed_steps["assessment"]).result()

    permissions = list(sql_fetch_all(f"SELECT * FROM hive_metastore.{install._config.inventory_database}.permissions"))
    tables = list(sql_fetch_all(f"SELECT * FROM hive_metastore.{install._config.inventory_database}.tables"))
    grants = list(sql_fetch_all(f"SELECT * FROM hive_metastore.{install._config.inventory_database}.grants"))

    assert len(permissions) > 0
    assert len(tables) >= 2
    assert len(grants) >= 5


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
    cpp_src = ws.permissions.get("cluster-policies", cluster_policy.policy_id)
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
    jp_src = ws.permissions.get("jobs", job.job_id)
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

        cp_dst = ws.permissions.get("cluster-policies", cluster_policy.policy_id)
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

        jp_dst = ws.permissions.get("jobs", job.job_id)
        job_dst_permissions = sorted(
            [_ for _ in jp_dst.access_control_list if _.group_name == ws_group_b.display_name],
            key=lambda p: p.group_name,
        )
        assert len(job_dst_permissions) == len(
            job_src_permissions
        ), f"Target permissions were not applied correctly for jobs/{job.job_id}"
        assert [t.all_permissions for t in job_dst_permissions] == [
            s.all_permissions for s in job_src_permissions
        ], f"Target permissions were not applied correctly for jobs/{job.job_id}"

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

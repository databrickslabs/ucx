import logging
import os
import random

from databricks.sdk.service.iam import PermissionLevel

from databricks.labs.ucx.config import GroupsConfig, WorkspaceConfig
from databricks.labs.ucx.hive_metastore.grants import Grant
from databricks.labs.ucx.hive_metastore.tables import Table
from databricks.labs.ucx.install import WorkspaceInstaller

logger = logging.getLogger(__name__)


def test_jobs_with_no_inventory_database(
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

    members_src_a = sorted([_.display for _ in ws.groups.get(id=ws_group_a.id).members])
    members_src_b = sorted([_.display for _ in ws.groups.get(id=ws_group_b.id).members])
    members_src_c = sorted([_.display for _ in ws.groups.get(id=ws_group_c.id).members])

    schema_a = make_schema()
    schema_b = make_schema()
    _ = make_schema()
    table_a = make_table(schema=schema_a)
    table_b = make_table(schema=schema_b)
    make_table(schema=schema_b, external=True)

    sql_exec(f"GRANT USAGE ON SCHEMA default TO `{ws_group_a.display_name}`")
    sql_exec(f"GRANT USAGE ON SCHEMA default TO `{ws_group_b.display_name}`")
    sql_exec(f"GRANT SELECT ON TABLE {table_a} TO `{ws_group_a.display_name}`")
    sql_exec(f"GRANT SELECT ON TABLE {table_b} TO `{ws_group_b.display_name}`")
    sql_exec(f"GRANT MODIFY ON SCHEMA {schema_b} TO `{ws_group_b.display_name}`")

    cluster_policy = make_cluster_policy()
    cpp_src = ws.permissions.get("cluster-policies", cluster_policy.policy_id)
    cluster_policy_src_permissions = sorted(
        [_ for _ in cpp_src.access_control_list if _.group_name == ws_group_a.display_name],
        key=lambda p: p.group_name,
    )
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
    jp_src = ws.permissions.get("jobs", job.job_id)
    job_src_permissions = sorted(
        [_ for _ in jp_src.access_control_list if _.group_name == ws_group_b.display_name],
        key=lambda p: p.group_name,
    )
    logger.info(f"cluster_policy={cluster_policy}, job={job}, ")

    inventory_database = f"ucx_{make_random(4)}"
    install = WorkspaceInstaller.run_for_config(
        ws,
        WorkspaceConfig(
            inventory_database=inventory_database,
            instance_pool_id=os.environ["TEST_INSTANCE_POOL_ID"],
            groups=GroupsConfig(selected=[ws_group_a.display_name, ws_group_b.display_name, ws_group_c.display_name]),
            log_level="DEBUG",
        ),
        prefix=make_random(4),
    )

    try:
        for step in ["assessment", "migrate-groups"]:
            logger.debug(f"starting {step} job: {ws.config.host}#job/{install._deployed_steps[step]}")
            ws.jobs.run_now(install._deployed_steps[step]).result()

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

        permissions = list(
            sql_fetch_all(f"SELECT * FROM hive_metastore.{install._config.inventory_database}.permissions")
        )
        tables = list(sql_fetch_all(f"SELECT * FROM hive_metastore.{install._config.inventory_database}.tables"))
        grants = list(sql_fetch_all(f"SELECT * FROM hive_metastore.{install._config.inventory_database}.grants"))

        assert len(permissions) > 0
        assert len(tables) >= 2
        assert len(grants) >= 5
    finally:
        logger.debug(f"cleaning up install folder: {install._install_folder}")
        ws.workspace.delete(install._install_folder, recursive=True)

        for step, job_id in install._deployed_steps.items():
            logger.debug(f"cleaning up {step} job_id={job_id}")
            ws.jobs.delete(job_id)

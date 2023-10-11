import logging
import random

import pytest
from databricks.sdk.errors import OperationFailed
from databricks.sdk.service.iam import PermissionLevel

from databricks.labs.ucx.config import GroupsConfig, WorkspaceConfig
from databricks.labs.ucx.hive_metastore.grants import GrantsCrawler
from databricks.labs.ucx.hive_metastore.tables import TablesCrawler
from databricks.labs.ucx.install import WorkspaceInstaller

logger = logging.getLogger(__name__)


def test_destroying_non_existing_schema_fails_with_correct_message(ws, sql_backend, env_or_skip, make_random):
    default_cluster_id = env_or_skip("TEST_DEFAULT_CLUSTER_ID")
    tacl_cluster_id = env_or_skip("TEST_LEGACY_TABLE_ACL_CLUSTER_ID")
    ws.clusters.ensure_cluster_is_running(default_cluster_id)
    ws.clusters.ensure_cluster_is_running(tacl_cluster_id)

    backup_group_prefix = "db-temp-"
    inventory_database = f"ucx_{make_random(4)}"
    install = WorkspaceInstaller.run_for_config(
        ws,
        WorkspaceConfig(
            inventory_database=inventory_database,
            groups=GroupsConfig(
                backup_group_prefix=backup_group_prefix,
                auto=True,
            ),
            log_level="DEBUG",
        ),
        prefix=make_random(4),
        override_clusters={
            "main": default_cluster_id,
            "tacl": tacl_cluster_id,
        },
    )

    with pytest.raises(OperationFailed) as failure:
        install.run_workflow("destroy-schema")

    assert "cannot be found" in str(failure.value)


def test_logs_are_available(ws, sql_backend, env_or_skip, make_random):
    default_cluster_id = env_or_skip("TEST_DEFAULT_CLUSTER_ID")
    ws.clusters.ensure_cluster_is_running(default_cluster_id)

    install = WorkspaceInstaller.run_for_config(
        ws,
        WorkspaceConfig(
            inventory_database=f"ucx_{make_random(4)}",
            instance_pool_id=env_or_skip("TEST_INSTANCE_POOL_ID"),
            groups=GroupsConfig(auto=True),
            log_level="INFO",
        ),
        prefix=make_random(4),
        override_clusters={
            "main": default_cluster_id,
        },
    )

    with pytest.raises(OperationFailed):
        install.run_workflow("destroy-schema")
        assert True

    workflow_run_logs = list(ws.workspace.list(f"{install._install_folder}/logs"))
    assert len(workflow_run_logs) == 1


def test_jobs_with_no_inventory_database(
    ws,
    sql_backend,
    make_cluster_policy,
    make_cluster_policy_permissions,
    make_ucx_group,
    make_job,
    make_job_permissions,
    make_random,
    make_schema,
    make_table,
    env_or_skip,
):
    default_cluster_id = env_or_skip("TEST_DEFAULT_CLUSTER_ID")
    tacl_cluster_id = env_or_skip("TEST_LEGACY_TABLE_ACL_CLUSTER_ID")
    ws.clusters.ensure_cluster_is_running(default_cluster_id)
    ws.clusters.ensure_cluster_is_running(tacl_cluster_id)

    ws_group_a, acc_group_a = make_ucx_group()
    ws_group_b, acc_group_b = make_ucx_group()
    ws_group_c, acc_group_c = make_ucx_group()

    members_src_a = sorted([_.display for _ in ws.groups.get(id=ws_group_a.id).members])
    members_src_b = sorted([_.display for _ in ws.groups.get(id=ws_group_b.id).members])
    members_src_c = sorted([_.display for _ in ws.groups.get(id=ws_group_c.id).members])

    schema_a = make_schema()
    schema_b = make_schema()
    _ = make_schema()
    table_a = make_table(schema_name=schema_a.name)
    table_b = make_table(schema_name=schema_b.name)
    make_table(schema_name=schema_b.name, external=True)

    sql_backend.execute(f"GRANT USAGE ON SCHEMA default TO `{ws_group_a.display_name}`")
    sql_backend.execute(f"GRANT USAGE ON SCHEMA default TO `{ws_group_b.display_name}`")
    sql_backend.execute(f"GRANT SELECT ON TABLE {table_a.full_name} TO `{ws_group_a.display_name}`")
    sql_backend.execute(f"GRANT SELECT ON TABLE {table_b.full_name} TO `{ws_group_b.display_name}`")
    sql_backend.execute(f"GRANT MODIFY ON SCHEMA {schema_b.full_name} TO `{ws_group_b.display_name}`")

    cluster_policy = make_cluster_policy()
    make_cluster_policy_permissions(
        object_id=cluster_policy.policy_id,
        permission_level=PermissionLevel.CAN_USE,
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

    backup_group_prefix = "db-temp-"
    inventory_database = f"ucx_{make_random(4)}"
    install = WorkspaceInstaller.run_for_config(
        ws,
        WorkspaceConfig(
            inventory_database=inventory_database,
            instance_pool_id=env_or_skip("TEST_INSTANCE_POOL_ID"),
            groups=GroupsConfig(
                selected=[ws_group_a.display_name, ws_group_b.display_name, ws_group_c.display_name],
                backup_group_prefix=backup_group_prefix,
            ),
            log_level="DEBUG",
        ),
        prefix=make_random(4),
        override_clusters={
            "main": default_cluster_id,
            "tacl": tacl_cluster_id,
        },
    )

    try:
        for step in ["assessment", "migrate-groups", "migrate-groups-cleanup"]:
            install.run_workflow(step)

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

        logger.info("validating clean up of backup groups")

        backup_ws_group_a_iter = ws.groups.list(
            filter=f"displayName eq { backup_group_prefix + ws_group_a.display_name}"
        )
        assert all(
            False for _ in backup_ws_group_a_iter
        ), f"Backup group {backup_group_prefix + ws_group_a.display_name} was not deleted"

        backup_ws_group_b_iter = ws.groups.list(
            filter=f"displayName eq { backup_group_prefix + ws_group_b.display_name}"
        )
        assert all(
            False for _ in backup_ws_group_b_iter
        ), f"Backup group {backup_group_prefix + ws_group_b.display_name} was not deleted"

        backup_ws_group_c_iter = ws.groups.list(
            filter=f"displayName eq { backup_group_prefix + ws_group_c.display_name}"
        )
        assert all(
            False for _ in backup_ws_group_c_iter
        ), f"Backup group {backup_group_prefix + ws_group_c.display_name} was not deleted"

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

        tables_crawler = TablesCrawler(sql_backend, install._config.inventory_database)
        grants_crawler = GrantsCrawler(tables_crawler)

        table_a_grants = grants_crawler.for_table_info(table_a)
        assert {"SELECT"} == table_a_grants.get(ws_group_a.display_name)

        table_b_grants = grants_crawler.for_table_info(table_b)
        assert {"SELECT"} == table_b_grants.get(ws_group_b.display_name)

        schema_b_grants = grants_crawler.for_schema_info(schema_b)
        assert {"MODIFY"} == schema_b_grants.get(ws_group_b.display_name)

        all_grants = grants_crawler.snapshot()
        logger.debug(f"all grants={all_grants}, ")

        permissions = list(
            sql_backend.fetch(f"SELECT * FROM hive_metastore.{install._config.inventory_database}.permissions")
        )
        tables = list(sql_backend.fetch(f"SELECT * FROM hive_metastore.{install._config.inventory_database}.tables"))
        grants = list(sql_backend.fetch(f"SELECT * FROM hive_metastore.{install._config.inventory_database}.grants"))

        assert len(permissions) > 0
        assert len(tables) >= 2
        assert len(grants) >= 5
    finally:
        logger.debug(f"cleaning up install folder: {install._install_folder}")
        ws.workspace.delete(install._install_folder, recursive=True)

        for step, job_id in install._deployed_steps.items():
            logger.debug(f"cleaning up {step} job_id={job_id}")
            ws.jobs.delete(job_id)

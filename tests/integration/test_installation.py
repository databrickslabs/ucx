import logging
from datetime import timedelta

import pytest
from databricks.sdk.errors import NotFound, OperationFailed
from databricks.sdk.retries import retried
from databricks.sdk.service.catalog import SchemaInfo
from databricks.sdk.service.iam import PermissionLevel

from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.framework.parallel import Threads
from databricks.labs.ucx.hive_metastore.grants import GrantsCrawler
from databricks.labs.ucx.hive_metastore.tables import TablesCrawler
from databricks.labs.ucx.install import WorkspaceInstaller
from databricks.labs.ucx.workspace_access.generic import GenericPermissionsSupport
from databricks.labs.ucx.workspace_access.groups import GroupManager

logger = logging.getLogger(__name__)


def test_job_failure_propagates_correct_error_message_and_logs(ws, sql_backend, env_or_skip, make_random):
    default_cluster_id = env_or_skip("TEST_DEFAULT_CLUSTER_ID")
    tacl_cluster_id = env_or_skip("TEST_LEGACY_TABLE_ACL_CLUSTER_ID")
    ws.clusters.ensure_cluster_is_running(default_cluster_id)
    ws.clusters.ensure_cluster_is_running(tacl_cluster_id)

    backup_group_prefix = "db-temp-"
    inventory_database = f"ucx_{make_random(4)}"
    install = WorkspaceInstaller.run_for_config(
        ws,
        WorkspaceConfig(
            inventory_database=inventory_database, log_level="DEBUG", renamed_group_prefix=backup_group_prefix
        ),
        sql_backend=sql_backend,
        prefix=make_random(4),
        override_clusters={
            "main": default_cluster_id,
            "tacl": tacl_cluster_id,
        },
    )

    sql_backend.execute(f"DROP SCHEMA {inventory_database} CASCADE")

    with pytest.raises(OperationFailed) as failure:
        install.run_workflow("099-destroy-schema")

    assert "cannot be found" in str(failure.value)

    workflow_run_logs = list(ws.workspace.list(f"{install._install_folder}/logs"))
    assert len(workflow_run_logs) == 1


@retried(on=[NotFound], timeout=timedelta(minutes=15))
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
    inventory_database = f"ucx_{make_random(4)}"
    default_cluster_id = env_or_skip("TEST_DEFAULT_CLUSTER_ID")
    tacl_cluster_id = env_or_skip("TEST_LEGACY_TABLE_ACL_CLUSTER_ID")
    logger.info(f"ensuring default ({default_cluster_id}) and tacl ({tacl_cluster_id}) clusters are running")
    ws.clusters.ensure_cluster_is_running(default_cluster_id)
    ws.clusters.ensure_cluster_is_running(tacl_cluster_id)

    ws_group_a, acc_group_a = make_ucx_group()
    ws_group_b, acc_group_b = make_ucx_group()
    ws_group_c, acc_group_c = make_ucx_group()

    schema_a = make_schema()
    schema_b = make_schema()
    schema_default = SchemaInfo(catalog_name="hive_metastore", name="default", full_name="hive_metastore.default")
    _ = make_schema()
    table_a = make_table(schema_name=schema_a.name)
    table_b = make_table(schema_name=schema_b.name)
    table_c = make_table(schema_name=schema_b.name, external=True)

    sql_backend.execute(f"GRANT USAGE ON SCHEMA {schema_a.name} TO `{ws_group_a.display_name}`")
    sql_backend.execute(f"ALTER SCHEMA {schema_a.name} OWNER TO `{ws_group_a.display_name}`")
    sql_backend.execute(f"GRANT ALL PRIVILEGES ON SCHEMA {schema_b.name} TO `{ws_group_b.display_name}`")
    sql_backend.execute(
        f"GRANT USAGE, SELECT, MODIFY, CREATE, READ_METADATA, CREATE_NAMED_FUNCTION ON SCHEMA default TO "
        f"`{ws_group_c.display_name}`"
    )
    sql_backend.execute(f"GRANT SELECT ON TABLE {table_a.full_name} TO `{ws_group_a.display_name}`")
    sql_backend.execute(
        f"GRANT SELECT, MODIFY, READ_METADATA ON TABLE {table_b.full_name} TO `{ws_group_b.display_name}`"
    )
    sql_backend.execute(f"ALTER TABLE {table_b.full_name} OWNER TO `{ws_group_b.display_name}`")
    sql_backend.execute(f"GRANT SELECT, MODIFY ON TABLE {table_c.full_name} TO `{ws_group_c.display_name}`")

    # Grant permission to "admins" so that we can read permissions from the objects for validation.
    # Assuming that the user running the tests is an admin.
    # This is needed because we changed the owner of the objects to a group, and we are not part of that group.
    sql_backend.execute(f"GRANT READ_METADATA ON SCHEMA {schema_a.name} TO `admins`")
    sql_backend.execute(f"GRANT READ_METADATA ON TABLE {table_b.full_name} TO `admins`")

    tables_crawler = TablesCrawler(sql_backend, inventory_database)
    grants_crawler = GrantsCrawler(tables_crawler)
    src_table_a_grants = grants_crawler.for_table_info(table_a)
    src_table_b_grants = grants_crawler.for_table_info(table_b)
    src_table_c_grants = grants_crawler.for_table_info(table_c)
    src_schema_a_grants = grants_crawler.for_schema_info(schema_a)
    src_schema_b_grants = grants_crawler.for_schema_info(schema_b)
    src_schema_default_grants = grants_crawler.for_schema_info(schema_default)

    cluster_policy = make_cluster_policy()
    make_cluster_policy_permissions(
        object_id=cluster_policy.policy_id,
        permission_level=PermissionLevel.CAN_USE,
        group_name=ws_group_a.display_name,
    )

    job = make_job()
    for permission_level in [
        PermissionLevel.CAN_VIEW,
        PermissionLevel.CAN_MANAGE_RUN,
        PermissionLevel.CAN_MANAGE
        # group cannot be an owner of a job or pipeline
    ]:
        make_job_permissions(
            object_id=job.job_id,
            permission_level=permission_level,
            group_name=ws_group_b.display_name,
        )

    generic_permissions = GenericPermissionsSupport(ws, [])
    src_policy_permissions = generic_permissions.load_as_dict("cluster-policies", cluster_policy.policy_id)
    src_job_permissions = generic_permissions.load_as_dict("jobs", job.job_id)

    backup_group_prefix = "db-temp-"
    install = WorkspaceInstaller.run_for_config(
        ws,
        WorkspaceConfig(
            inventory_database=inventory_database,
            instance_pool_id=env_or_skip("TEST_INSTANCE_POOL_ID"),
            include_group_names=[ws_group_a.display_name, ws_group_b.display_name, ws_group_c.display_name],
            renamed_group_prefix=backup_group_prefix,
            log_level="DEBUG",
        ),
        sql_backend=sql_backend,
        prefix=make_random(4),
        override_clusters={
            "main": default_cluster_id,
            "tacl": tacl_cluster_id,
        },
    )

    try:
        required_workflows = ["assessment", "migrate-groups", "remove-workspace-local-backup-groups"]
        for step in required_workflows:
            install.run_workflow(step)

        @retried(on=[AssertionError], timeout=timedelta(minutes=2))
        def validate_groups():
            group_manager = GroupManager(sql_backend, ws, inventory_database)
            acc_membership = group_manager.get_workspace_membership("Group")

            logger.info("validating replaced account groups")
            assert acc_group_a.display_name in acc_membership, f"{acc_group_a.display_name} not found in workspace"
            assert acc_group_b.display_name in acc_membership, f"{acc_group_b.display_name} not found in workspace"
            assert acc_group_c.display_name in acc_membership, f"{acc_group_c.display_name} not found in workspace"

            logger.info("validating replaced group members")
            for g in (ws_group_a, ws_group_b, ws_group_c):
                for m in g.members:
                    assert m.display in acc_membership[g.display_name], f"{m.display} not in {g.display_name}"

            return True

        @retried(on=[AssertionError], timeout=timedelta(minutes=2))
        def validate_permissions():
            logger.info("validating permissions")
            policy_permissions = generic_permissions.load_as_dict("cluster-policies", cluster_policy.policy_id)
            job_permissions = generic_permissions.load_as_dict("jobs", job.job_id)
            assert src_policy_permissions == policy_permissions
            assert src_job_permissions == job_permissions

            return True

        @retried(on=[AssertionError], timeout=timedelta(minutes=2))
        def validate_tacl():
            logger.info("validating tacl")
            table_a_grants = grants_crawler.for_table_info(table_a)
            table_b_grants = grants_crawler.for_table_info(table_b)
            table_c_grants = grants_crawler.for_table_info(table_c)
            schema_a_grants = grants_crawler.for_schema_info(schema_a)
            schema_b_grants = grants_crawler.for_schema_info(schema_b)
            schema_default_grants = grants_crawler.for_schema_info(schema_default)
            all_grants = grants_crawler.snapshot()

            assert table_a_grants == src_table_a_grants
            assert table_b_grants == src_table_b_grants
            assert table_c_grants == src_table_c_grants
            assert schema_a_grants == src_schema_a_grants
            assert schema_b_grants == src_schema_b_grants
            assert schema_default_grants[ws_group_c.display_name] == src_schema_default_grants[ws_group_c.display_name]
            assert len(all_grants) >= 6

            return True

        _, errors = Threads.gather("validating results", [validate_tacl, validate_permissions, validate_groups])
        assert len(errors) == 0
    finally:
        logger.debug(f"cleaning up install folder: {install._install_folder}")
        ws.workspace.delete(install._install_folder, recursive=True)

        for step, job_id in install._state.jobs.items():
            logger.debug(f"cleaning up {step} job_id={job_id}")
            ws.jobs.delete(job_id)

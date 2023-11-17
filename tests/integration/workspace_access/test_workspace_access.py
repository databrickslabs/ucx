import logging
import random
from datetime import timedelta

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service import workspace
from databricks.sdk.service.iam import PermissionLevel

from databricks.labs.ucx.config import ConnectConfig, WorkspaceConfig
from databricks.labs.ucx.workspace_access import GroupMigrationToolkit

from ..conftest import get_workspace_membership

logger = logging.getLogger(__name__)


@retried(on=[NotFound, AssertionError], timeout=timedelta(minutes=15))
def test_workspace_access_e2e(
    ws: WorkspaceClient,
    sql_backend,
    inventory_schema,
    make_schema,
    make_table,
    make_ucx_group,
    make_instance_pool,
    make_instance_pool_permissions,
    make_cluster,
    make_cluster_permissions,
    make_cluster_policy,
    make_cluster_policy_permissions,
    make_model,
    make_registered_model_permissions,
    make_experiment,
    make_experiment_permissions,
    make_job,
    make_job_permissions,
    make_notebook,
    make_notebook_permissions,
    make_directory,
    make_directory_permissions,
    make_pipeline,
    make_pipeline_permissions,
    make_secret_scope,
    make_secret_scope_acl,
    make_authorization_permissions,
    make_warehouse,
    make_warehouse_permissions,
    env_or_skip,
):
    ws_group, acc_group = make_ucx_group()

    schema_a = make_schema()
    schema_b = make_schema()
    _ = make_schema()
    table_a = make_table(schema_name=schema_a.name)
    table_b = make_table(schema_name=schema_b.name)
    make_table(schema_name=schema_b.name, external=True)

    sql_backend.execute(f"GRANT USAGE ON SCHEMA default TO `{ws_group.display_name}`")
    sql_backend.execute(f"GRANT SELECT ON TABLE {table_a.full_name} TO `{ws_group.display_name}`")
    sql_backend.execute(f"GRANT MODIFY ON TABLE {table_b.full_name} TO `{ws_group.display_name}`")

    to_verify = set()

    pool = make_instance_pool()
    make_instance_pool_permissions(
        object_id=pool.instance_pool_id,
        permission_level=random.choice([PermissionLevel.CAN_ATTACH_TO, PermissionLevel.CAN_MANAGE]),
        group_name=ws_group.display_name,
    )
    to_verify.add(("instance-pools", pool.instance_pool_id))

    cluster = make_cluster(instance_pool_id=env_or_skip("TEST_INSTANCE_POOL_ID"), single_node=True)
    make_cluster_permissions(
        object_id=cluster.cluster_id,
        permission_level=random.choice(
            [PermissionLevel.CAN_ATTACH_TO, PermissionLevel.CAN_MANAGE, PermissionLevel.CAN_RESTART]
        ),
        group_name=ws_group.display_name,
    )
    to_verify.add(("clusters", cluster.cluster_id))

    cluster_policy = make_cluster_policy()
    make_cluster_policy_permissions(
        object_id=cluster_policy.policy_id,
        permission_level=random.choice([PermissionLevel.CAN_USE]),
        group_name=ws_group.display_name,
    )
    to_verify.add(("cluster-policies", cluster_policy.policy_id))

    model = make_model()
    make_registered_model_permissions(
        object_id=model.id,
        permission_level=random.choice(
            [
                PermissionLevel.CAN_READ,
                PermissionLevel.CAN_MANAGE,
                PermissionLevel.CAN_MANAGE_PRODUCTION_VERSIONS,
                PermissionLevel.CAN_MANAGE_STAGING_VERSIONS,
            ]
        ),
        group_name=ws_group.display_name,
    )
    to_verify.add(("registered-models", model.id))

    experiment = make_experiment()
    make_experiment_permissions(
        object_id=experiment.experiment_id,
        permission_level=random.choice(
            [PermissionLevel.CAN_MANAGE, PermissionLevel.CAN_READ, PermissionLevel.CAN_EDIT]
        ),
        group_name=ws_group.display_name,
    )
    to_verify.add(("experiments", experiment.experiment_id))

    directory = make_directory()
    make_directory_permissions(
        object_id=directory,
        permission_level=random.choice(
            [PermissionLevel.CAN_READ, PermissionLevel.CAN_MANAGE, PermissionLevel.CAN_EDIT, PermissionLevel.CAN_RUN]
        ),
        group_name=ws_group.display_name,
    )
    to_verify.add(("directories", ws.workspace.get_status(directory).object_id))

    notebook = make_notebook(path=f"{directory}/sample.py")
    make_notebook_permissions(
        object_id=notebook,
        permission_level=random.choice(
            [PermissionLevel.CAN_READ, PermissionLevel.CAN_MANAGE, PermissionLevel.CAN_EDIT, PermissionLevel.CAN_RUN]
        ),
        group_name=ws_group.display_name,
    )
    to_verify.add(("notebooks", ws.workspace.get_status(notebook).object_id))

    job = make_job()
    make_job_permissions(
        object_id=job.job_id,
        permission_level=random.choice(
            [PermissionLevel.CAN_VIEW, PermissionLevel.CAN_MANAGE_RUN, PermissionLevel.CAN_MANAGE]
        ),
        group_name=ws_group.display_name,
    )
    to_verify.add(("jobs", job.job_id))

    pipeline = make_pipeline()
    make_pipeline_permissions(
        object_id=pipeline.pipeline_id,
        permission_level=random.choice([PermissionLevel.CAN_VIEW, PermissionLevel.CAN_RUN, PermissionLevel.CAN_MANAGE]),
        group_name=ws_group.display_name,
    )
    to_verify.add(("pipelines", pipeline.pipeline_id))

    scope = make_secret_scope()
    make_secret_scope_acl(scope=scope, principal=ws_group.display_name, permission=workspace.AclPermission.WRITE)
    to_verify.add(("secrets", scope))

    make_authorization_permissions(
        object_id="tokens",
        permission_level=PermissionLevel.CAN_USE,
        group_name=ws_group.display_name,
    )
    to_verify.add(("authorization", "tokens"))

    warehouse = make_warehouse()
    make_warehouse_permissions(
        object_id=warehouse.id,
        permission_level=random.choice([PermissionLevel.CAN_USE, PermissionLevel.CAN_MANAGE]),
        group_name=ws_group.display_name,
    )
    to_verify.add(("sql/warehouses", warehouse.id))

    config = WorkspaceConfig(
        connect=ConnectConfig.from_databricks_config(ws.config),
        inventory_database=inventory_schema,
        workspace_start_path=directory,
        log_level="DEBUG",
        num_threads=8,
        include_group_names=[ws_group.display_name],
    )

    warehouse_id = env_or_skip("TEST_DEFAULT_WAREHOUSE_ID")
    toolkit = GroupMigrationToolkit(config, warehouse_id=warehouse_id)

    logger.debug("Verifying that the groups were created - done")

    toolkit.cleanup_inventory_table()

    toolkit.inventorize_permissions()

    toolkit.apply_permissions_to_backup_groups()

    toolkit.replace_workspace_groups_with_account_groups()

    workspace_acc_membership = get_workspace_membership(ws, "Group")
    assert acc_group.display_name in workspace_acc_membership

    toolkit.apply_permissions_to_account_groups()

    toolkit.verify_permissions_on_account_groups(to_verify)

    toolkit.delete_backup_groups()

    workspace_membership = get_workspace_membership(ws)
    assert f"db-temp-{ws_group.display_name}" not in workspace_membership

    toolkit.cleanup_inventory_table()

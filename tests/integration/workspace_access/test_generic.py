import json
from datetime import timedelta

import pytest
from databricks.sdk import AccountClient, WorkspaceClient
from databricks.sdk.errors import BadRequest, NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service import iam
from databricks.sdk.service.iam import (
    AccessControlRequest,
    PermissionLevel,
    WorkspacePermission,
)

from databricks.labs.ucx.workspace_access.base import Permissions
from databricks.labs.ucx.workspace_access.generic import (
    GenericPermissionsSupport,
    Listing,
    WorkspaceListing,
    experiments_listing,
    feature_store_listing,
    feature_tables_root_page,
    models_listing,
    models_root_page,
    tokens_and_passwords,
)
from databricks.labs.ucx.workspace_access.groups import MigratedGroup, MigrationState
from databricks.labs.ucx.workspace_access.manager import PermissionManager

from . import apply_tasks


@pytest.mark.parametrize("use_permission_migration_api", [True, False])
@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_instance_pools(
    acc: AccountClient,
    ws: WorkspaceClient,
    permission_manager: PermissionManager,
    make_group,
    make_acc_group,
    make_instance_pool,
    make_instance_pool_permissions,
    use_permission_migration_api: bool,
):
    group_a = make_group()
    group_b = make_acc_group()
    acc.workspace_assignment.update(ws.get_workspace_id(), group_b.id, [WorkspacePermission.USER])
    pool = make_instance_pool()
    make_instance_pool_permissions(
        object_id=pool.instance_pool_id,
        permission_level=PermissionLevel.CAN_MANAGE,
        group_name=group_a.display_name,
    )

    generic_permissions = GenericPermissionsSupport(
        ws,
        [
            Listing(ws.instance_pools.list, "instance_pool_id", "instance-pools"),
        ],
    )
    before = generic_permissions.load_as_dict("instance-pools", pool.instance_pool_id)
    assert before[group_a.display_name] == PermissionLevel.CAN_MANAGE

    migrated_groups: list[MigratedGroup] = [MigratedGroup.partial_info(group_a, group_b)]

    if use_permission_migration_api:
        permission_manager.apply_group_permissions_private_preview_api(MigrationState(migrated_groups))
    else:
        apply_tasks(generic_permissions, migrated_groups)

    after = generic_permissions.load_as_dict("instance-pools", pool.instance_pool_id)
    assert after[group_b.display_name] == PermissionLevel.CAN_MANAGE


@pytest.mark.parametrize("use_permission_migration_api", [True, False])
@retried(on=[BadRequest], timeout=timedelta(minutes=3))
def test_clusters(
    acc: AccountClient,
    ws: WorkspaceClient,
    permission_manager: PermissionManager,
    make_group,
    make_acc_group,
    make_cluster,
    make_cluster_permissions,
    env_or_skip,
    use_permission_migration_api: bool,
):
    group_a = make_group()
    group_b = make_acc_group()
    acc.workspace_assignment.update(ws.get_workspace_id(), group_b.id, [WorkspacePermission.USER])
    cluster = make_cluster(instance_pool_id=env_or_skip("TEST_INSTANCE_POOL_ID"), single_node=True)
    make_cluster_permissions(
        object_id=cluster.cluster_id,
        permission_level=PermissionLevel.CAN_MANAGE,
        group_name=group_a.display_name,
    )

    generic_permissions = GenericPermissionsSupport(
        ws,
        [
            Listing(ws.clusters.list, "cluster_id", "clusters"),
        ],
    )
    before = generic_permissions.load_as_dict("clusters", cluster.cluster_id)
    assert before[group_a.display_name] == PermissionLevel.CAN_MANAGE

    migrated_groups: list[MigratedGroup] = [MigratedGroup.partial_info(group_a, group_b)]

    if use_permission_migration_api:
        permission_manager.apply_group_permissions_private_preview_api(MigrationState(migrated_groups))
    else:
        apply_tasks(generic_permissions, migrated_groups)

    after = generic_permissions.load_as_dict("clusters", cluster.cluster_id)
    assert after[group_b.display_name] == PermissionLevel.CAN_MANAGE


@pytest.mark.parametrize("use_permission_migration_api", [True, False])
@retried(on=[BadRequest], timeout=timedelta(minutes=3))
def test_jobs(
    acc: AccountClient,
    ws: WorkspaceClient,
    permission_manager: PermissionManager,
    make_group,
    make_acc_group,
    make_job,
    make_job_permissions,
    use_permission_migration_api: bool,
):
    group_a = make_group()
    group_b = make_acc_group()
    acc.workspace_assignment.update(ws.get_workspace_id(), group_b.id, [WorkspacePermission.USER])
    job = make_job()
    make_job_permissions(
        object_id=job.job_id,
        permission_level=PermissionLevel.CAN_MANAGE,
        group_name=group_a.display_name,
    )

    generic_permissions = GenericPermissionsSupport(
        ws,
        [
            Listing(ws.jobs.list, "job_id", "jobs"),
        ],
    )
    before = generic_permissions.load_as_dict("jobs", job.job_id)
    assert before[group_a.display_name] == PermissionLevel.CAN_MANAGE

    migrated_groups: list[MigratedGroup] = [MigratedGroup.partial_info(group_a, group_b)]

    if use_permission_migration_api:
        permission_manager.apply_group_permissions_private_preview_api(MigrationState(migrated_groups))
    else:
        apply_tasks(generic_permissions, migrated_groups)

    after = generic_permissions.load_as_dict("jobs", job.job_id)
    assert after[group_b.display_name] == PermissionLevel.CAN_MANAGE


@pytest.mark.parametrize("use_permission_migration_api", [True, False])
@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_pipelines(
    acc: AccountClient,
    ws: WorkspaceClient,
    permission_manager: PermissionManager,
    make_group,
    make_acc_group,
    make_pipeline,
    make_pipeline_permissions,
    use_permission_migration_api: bool,
):
    group_a = make_group()
    group_b = make_acc_group()
    acc.workspace_assignment.update(ws.get_workspace_id(), group_b.id, [WorkspacePermission.USER])
    pipeline = make_pipeline()
    make_pipeline_permissions(
        object_id=pipeline.pipeline_id,
        permission_level=PermissionLevel.CAN_MANAGE,
        group_name=group_a.display_name,
    )

    generic_permissions = GenericPermissionsSupport(
        ws,
        [
            Listing(ws.pipelines.list_pipelines, "pipeline_id", "pipelines"),
        ],
    )
    before = generic_permissions.load_as_dict("pipelines", pipeline.pipeline_id)
    assert before[group_a.display_name] == PermissionLevel.CAN_MANAGE

    migrated_groups: list[MigratedGroup] = [MigratedGroup.partial_info(group_a, group_b)]

    if use_permission_migration_api:
        permission_manager.apply_group_permissions_private_preview_api(MigrationState(migrated_groups))
    else:
        apply_tasks(generic_permissions, migrated_groups)

    after = generic_permissions.load_as_dict("pipelines", pipeline.pipeline_id)
    assert after[group_b.display_name] == PermissionLevel.CAN_MANAGE


@pytest.mark.parametrize("use_permission_migration_api", [True, False])
@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_cluster_policies(
    acc: AccountClient,
    ws: WorkspaceClient,
    permission_manager: PermissionManager,
    make_group,
    make_acc_group,
    make_cluster_policy,
    make_cluster_policy_permissions,
    use_permission_migration_api: bool,
):
    group_a = make_group()
    group_b = make_acc_group()
    acc.workspace_assignment.update(ws.get_workspace_id(), group_b.id, [WorkspacePermission.USER])
    cluster_policy = make_cluster_policy()
    make_cluster_policy_permissions(
        object_id=cluster_policy.policy_id,
        permission_level=PermissionLevel.CAN_USE,
        group_name=group_a.display_name,
    )

    generic_permissions = GenericPermissionsSupport(
        ws,
        [
            Listing(ws.cluster_policies.list, "policy_id", "cluster-policies"),
        ],
    )
    before = generic_permissions.load_as_dict("cluster-policies", cluster_policy.policy_id)
    assert before[group_a.display_name] == PermissionLevel.CAN_USE

    migrated_groups: list[MigratedGroup] = [MigratedGroup.partial_info(group_a, group_b)]

    if use_permission_migration_api:
        permission_manager.apply_group_permissions_private_preview_api(MigrationState(migrated_groups))
    else:
        apply_tasks(generic_permissions, migrated_groups)

    after = generic_permissions.load_as_dict("cluster-policies", cluster_policy.policy_id)
    assert after[group_b.display_name] == PermissionLevel.CAN_USE


@pytest.mark.parametrize("use_permission_migration_api", [True, False])
@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_warehouses(
    acc: AccountClient,
    ws: WorkspaceClient,
    permission_manager: PermissionManager,
    make_group,
    make_acc_group,
    make_warehouse,
    make_warehouse_permissions,
    use_permission_migration_api: bool,
):
    group_a = make_group()
    group_b = make_acc_group()
    acc.workspace_assignment.update(ws.get_workspace_id(), group_b.id, [WorkspacePermission.USER])
    warehouse = make_warehouse()
    make_warehouse_permissions(
        object_id=warehouse.id,
        permission_level=PermissionLevel.CAN_MANAGE,
        group_name=group_a.display_name,
    )

    generic_permissions = GenericPermissionsSupport(
        ws,
        [
            Listing(ws.warehouses.list, "id", "sql/warehouses"),
        ],
    )
    before = generic_permissions.load_as_dict("sql/warehouses", warehouse.id)
    assert before[group_a.display_name] == PermissionLevel.CAN_MANAGE

    migrated_groups: list[MigratedGroup] = [MigratedGroup.partial_info(group_a, group_b)]

    if use_permission_migration_api:
        permission_manager.apply_group_permissions_private_preview_api(MigrationState(migrated_groups))
    else:
        apply_tasks(generic_permissions, migrated_groups)

    after = generic_permissions.load_as_dict("sql/warehouses", warehouse.id)
    assert after[group_b.display_name] == PermissionLevel.CAN_MANAGE


@pytest.mark.parametrize("use_permission_migration_api", [True, False])
@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_models(
    ws: WorkspaceClient,
    acc: AccountClient,
    permission_manager: PermissionManager,
    make_group,
    make_acc_group,
    make_model,
    make_registered_model_permissions,  # pylint: disable=invalid-name
    use_permission_migration_api: bool,
):
    group_a = make_group()
    group_b = make_acc_group()
    acc.workspace_assignment.update(ws.get_workspace_id(), group_b.id, [WorkspacePermission.USER])
    model = make_model()
    make_registered_model_permissions(
        object_id=model.id,
        permission_level=PermissionLevel.CAN_MANAGE,
        group_name=group_a.display_name,
    )

    generic_permissions = GenericPermissionsSupport(
        ws,
        [
            Listing(models_listing(ws, 10), "id", "registered-models"),
        ],
    )
    before = generic_permissions.load_as_dict("registered-models", model.id)
    assert before[group_a.display_name] == PermissionLevel.CAN_MANAGE

    migrated_groups: list[MigratedGroup] = [MigratedGroup.partial_info(group_a, group_b)]

    if use_permission_migration_api:
        permission_manager.apply_group_permissions_private_preview_api(MigrationState(migrated_groups))
    else:
        apply_tasks(generic_permissions, migrated_groups)

    after = generic_permissions.load_as_dict("registered-models", model.id)
    assert after[group_b.display_name] == PermissionLevel.CAN_MANAGE


@pytest.mark.parametrize("use_permission_migration_api", [True, False])
@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_experiments(
    acc: AccountClient,
    ws: WorkspaceClient,
    permission_manager: PermissionManager,
    make_group,
    make_acc_group,
    make_experiment,
    make_experiment_permissions,
    use_permission_migration_api: bool,
):
    group_a = make_group()
    group_b = make_acc_group()
    acc.workspace_assignment.update(ws.get_workspace_id(), group_b.id, [WorkspacePermission.USER])
    experiment = make_experiment()
    make_experiment_permissions(
        object_id=experiment.experiment_id,
        permission_level=PermissionLevel.CAN_MANAGE,
        group_name=group_a.display_name,
    )

    generic_permissions = GenericPermissionsSupport(
        ws, [Listing(experiments_listing(ws), "experiment_id", "experiments")]
    )
    before = generic_permissions.load_as_dict("experiments", experiment.experiment_id)
    assert before[group_a.display_name] == PermissionLevel.CAN_MANAGE

    migrated_groups: list[MigratedGroup] = [MigratedGroup.partial_info(group_a, group_b)]

    if use_permission_migration_api:
        permission_manager.apply_group_permissions_private_preview_api(MigrationState(migrated_groups))
    else:
        apply_tasks(generic_permissions, migrated_groups)

    after = generic_permissions.load_as_dict("experiments", experiment.experiment_id)
    assert after[group_b.display_name] == PermissionLevel.CAN_MANAGE


@pytest.mark.parametrize("use_permission_migration_api", [True, False])
@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_directories(
    ws: WorkspaceClient,
    sql_backend,
    inventory_schema,
    permission_manager: PermissionManager,
    make_group,
    make_acc_group,
    make_directory,
    make_directory_permissions,
    use_permission_migration_api: bool,
):
    group_a = make_group()
    group_b = make_acc_group()
    acc = AccountClient(host=ws.config.environment.deployment_url('accounts'))
    acc.workspace_assignment.update(ws.get_workspace_id(), group_b.id, [WorkspacePermission.USER])
    directory = make_directory()
    make_directory_permissions(
        object_id=directory,
        permission_level=PermissionLevel.CAN_MANAGE,
        group_name=group_a.display_name,
    )

    generic_permissions = GenericPermissionsSupport(
        ws,
        [
            WorkspaceListing(
                ws,
                sql_backend=sql_backend,
                inventory_database=inventory_schema,
                num_threads=10,
                start_path="/",
            )
        ],
    )
    object_id = ws.workspace.get_status(directory).object_id
    before = generic_permissions.load_as_dict("directories", str(object_id))
    assert before[group_a.display_name] == PermissionLevel.CAN_MANAGE

    migrated_groups: list[MigratedGroup] = [MigratedGroup.partial_info(group_a, group_b)]

    if use_permission_migration_api:
        permission_manager.apply_group_permissions_private_preview_api(MigrationState(migrated_groups))
    else:
        apply_tasks(generic_permissions, migrated_groups)

    after = generic_permissions.load_as_dict("directories", str(object_id))
    assert after[group_b.display_name] == PermissionLevel.CAN_MANAGE


@pytest.mark.parametrize("use_permission_migration_api", [True, False])
@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_notebooks(
    ws: WorkspaceClient,
    permission_manager: PermissionManager,
    sql_backend,
    inventory_schema,
    make_group,
    make_acc_group,
    make_notebook,
    make_notebook_permissions,
    use_permission_migration_api: bool,
):
    group_a = make_group()
    group_b = make_acc_group()
    acc = AccountClient(host=ws.config.environment.deployment_url('accounts'))
    acc.workspace_assignment.update(ws.get_workspace_id(), group_b.id, [WorkspacePermission.USER])
    notebook = make_notebook()
    make_notebook_permissions(
        object_id=notebook,
        permission_level=PermissionLevel.CAN_MANAGE,
        group_name=group_a.display_name,
    )

    generic_permissions = GenericPermissionsSupport(
        ws,
        [
            WorkspaceListing(
                ws,
                sql_backend=sql_backend,
                inventory_database=inventory_schema,
                num_threads=10,
                start_path="/",
            )
        ],
    )
    object_id = ws.workspace.get_status(notebook).object_id
    before = generic_permissions.load_as_dict("notebooks", str(object_id))
    assert before[group_a.display_name] == PermissionLevel.CAN_MANAGE

    migrated_groups: list[MigratedGroup] = [MigratedGroup.partial_info(group_a, group_b)]

    if use_permission_migration_api:
        permission_manager.apply_group_permissions_private_preview_api(MigrationState(migrated_groups))
    else:
        apply_tasks(generic_permissions, migrated_groups)

    after = generic_permissions.load_as_dict("notebooks", str(object_id))
    assert after[group_b.display_name] == PermissionLevel.CAN_MANAGE


@retried(on=[BadRequest], timeout=timedelta(minutes=3))
@pytest.mark.parametrize("use_permission_migration_api", [True, False])
def test_tokens(
    acc: AccountClient,
    ws: WorkspaceClient,
    permission_manager: PermissionManager,
    make_group,
    make_acc_group,
    make_authorization_permissions,
    use_permission_migration_api: bool,
):
    group_a = make_group()
    group_b = make_acc_group()
    acc.workspace_assignment.update(ws.get_workspace_id(), group_b.id, [WorkspacePermission.USER])
    make_authorization_permissions(
        object_id="tokens",
        permission_level=PermissionLevel.CAN_USE,
        group_name=group_a.display_name,
    )

    generic_permissions = GenericPermissionsSupport(
        ws,
        [
            Listing(tokens_and_passwords, "object_id", "authorization"),
        ],
    )
    before = generic_permissions.load_as_dict("authorization", "tokens")
    assert before[group_a.display_name] == PermissionLevel.CAN_USE

    migrated_groups: list[MigratedGroup] = [MigratedGroup.partial_info(group_a, group_b)]

    if use_permission_migration_api:
        permission_manager.apply_group_permissions_private_preview_api(MigrationState(migrated_groups))
    else:
        apply_tasks(generic_permissions, migrated_groups)

    after = generic_permissions.load_as_dict("authorization", "tokens")
    assert after[group_b.display_name] == PermissionLevel.CAN_USE


@retried(on=[BadRequest], timeout=timedelta(minutes=3))
def test_verify_permissions(ws: WorkspaceClient, make_group, make_acc_group, make_job, make_job_permissions):
    group_a = make_group()
    job = make_job()
    make_job_permissions(
        object_id=job.job_id,
        permission_level=PermissionLevel.CAN_MANAGE,
        group_name=group_a.display_name,
    )

    generic_permissions = GenericPermissionsSupport(
        ws,
        [
            Listing(ws.jobs.list, "job_id", "jobs"),
        ],
    )

    item = Permissions(
        object_id=job.job_id,
        object_type="jobs",
        raw=json.dumps(
            iam.ObjectPermissions(
                object_id=job.job_id,
                object_type="jobs",
                access_control_list=[
                    iam.AccessControlResponse(
                        group_name=group_a.display_name,
                        all_permissions=[
                            iam.Permission(inherited=False, permission_level=iam.PermissionLevel.CAN_MANAGE)
                        ],
                    )
                ],
            ).as_dict()
        ),
    )

    task = generic_permissions.get_verify_task(item)
    result = task()

    assert result


@retried(on=[NotFound], timeout=timedelta(minutes=3))
@pytest.mark.parametrize("use_permission_migration_api", [True, False])
def test_endpoints(
    acc: AccountClient,
    ws: WorkspaceClient,
    permission_manager: PermissionManager,
    make_group,
    make_acc_group,
    make_serving_endpoint,
    make_serving_endpoint_permissions,  # pylint: disable=invalid-name
    use_permission_migration_api: bool,
):
    group_a = make_group()
    group_b = make_acc_group()
    acc.workspace_assignment.update(ws.get_workspace_id(), group_b.id, [WorkspacePermission.USER])
    endpoint = make_serving_endpoint()
    make_serving_endpoint_permissions(
        object_id=endpoint.response.id,
        permission_level=PermissionLevel.CAN_QUERY,
        group_name=group_a.display_name,
    )

    generic_permissions = GenericPermissionsSupport(ws, [Listing(ws.serving_endpoints.list, "id", "serving-endpoints")])
    before = generic_permissions.load_as_dict("serving-endpoints", endpoint.response.id)
    assert before[group_a.display_name] == PermissionLevel.CAN_QUERY

    migrated_groups: list[MigratedGroup] = [MigratedGroup.partial_info(group_a, group_b)]

    if use_permission_migration_api:
        permission_manager.apply_group_permissions_private_preview_api(MigrationState(migrated_groups))
    else:
        apply_tasks(generic_permissions, migrated_groups)

    after = generic_permissions.load_as_dict("serving-endpoints", endpoint.response.id)
    assert after[group_b.display_name] == PermissionLevel.CAN_QUERY


@pytest.mark.parametrize("use_permission_migration_api", [True, False])
def test_feature_tables(
    acc: AccountClient,
    ws: WorkspaceClient,
    permission_manager: PermissionManager,
    make_group,
    make_acc_group,
    make_feature_table,
    make_feature_table_permissions,
    use_permission_migration_api: bool,
):
    group_a = make_group()
    group_b = make_acc_group()
    acc.workspace_assignment.update(ws.get_workspace_id(), group_b.id, [WorkspacePermission.USER])
    feature_table = make_feature_table()
    make_feature_table_permissions(
        object_id=feature_table["id"],
        permission_level=PermissionLevel.CAN_EDIT_METADATA,
        group_name=group_a.display_name,
    )

    generic_permissions = GenericPermissionsSupport(
        ws, [Listing(feature_store_listing(ws), "object_id", "feature-tables")]
    )
    before = generic_permissions.load_as_dict("feature-tables", feature_table["id"])
    assert before[group_a.display_name] == PermissionLevel.CAN_EDIT_METADATA

    migrated_groups: list[MigratedGroup] = [MigratedGroup.partial_info(group_a, group_b)]

    if use_permission_migration_api:
        permission_manager.apply_group_permissions_private_preview_api(MigrationState(migrated_groups))
    else:
        apply_tasks(generic_permissions, migrated_groups)

    after = generic_permissions.load_as_dict("feature-tables", feature_table["id"])
    assert after[group_b.display_name] == PermissionLevel.CAN_EDIT_METADATA


@pytest.mark.parametrize("use_permission_migration_api", [True, False])
def test_feature_store_root_page(
    acc: AccountClient,
    ws: WorkspaceClient,
    permission_manager: PermissionManager,
    make_group,
    make_acc_group,
    use_permission_migration_api: bool,
):
    group_a = make_group()
    group_b = make_acc_group()
    acc.workspace_assignment.update(ws.get_workspace_id(), group_b.id, [WorkspacePermission.USER])
    ws.permissions.update(
        "feature-tables",
        "/root",
        access_control_list=[
            AccessControlRequest(group_name=group_a.display_name, permission_level=PermissionLevel.CAN_EDIT_METADATA)
        ],
    )

    generic_permissions = GenericPermissionsSupport(
        ws, [Listing(feature_tables_root_page, "object_id", "feature-tables")]
    )
    before = generic_permissions.load_as_dict("feature-tables", "/root")
    assert before[group_a.display_name] == PermissionLevel.CAN_EDIT_METADATA

    migrated_groups: list[MigratedGroup] = [MigratedGroup.partial_info(group_a, group_b)]

    if use_permission_migration_api:
        permission_manager.apply_group_permissions_private_preview_api(MigrationState(migrated_groups))
    else:
        apply_tasks(
            generic_permissions,
            migrated_groups,
        )

    after = generic_permissions.load_as_dict("feature-tables", "/root")
    assert after[group_b.display_name] == PermissionLevel.CAN_EDIT_METADATA


@pytest.mark.parametrize("use_permission_migration_api", [True, False])
def test_models_root_page(
    acc: AccountClient,
    ws: WorkspaceClient,
    permission_manager: PermissionManager,
    make_group,
    make_acc_group,
    use_permission_migration_api: bool,
):
    group_a = make_group()
    group_b = make_acc_group()
    acc.workspace_assignment.update(ws.get_workspace_id(), group_b.id, [WorkspacePermission.USER])

    ws.permissions.update(
        "registered-models",
        "/root",
        access_control_list=[
            AccessControlRequest(
                group_name=group_a.display_name, permission_level=PermissionLevel.CAN_MANAGE_PRODUCTION_VERSIONS
            )
        ],
    )

    generic_permissions = GenericPermissionsSupport(ws, [Listing(models_root_page, "object_id", "registered-models")])
    before = generic_permissions.load_as_dict("registered-models", "/root")
    assert before[group_a.display_name] == PermissionLevel.CAN_MANAGE_PRODUCTION_VERSIONS

    migrated_groups: list[MigratedGroup] = [MigratedGroup.partial_info(group_a, group_b)]

    if use_permission_migration_api:
        permission_manager.apply_group_permissions_private_preview_api(MigrationState(migrated_groups))
    else:
        apply_tasks(
            generic_permissions,
            migrated_groups,
        )

    after = generic_permissions.load_as_dict("registered-models", "/root")
    assert after[group_b.display_name] == PermissionLevel.CAN_MANAGE_PRODUCTION_VERSIONS

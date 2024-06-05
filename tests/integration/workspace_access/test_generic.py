import json
from datetime import timedelta

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import BadRequest, NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service import iam
from databricks.sdk.service.iam import AccessControlRequest, PermissionLevel

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
from databricks.labs.ucx.workspace_access.groups import MigrationState

from . import apply_tasks


@pytest.mark.parametrize("is_experimental", [True, False])
@retried(on=[NotFound, TimeoutError], timeout=timedelta(minutes=3))
def test_instance_pools(
    ws: WorkspaceClient,
    migrated_group,
    make_instance_pool,
    make_instance_pool_permissions,
    is_experimental: bool,
):
    pool = make_instance_pool()
    make_instance_pool_permissions(
        object_id=pool.instance_pool_id,
        permission_level=PermissionLevel.CAN_MANAGE,
        group_name=migrated_group.name_in_workspace,
    )

    generic_permissions = GenericPermissionsSupport(
        ws,
        [
            Listing(ws.instance_pools.list, "instance_pool_id", "instance-pools"),
        ],
    )
    before = generic_permissions.load_as_dict("instance-pools", pool.instance_pool_id)
    assert before[migrated_group.name_in_workspace] == PermissionLevel.CAN_MANAGE

    if is_experimental:
        MigrationState([migrated_group]).apply_to_groups_with_different_names(ws)
    else:
        apply_tasks(generic_permissions, [migrated_group])

    after = generic_permissions.load_as_dict("instance-pools", pool.instance_pool_id)
    assert after[migrated_group.name_in_account] == PermissionLevel.CAN_MANAGE


@pytest.mark.parametrize("is_experimental", [True, False])
@retried(on=[BadRequest], timeout=timedelta(minutes=3))
def test_clusters(
    ws: WorkspaceClient,
    migrated_group,
    make_cluster,
    make_cluster_permissions,
    env_or_skip,
    is_experimental: bool,
):
    cluster = make_cluster(instance_pool_id=env_or_skip("TEST_INSTANCE_POOL_ID"), single_node=True)
    make_cluster_permissions(
        object_id=cluster.cluster_id,
        permission_level=PermissionLevel.CAN_MANAGE,
        group_name=migrated_group.name_in_workspace,
    )

    generic_permissions = GenericPermissionsSupport(
        ws,
        [
            Listing(ws.clusters.list, "cluster_id", "clusters"),
        ],
    )
    before = generic_permissions.load_as_dict("clusters", cluster.cluster_id)
    assert before[migrated_group.name_in_workspace] == PermissionLevel.CAN_MANAGE

    if is_experimental:
        MigrationState([migrated_group]).apply_to_groups_with_different_names(ws)
    else:
        apply_tasks(generic_permissions, [migrated_group])

    after = generic_permissions.load_as_dict("clusters", cluster.cluster_id)
    assert after[migrated_group.name_in_account] == PermissionLevel.CAN_MANAGE


@pytest.mark.parametrize("is_experimental", [True, False])
@retried(on=[BadRequest], timeout=timedelta(minutes=3))
def test_jobs(ws: WorkspaceClient, migrated_group, make_job, make_job_permissions, is_experimental: bool):
    job = make_job()
    make_job_permissions(
        object_id=job.job_id,
        permission_level=PermissionLevel.CAN_MANAGE,
        group_name=migrated_group.name_in_workspace,
    )

    generic_permissions = GenericPermissionsSupport(
        ws,
        [
            Listing(ws.jobs.list, "job_id", "jobs"),
        ],
    )
    before = generic_permissions.load_as_dict("jobs", job.job_id)
    assert before[migrated_group.name_in_workspace] == PermissionLevel.CAN_MANAGE

    if is_experimental:
        MigrationState([migrated_group]).apply_to_groups_with_different_names(ws)
    else:
        apply_tasks(generic_permissions, [migrated_group])

    after = generic_permissions.load_as_dict("jobs", job.job_id)
    assert after[migrated_group.name_in_account] == PermissionLevel.CAN_MANAGE


@pytest.mark.parametrize("is_experimental", [True, False])
@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_pipelines(
    ws: WorkspaceClient,
    migrated_group,
    make_pipeline,
    make_pipeline_permissions,
    is_experimental: bool,
):
    pipeline = make_pipeline()
    make_pipeline_permissions(
        object_id=pipeline.pipeline_id,
        permission_level=PermissionLevel.CAN_MANAGE,
        group_name=migrated_group.name_in_workspace,
    )

    generic_permissions = GenericPermissionsSupport(
        ws,
        [
            Listing(ws.pipelines.list_pipelines, "pipeline_id", "pipelines"),
        ],
    )
    before = generic_permissions.load_as_dict("pipelines", pipeline.pipeline_id)
    assert before[migrated_group.name_in_workspace] == PermissionLevel.CAN_MANAGE

    if is_experimental:
        MigrationState([migrated_group]).apply_to_groups_with_different_names(ws)
    else:
        apply_tasks(generic_permissions, [migrated_group])

    after = generic_permissions.load_as_dict("pipelines", pipeline.pipeline_id)
    assert after[migrated_group.name_in_account] == PermissionLevel.CAN_MANAGE


@pytest.mark.parametrize("is_experimental", [True, False])
@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_cluster_policies(
    ws: WorkspaceClient,
    migrated_group,
    make_cluster_policy,
    make_cluster_policy_permissions,
    is_experimental: bool,
):
    cluster_policy = make_cluster_policy()
    make_cluster_policy_permissions(
        object_id=cluster_policy.policy_id,
        permission_level=PermissionLevel.CAN_USE,
        group_name=migrated_group.name_in_workspace,
    )

    generic_permissions = GenericPermissionsSupport(
        ws,
        [
            Listing(ws.cluster_policies.list, "policy_id", "cluster-policies"),
        ],
    )
    before = generic_permissions.load_as_dict("cluster-policies", cluster_policy.policy_id)
    assert before[migrated_group.name_in_workspace] == PermissionLevel.CAN_USE

    if is_experimental:
        MigrationState([migrated_group]).apply_to_groups_with_different_names(ws)
    else:
        apply_tasks(generic_permissions, [migrated_group])

    after = generic_permissions.load_as_dict("cluster-policies", cluster_policy.policy_id)
    assert after[migrated_group.name_in_account] == PermissionLevel.CAN_USE


@pytest.mark.parametrize("is_experimental", [True, False])
@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_warehouses(
    ws: WorkspaceClient,
    migrated_group,
    make_warehouse,
    make_warehouse_permissions,
    is_experimental: bool,
):
    warehouse = make_warehouse()
    make_warehouse_permissions(
        object_id=warehouse.id,
        permission_level=PermissionLevel.CAN_MANAGE,
        group_name=migrated_group.name_in_workspace,
    )

    generic_permissions = GenericPermissionsSupport(
        ws,
        [
            Listing(ws.warehouses.list, "id", "sql/warehouses"),
        ],
    )
    before = generic_permissions.load_as_dict("sql/warehouses", warehouse.id)
    assert before[migrated_group.name_in_workspace] == PermissionLevel.CAN_MANAGE

    if is_experimental:
        MigrationState([migrated_group]).apply_to_groups_with_different_names(ws)
    else:
        apply_tasks(generic_permissions, [migrated_group])

    after = generic_permissions.load_as_dict("sql/warehouses", warehouse.id)
    assert after[migrated_group.name_in_account] == PermissionLevel.CAN_MANAGE


@pytest.mark.parametrize("is_experimental", [True, False])
@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_models(
    ws: WorkspaceClient,
    migrated_group,
    make_model,
    make_registered_model_permissions,  # pylint: disable=invalid-name
    is_experimental: bool,
):
    model = make_model()
    make_registered_model_permissions(
        object_id=model.id,
        permission_level=PermissionLevel.CAN_MANAGE,
        group_name=migrated_group.name_in_workspace,
    )

    generic_permissions = GenericPermissionsSupport(
        ws,
        [
            Listing(models_listing(ws, 10), "id", "registered-models"),
        ],
    )
    before = generic_permissions.load_as_dict("registered-models", model.id)
    assert before[migrated_group.name_in_workspace] == PermissionLevel.CAN_MANAGE

    if is_experimental:
        MigrationState([migrated_group]).apply_to_groups_with_different_names(ws)
    else:
        apply_tasks(generic_permissions, [migrated_group])

    after = generic_permissions.load_as_dict("registered-models", model.id)
    assert after[migrated_group.name_in_account] == PermissionLevel.CAN_MANAGE


@pytest.mark.parametrize("is_experimental", [True, False])
@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_experiments(
    ws: WorkspaceClient,
    migrated_group,
    make_experiment,
    make_experiment_permissions,
    is_experimental: bool,
):
    experiment = make_experiment()
    make_experiment_permissions(
        object_id=experiment.experiment_id,
        permission_level=PermissionLevel.CAN_MANAGE,
        group_name=migrated_group.name_in_workspace,
    )

    generic_permissions = GenericPermissionsSupport(
        ws, [Listing(experiments_listing(ws), "experiment_id", "experiments")]
    )
    before = generic_permissions.load_as_dict("experiments", experiment.experiment_id)
    assert before[migrated_group.name_in_workspace] == PermissionLevel.CAN_MANAGE

    if is_experimental:
        MigrationState([migrated_group]).apply_to_groups_with_different_names(ws)
    else:
        apply_tasks(generic_permissions, [migrated_group])

    after = generic_permissions.load_as_dict("experiments", experiment.experiment_id)
    assert after[migrated_group.name_in_account] == PermissionLevel.CAN_MANAGE


@pytest.mark.parametrize("is_experimental", [True, False])
@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_directories(
    ws: WorkspaceClient,
    sql_backend,
    inventory_schema,
    migrated_group,
    make_directory,
    make_directory_permissions,
    is_experimental: bool,
):
    directory = make_directory()
    make_directory_permissions(
        object_id=directory,
        permission_level=PermissionLevel.CAN_MANAGE,
        group_name=migrated_group.name_in_workspace,
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
    assert before[migrated_group.name_in_workspace] == PermissionLevel.CAN_MANAGE

    if is_experimental:
        MigrationState([migrated_group]).apply_to_groups_with_different_names(ws)
    else:
        apply_tasks(generic_permissions, [migrated_group])

    after = generic_permissions.load_as_dict("directories", str(object_id))
    assert after[migrated_group.name_in_account] == PermissionLevel.CAN_MANAGE


@pytest.mark.parametrize("is_experimental", [True, False])
@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_notebooks(
    ws: WorkspaceClient,
    sql_backend,
    inventory_schema,
    migrated_group,
    make_notebook,
    make_notebook_permissions,
    is_experimental: bool,
):
    notebook = make_notebook()
    make_notebook_permissions(
        object_id=notebook,
        permission_level=PermissionLevel.CAN_MANAGE,
        group_name=migrated_group.name_in_workspace,
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
    assert before[migrated_group.name_in_workspace] == PermissionLevel.CAN_MANAGE

    if is_experimental:
        MigrationState([migrated_group]).apply_to_groups_with_different_names(ws)
    else:
        apply_tasks(generic_permissions, [migrated_group])

    after = generic_permissions.load_as_dict("notebooks", str(object_id))
    assert after[migrated_group.name_in_account] == PermissionLevel.CAN_MANAGE


@retried(on=[BadRequest], timeout=timedelta(minutes=3))
@pytest.mark.parametrize("is_experimental", [True, False])
def test_tokens(ws: WorkspaceClient, migrated_group, make_authorization_permissions, is_experimental: bool):
    make_authorization_permissions(
        object_id="tokens",
        permission_level=PermissionLevel.CAN_USE,
        group_name=migrated_group.name_in_workspace,
    )

    generic_permissions = GenericPermissionsSupport(
        ws,
        [
            Listing(tokens_and_passwords, "object_id", "authorization"),
        ],
    )
    before = generic_permissions.load_as_dict("authorization", "tokens")
    assert before[migrated_group.name_in_workspace] == PermissionLevel.CAN_USE

    if is_experimental:
        MigrationState([migrated_group]).apply_to_groups_with_different_names(ws)
    else:
        apply_tasks(generic_permissions, [migrated_group])

    after = generic_permissions.load_as_dict("authorization", "tokens")
    assert after[migrated_group.name_in_account] == PermissionLevel.CAN_USE


@retried(on=[BadRequest], timeout=timedelta(minutes=3))
def test_verify_permissions(ws: WorkspaceClient, make_group, make_job, make_job_permissions):
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
@pytest.mark.parametrize("is_experimental", [True, False])
def test_endpoints(
    ws: WorkspaceClient,
    migrated_group,
    make_serving_endpoint,
    make_serving_endpoint_permissions,  # pylint: disable=invalid-name
    is_experimental: bool,
):
    endpoint = make_serving_endpoint()
    make_serving_endpoint_permissions(
        object_id=endpoint.response.id,
        permission_level=PermissionLevel.CAN_QUERY,
        group_name=migrated_group.name_in_workspace,
    )

    generic_permissions = GenericPermissionsSupport(ws, [Listing(ws.serving_endpoints.list, "id", "serving-endpoints")])
    before = generic_permissions.load_as_dict("serving-endpoints", endpoint.response.id)
    assert before[migrated_group.name_in_workspace] == PermissionLevel.CAN_QUERY

    if is_experimental:
        MigrationState([migrated_group]).apply_to_groups_with_different_names(ws)
    else:
        apply_tasks(generic_permissions, [migrated_group])

    after = generic_permissions.load_as_dict("serving-endpoints", endpoint.response.id)
    assert after[migrated_group.name_in_account] == PermissionLevel.CAN_QUERY


@pytest.mark.parametrize("is_experimental", [True, False])
def test_feature_tables(
    ws: WorkspaceClient,
    migrated_group,
    make_feature_table,
    make_feature_table_permissions,
    is_experimental: bool,
):
    feature_table = make_feature_table()
    make_feature_table_permissions(
        object_id=feature_table["id"],
        permission_level=PermissionLevel.CAN_EDIT_METADATA,
        group_name=migrated_group.name_in_workspace,
    )

    generic_permissions = GenericPermissionsSupport(
        ws, [Listing(feature_store_listing(ws), "object_id", "feature-tables")]
    )
    before = generic_permissions.load_as_dict("feature-tables", feature_table["id"])
    assert before[migrated_group.name_in_workspace] == PermissionLevel.CAN_EDIT_METADATA

    if is_experimental:
        MigrationState([migrated_group]).apply_to_groups_with_different_names(ws)
    else:
        apply_tasks(generic_permissions, [migrated_group])

    after = generic_permissions.load_as_dict("feature-tables", feature_table["id"])
    assert after[migrated_group.name_in_account] == PermissionLevel.CAN_EDIT_METADATA


@pytest.mark.parametrize("is_experimental", [True, False])
def test_feature_store_root_page(ws: WorkspaceClient, migrated_group, is_experimental: bool):
    ws.permissions.update(
        "feature-tables",
        "/root",
        access_control_list=[
            AccessControlRequest(
                group_name=migrated_group.name_in_workspace, permission_level=PermissionLevel.CAN_EDIT_METADATA
            )
        ],
    )

    generic_permissions = GenericPermissionsSupport(
        ws, [Listing(feature_tables_root_page, "object_id", "feature-tables")]
    )
    before = generic_permissions.load_as_dict("feature-tables", "/root")
    assert before[migrated_group.name_in_workspace] == PermissionLevel.CAN_EDIT_METADATA

    if is_experimental:
        MigrationState([migrated_group]).apply_to_groups_with_different_names(ws)
    else:
        apply_tasks(
            generic_permissions,
            [migrated_group],
        )

    after = generic_permissions.load_as_dict("feature-tables", "/root")
    assert after[migrated_group.name_in_account] == PermissionLevel.CAN_EDIT_METADATA


@pytest.mark.parametrize("is_experimental", [True, False])
def test_models_root_page(ws: WorkspaceClient, migrated_group, is_experimental: bool):

    ws.permissions.update(
        "registered-models",
        "/root",
        access_control_list=[
            AccessControlRequest(
                group_name=migrated_group.name_in_workspace,
                permission_level=PermissionLevel.CAN_MANAGE_PRODUCTION_VERSIONS,
            )
        ],
    )

    generic_permissions = GenericPermissionsSupport(ws, [Listing(models_root_page, "object_id", "registered-models")])
    before = generic_permissions.load_as_dict("registered-models", "/root")
    assert before[migrated_group.name_in_workspace] == PermissionLevel.CAN_MANAGE_PRODUCTION_VERSIONS

    if is_experimental:
        MigrationState([migrated_group]).apply_to_groups_with_different_names(ws)
    else:
        apply_tasks(
            generic_permissions,
            [migrated_group],
        )

    after = generic_permissions.load_as_dict("registered-models", "/root")
    assert after[migrated_group.name_in_account] == PermissionLevel.CAN_MANAGE_PRODUCTION_VERSIONS

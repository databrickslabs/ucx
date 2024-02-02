import json
from datetime import timedelta

import pytest
from databricks.sdk.errors import BadRequest, NotFound
from databricks.sdk.retries import retried
from databricks.sdk.service import iam
from databricks.sdk.service.iam import PermissionLevel

from databricks.labs.ucx.workspace_access.base import Permissions
from databricks.labs.ucx.workspace_access.generic import (
    GenericPermissionsSupport,
    Listing,
    WorkspaceListing,
    experiments_listing,
    models_listing,
    tokens_and_passwords,
)
from databricks.labs.ucx.workspace_access.groups import MigratedGroup

from . import apply_tasks


@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_instance_pools(ws, make_group, make_instance_pool, make_instance_pool_permissions):
    group_a = make_group()
    group_b = make_group()
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

    apply_tasks(
        generic_permissions,
        [
            MigratedGroup.partial_info(group_a, group_b),
        ],
    )

    after = generic_permissions.load_as_dict("instance-pools", pool.instance_pool_id)
    assert after[group_b.display_name] == PermissionLevel.CAN_MANAGE


@retried(on=[BadRequest], timeout=timedelta(minutes=3))
def test_clusters(ws, make_group, make_cluster, make_cluster_permissions, env_or_skip):
    group_a = make_group()
    group_b = make_group()
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

    apply_tasks(
        generic_permissions,
        [
            MigratedGroup.partial_info(group_a, group_b),
        ],
    )

    after = generic_permissions.load_as_dict("clusters", cluster.cluster_id)
    assert after[group_b.display_name] == PermissionLevel.CAN_MANAGE


@retried(on=[BadRequest], timeout=timedelta(minutes=3))
def test_jobs(ws, make_group, make_job, make_job_permissions):
    group_a = make_group()
    group_b = make_group()
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

    apply_tasks(
        generic_permissions,
        [
            MigratedGroup.partial_info(group_a, group_b),
        ],
    )

    after = generic_permissions.load_as_dict("jobs", job.job_id)
    assert after[group_b.display_name] == PermissionLevel.CAN_MANAGE


@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_pipelines(ws, make_group, make_pipeline, make_pipeline_permissions):
    group_a = make_group()
    group_b = make_group()
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

    apply_tasks(
        generic_permissions,
        [
            MigratedGroup.partial_info(group_a, group_b),
        ],
    )

    after = generic_permissions.load_as_dict("pipelines", pipeline.pipeline_id)
    assert after[group_b.display_name] == PermissionLevel.CAN_MANAGE


@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_cluster_policies(ws, make_group, make_cluster_policy, make_cluster_policy_permissions, env_or_skip):
    group_a = make_group()
    group_b = make_group()
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

    apply_tasks(
        generic_permissions,
        [
            MigratedGroup.partial_info(group_a, group_b),
        ],
    )

    after = generic_permissions.load_as_dict("cluster-policies", cluster_policy.policy_id)
    assert after[group_b.display_name] == PermissionLevel.CAN_USE


@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_warehouses(ws, make_group, make_warehouse, make_warehouse_permissions):
    group_a = make_group()
    group_b = make_group()
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

    apply_tasks(
        generic_permissions,
        [
            MigratedGroup.partial_info(group_a, group_b),
        ],
    )

    after = generic_permissions.load_as_dict("sql/warehouses", warehouse.id)
    assert after[group_b.display_name] == PermissionLevel.CAN_MANAGE


@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_models(ws, make_group, make_model, make_registered_model_permissions):
    group_a = make_group()
    group_b = make_group()
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

    apply_tasks(
        generic_permissions,
        [
            MigratedGroup.partial_info(group_a, group_b),
        ],
    )

    after = generic_permissions.load_as_dict("registered-models", model.id)
    assert after[group_b.display_name] == PermissionLevel.CAN_MANAGE


@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_experiments(ws, make_group, make_experiment, make_experiment_permissions):
    group_a = make_group()
    group_b = make_group()
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

    apply_tasks(
        generic_permissions,
        [
            MigratedGroup.partial_info(group_a, group_b),
        ],
    )

    after = generic_permissions.load_as_dict("experiments", experiment.experiment_id)
    assert after[group_b.display_name] == PermissionLevel.CAN_MANAGE


@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_directories(ws, sql_backend, inventory_schema, make_group, make_directory, make_directory_permissions):
    group_a = make_group()
    group_b = make_group()
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
    before = generic_permissions.load_as_dict("directories", object_id)
    assert before[group_a.display_name] == PermissionLevel.CAN_MANAGE

    apply_tasks(
        generic_permissions,
        [
            MigratedGroup.partial_info(group_a, group_b),
        ],
    )

    after = generic_permissions.load_as_dict("directories", object_id)
    assert after[group_b.display_name] == PermissionLevel.CAN_MANAGE


@retried(on=[NotFound], timeout=timedelta(minutes=3))
def test_notebooks(ws, sql_backend, inventory_schema, make_group, make_notebook, make_notebook_permissions):
    group_a = make_group()
    group_b = make_group()
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
    before = generic_permissions.load_as_dict("notebooks", object_id)
    assert before[group_a.display_name] == PermissionLevel.CAN_MANAGE

    apply_tasks(
        generic_permissions,
        [
            MigratedGroup.partial_info(group_a, group_b),
        ],
    )

    after = generic_permissions.load_as_dict("notebooks", object_id)
    assert after[group_b.display_name] == PermissionLevel.CAN_MANAGE


@retried(on=[BadRequest], timeout=timedelta(minutes=3))
def test_tokens(ws, make_group, make_authorization_permissions):
    group_a = make_group()
    group_b = make_group()
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

    apply_tasks(
        generic_permissions,
        [
            MigratedGroup.partial_info(group_a, group_b),
        ],
    )

    after = generic_permissions.load_as_dict("authorization", "tokens")
    assert after[group_b.display_name] == PermissionLevel.CAN_USE


@retried(on=[BadRequest], timeout=timedelta(minutes=3))
def test_verify_permissions(ws, make_group, make_job, make_job_permissions):
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


@retried(on=[BadRequest], timeout=timedelta(minutes=3))
def test_verify_permissions_missing(ws, make_group, make_job, make_job_permissions):
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
                        all_permissions=[iam.Permission(inherited=False, permission_level=iam.PermissionLevel.CAN_USE)],
                    )
                ],
            ).as_dict()
        ),
    )

    task = generic_permissions.get_verify_task(item)
    with pytest.raises(ValueError):
        task()

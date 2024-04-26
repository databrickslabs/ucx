import json
from datetime import timedelta
from unittest.mock import Mock, create_autospec

import pytest
from databricks.labs.blueprint.parallel import ManyError
from databricks.labs.lsql.backends import MockBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import DatabricksError
from databricks.sdk.errors import Aborted, InternalError, NotFound, PermissionDenied
from databricks.sdk.service import compute, iam, ml
from databricks.sdk.service.compute import ClusterDetails
from databricks.sdk.service.iam import (
    AccessControlResponse,
    ObjectPermissions,
    Permission,
    PermissionLevel,
)
from databricks.sdk.service.jobs import BaseJob
from databricks.sdk.service.pipelines import PipelineStateInfo
from databricks.sdk.service.workspace import Language, ObjectInfo, ObjectType

from databricks.labs.ucx.workspace_access.generic import (
    GenericPermissionsSupport,
    Listing,
    Permissions,
    WorkspaceListing,
    WorkspaceObjectInfo,
    experiments_listing,
    feature_store_listing,
    feature_tables_root_page,
    models_listing,
    models_root_page,
    tokens_and_passwords,
)
from databricks.labs.ucx.workspace_access.groups import MigrationState

# pylint: disable=protected-access


def test_crawler():
    ws = create_autospec(WorkspaceClient)
    ws.clusters.list.return_value = [
        compute.ClusterDetails(
            cluster_id="test",
        )
    ]

    sample_permission = iam.ObjectPermissions(
        object_id="test",
        object_type="clusters",
        access_control_list=[
            iam.AccessControlResponse(
                group_name="test",
                all_permissions=[iam.Permission(inherited=False, permission_level=iam.PermissionLevel.CAN_USE)],
            )
        ],
    )

    ws.permissions.get.return_value = sample_permission

    sup = GenericPermissionsSupport(
        ws=ws,
        listings=[
            Listing(ws.clusters.list, "cluster_id", "clusters"),
        ],
    )

    tasks = list(sup.get_crawler_tasks())
    assert len(tasks) == 1
    ws.clusters.list.assert_called_once()
    _task = tasks[0]
    item = _task()
    ws.permissions.get.assert_called_once()
    assert item.object_id == "test"
    assert item.object_type == "clusters"
    assert json.loads(item.raw) == sample_permission.as_dict()


def test_apply(migration_state):
    ws = create_autospec(WorkspaceClient)

    acl1 = iam.AccessControlResponse(
        all_permissions=[iam.Permission(permission_level=iam.PermissionLevel.CAN_USE)], group_name="test"
    )
    ws.permissions.get.return_value = iam.ObjectPermissions(access_control_list=[acl1])
    sup = GenericPermissionsSupport(ws=ws, listings=[])  # no listings since only apply is tested

    item = Permissions(
        object_id="test",
        object_type="clusters",
        raw=json.dumps(
            iam.ObjectPermissions(
                object_id="test",
                object_type="clusters",
                access_control_list=[
                    iam.AccessControlResponse(
                        group_name="test",
                        all_permissions=[iam.Permission(inherited=False, permission_level=iam.PermissionLevel.CAN_USE)],
                    ),
                    iam.AccessControlResponse(
                        group_name="irrelevant",
                        all_permissions=[
                            iam.Permission(inherited=False, permission_level=iam.PermissionLevel.CAN_MANAGE)
                        ],
                    ),
                ],
            ).as_dict()
        ),
    )

    _task = sup.get_apply_task(item, migration_state)
    _task()
    ws.permissions.update.assert_called_once()

    expected_acl_payload = [
        iam.AccessControlRequest(
            group_name="test",
            permission_level=iam.PermissionLevel.CAN_USE,
        )
    ]

    ws.permissions.update.assert_called_with("clusters", "test", access_control_list=expected_acl_payload)


def test_relevance():
    ws = create_autospec(WorkspaceClient)
    sup = GenericPermissionsSupport(ws=ws, listings=[])  # no listings since only apply is tested
    item = Permissions(object_id="passwords", object_type="passwords", raw="{}")
    migration_state = MigrationState([])
    task = sup.get_apply_task(item, migration_state)
    assert task is not None
    ws.permissions.update.assert_not_called()


def test_safe_get_permissions_when_error_non_retriable():
    ws = create_autospec(WorkspaceClient)
    ws.permissions.get.side_effect = NotFound(...)
    sup = GenericPermissionsSupport(ws=ws, listings=[])
    result = sup.load_as_dict("clusters", "test")
    assert not result


def test_safe_get_permissions_when_error_retriable():
    ws = create_autospec(WorkspaceClient)
    ws.permissions.get.side_effect = Aborted(...)
    sup = GenericPermissionsSupport(ws=ws, listings=[])
    with pytest.raises(DatabricksError) as e:
        sup.load_as_dict("clusters", "test")
    assert e.type == Aborted


def test_no_permissions():
    ws = create_autospec(WorkspaceClient)
    ws.clusters.list.return_value = [
        compute.ClusterDetails(
            cluster_id="test",
        )
    ]
    ws.permissions.get.side_effect = NotFound(...)
    sup = GenericPermissionsSupport(
        ws=ws,
        listings=[
            Listing(ws.clusters.list, "cluster_id", "clusters"),
        ],
    )
    tasks = list(sup.get_crawler_tasks())
    assert len(tasks) == 1
    ws.clusters.list.assert_called_once()
    _task = tasks[0]
    item = _task()
    assert item is None


def test_passwords_tokens_crawler(migration_state):
    ws = create_autospec(WorkspaceClient)

    basic_acl = [
        iam.AccessControlResponse(
            group_name="test",
            all_permissions=[iam.Permission(inherited=False, permission_level=iam.PermissionLevel.CAN_USE)],
        )
    ]

    temp_acl = [
        iam.AccessControlResponse(
            group_name="test",
            all_permissions=[iam.Permission(inherited=False, permission_level=iam.PermissionLevel.CAN_USE)],
        )
    ]
    ws.permissions.get.side_effect = [
        iam.ObjectPermissions(object_id="passwords", object_type="authorization", access_control_list=basic_acl),
        iam.ObjectPermissions(object_id="tokens", object_type="authorization", access_control_list=basic_acl),
        iam.ObjectPermissions(object_id="passwords", object_type="authorization", access_control_list=temp_acl),
        iam.ObjectPermissions(object_id="tokens", object_type="authorization", access_control_list=temp_acl),
    ]

    sup = GenericPermissionsSupport(ws=ws, listings=[Listing(tokens_and_passwords, "object_id", "authorization")])
    tasks = list(sup.get_crawler_tasks())
    assert len(tasks) == 2
    auth_items = [task() for task in tasks]
    for item in auth_items:
        assert item.object_id in {"tokens", "passwords"}
        assert item.object_type == "authorization"
        applier = sup.get_apply_task(item, migration_state)
        applier()
        ws.permissions.update.assert_called_once_with(
            item.object_type,
            item.object_id,
            access_control_list=[
                iam.AccessControlRequest(group_name="test", permission_level=iam.PermissionLevel.CAN_USE)
            ],
        )
        ws.permissions.update.reset_mock()


def test_models_listing():
    ws = create_autospec(WorkspaceClient)
    ws.model_registry.list_models.return_value = [ml.Model(name="test"), ml.Model(name="test2")]
    ws.model_registry.get_model.return_value = ml.GetModelResponse(
        registered_model_databricks=ml.ModelDatabricks(
            id="some-id",
            name="test",
        )
    )

    wrapped = Listing(models_listing(ws, 2), id_attribute="id", object_type="registered-models")
    result = list(wrapped)
    assert len(result) == 2
    assert result[0].object_id == "some-id"
    assert result[0].request_type == "registered-models"


def test_models_listing_failure_raise_error():
    ws = create_autospec(WorkspaceClient)
    ws.model_registry.list_models.return_value = [ml.Model(name="test")]
    ws.model_registry.get_model.side_effect = InternalError(...)

    wrapped = Listing(models_listing(ws, 2), id_attribute="id", object_type="registered-models")
    with pytest.raises(ManyError) as e:
        list(wrapped)
    assert e.type == ManyError


def test_experiment_listing():
    ws = create_autospec(WorkspaceClient)
    ws.experiments.list_experiments.return_value = [
        ml.Experiment(experiment_id="test"),
        ml.Experiment(experiment_id="test2", tags=[ml.ExperimentTag(key="whatever", value="SOMETHING")]),
        ml.Experiment(experiment_id="test3", tags=[ml.ExperimentTag(key="mlflow.experimentType", value="NOTEBOOK")]),
        ml.Experiment(
            experiment_id="test4", tags=[ml.ExperimentTag(key="mlflow.experiment.sourceType", value="REPO_NOTEBOOK")]
        ),
    ]
    wrapped = Listing(experiments_listing(ws), id_attribute="experiment_id", object_type="experiments")
    results = list(wrapped)
    assert len(results) == 2
    for res in results:
        assert res.request_type == "experiments"
        assert res.object_id in {"test", "test2"}


def test_response_to_request_mapping():
    permissions1 = [
        iam.Permission(permission_level=iam.PermissionLevel.CAN_BIND),
        iam.Permission(permission_level=iam.PermissionLevel.CAN_MANAGE),
    ]
    response1 = iam.AccessControlResponse(all_permissions=permissions1, user_name="test1212")

    permissions2 = [iam.Permission(permission_level=iam.PermissionLevel.CAN_ATTACH_TO)]
    response2 = iam.AccessControlResponse(all_permissions=permissions2, group_name="data-engineers")

    permissions3 = [iam.Permission(permission_level=iam.PermissionLevel.CAN_MANAGE_PRODUCTION_VERSIONS)]
    response3 = iam.AccessControlResponse(all_permissions=permissions3, service_principal_name="sp1")

    object_permissions = iam.ObjectPermissions(access_control_list=[response1, response2, response3])

    ws = create_autospec(WorkspaceClient)
    sup = GenericPermissionsSupport(ws=ws, listings=[])
    results = sup._response_to_request(object_permissions.access_control_list)
    ws.permissions.update.assert_not_called()

    assert results == [
        iam.AccessControlRequest(permission_level=iam.PermissionLevel.CAN_BIND, user_name="test1212"),
        iam.AccessControlRequest(permission_level=iam.PermissionLevel.CAN_MANAGE, user_name="test1212"),
        iam.AccessControlRequest(permission_level=iam.PermissionLevel.CAN_ATTACH_TO, group_name="data-engineers"),
        iam.AccessControlRequest(
            permission_level=iam.PermissionLevel.CAN_MANAGE_PRODUCTION_VERSIONS, service_principal_name="sp1"
        ),
    ]


def test_applier_task_should_return_true_if_permission_is_up_to_date():
    ws = create_autospec(WorkspaceClient)
    acl1 = iam.AccessControlResponse(
        all_permissions=[iam.Permission(permission_level=iam.PermissionLevel.CAN_USE)], group_name="group"
    )
    acl2 = iam.AccessControlResponse(
        all_permissions=[iam.Permission(permission_level=iam.PermissionLevel.CAN_RUN)], group_name="group2"
    )
    ws.permissions.get.return_value = iam.ObjectPermissions(access_control_list=[acl1, acl2])

    sup = GenericPermissionsSupport(ws=ws, listings=[], verify_timeout=timedelta(seconds=1))
    result = sup._applier_task(
        object_type="clusters",
        object_id="cluster_id",
        acl=[iam.AccessControlRequest(group_name="group", permission_level=iam.PermissionLevel.CAN_USE)],
    )
    assert result


def test_applier_task_should_return_true_if_permission_is_up_to_date_with_multiple_permissions():
    ws = create_autospec(WorkspaceClient)
    acl = iam.AccessControlResponse(
        all_permissions=[
            iam.Permission(permission_level=iam.PermissionLevel.CAN_USE),
            iam.Permission(permission_level=iam.PermissionLevel.CAN_ATTACH_TO),
            iam.Permission(permission_level=iam.PermissionLevel.CAN_RUN),
        ],
        group_name="group",
    )

    ws.permissions.get.return_value = iam.ObjectPermissions(access_control_list=[acl])
    sup = GenericPermissionsSupport(ws=ws, listings=[], verify_timeout=timedelta(seconds=1))

    result = sup._applier_task(
        object_type="clusters",
        object_id="cluster_id",
        acl=[
            iam.AccessControlRequest(group_name="group", permission_level=iam.PermissionLevel.CAN_USE),
            iam.AccessControlRequest(group_name="group", permission_level=iam.PermissionLevel.CAN_ATTACH_TO),
        ],
    )
    assert result


def test_applier_task_failed():
    ws = create_autospec(WorkspaceClient)
    acl = iam.AccessControlResponse(all_permissions=[], group_name="group")

    ws.permissions.update.return_value = iam.ObjectPermissions(access_control_list=[acl])
    ws.permissions.get.return_value = iam.ObjectPermissions(access_control_list=[acl])
    sup = GenericPermissionsSupport(ws=ws, listings=[], verify_timeout=timedelta(seconds=1))
    with pytest.raises(TimeoutError) as e:
        sup._applier_task(
            object_type="clusters",
            object_id="cluster_id",
            acl=[iam.AccessControlRequest(group_name="group", permission_level=iam.PermissionLevel.CAN_USE)],
        )
    assert "Timed out after" in str(e.value)


def test_applier_task_failed_when_all_permissions_not_up_to_date():
    ws = create_autospec(WorkspaceClient)
    group_1_acl = iam.AccessControlResponse(
        all_permissions=[
            iam.Permission(permission_level=iam.PermissionLevel.CAN_USE),
            iam.Permission(permission_level=iam.PermissionLevel.CAN_ATTACH_TO),
            iam.Permission(permission_level=iam.PermissionLevel.CAN_RUN),
        ],
        group_name="group_1",
    )
    group_2_acl = iam.AccessControlResponse(
        all_permissions=[
            iam.Permission(permission_level=iam.PermissionLevel.CAN_USE),
            iam.Permission(permission_level=iam.PermissionLevel.CAN_MANAGE),
        ],
        group_name="group_2",
    )
    ws.permissions.update.return_value = iam.ObjectPermissions(access_control_list=[group_1_acl, group_2_acl])
    ws.permissions.get.return_value = iam.ObjectPermissions(access_control_list=[group_1_acl, group_2_acl])
    sup = GenericPermissionsSupport(ws=ws, listings=[], verify_timeout=timedelta(seconds=1))
    with pytest.raises(TimeoutError) as e:
        sup._applier_task(
            object_type="clusters",
            object_id="cluster_id",
            acl=[
                iam.AccessControlRequest(group_name="group_1", permission_level=iam.PermissionLevel.CAN_USE),
                iam.AccessControlRequest(group_name="group_1", permission_level=iam.PermissionLevel.CAN_MANAGE),
            ],
        )
    assert "Timed out after" in str(e.value)


def test_applier_task_failed_when_get_error_retriable():
    ws = create_autospec(WorkspaceClient)
    group_1_acl = iam.AccessControlResponse(
        all_permissions=[
            iam.Permission(permission_level=iam.PermissionLevel.CAN_USE),
        ],
        group_name="group_1",
    )
    group_2_acl = iam.AccessControlResponse(
        all_permissions=[
            iam.Permission(permission_level=iam.PermissionLevel.CAN_USE),
        ],
        group_name="group_2",
    )
    ws.permissions.update.return_value = iam.ObjectPermissions(access_control_list=[group_1_acl, group_2_acl])
    ws.permissions.get.side_effect = Aborted(...)
    sup = GenericPermissionsSupport(ws=ws, listings=[], verify_timeout=timedelta(seconds=1))
    with pytest.raises(TimeoutError) as e:
        sup._applier_task(
            object_type="clusters",
            object_id="cluster_id",
            acl=[
                iam.AccessControlRequest(group_name="group_1", permission_level=iam.PermissionLevel.CAN_USE),
            ],
        )
    assert "Timed out" in str(e.value)


def test_applier_task_failed_when_get_error_non_retriable():
    ws = create_autospec(WorkspaceClient)
    group_1_acl = iam.AccessControlResponse(
        all_permissions=[
            iam.Permission(permission_level=iam.PermissionLevel.CAN_USE),
        ],
        group_name="group_1",
    )
    group_2_acl = iam.AccessControlResponse(
        all_permissions=[
            iam.Permission(permission_level=iam.PermissionLevel.CAN_USE),
        ],
        group_name="group_2",
    )
    ws.permissions.update.return_value = iam.ObjectPermissions(access_control_list=[group_1_acl, group_2_acl])
    ws.permissions.get.side_effect = PermissionDenied(...)
    sup = GenericPermissionsSupport(ws=ws, listings=[], verify_timeout=timedelta(seconds=1))
    result = sup._applier_task(
        object_type="clusters",
        object_id="cluster_id",
        acl=[
            iam.AccessControlRequest(group_name="group_1", permission_level=iam.PermissionLevel.CAN_USE),
        ],
    )
    assert result is False


def test_safe_update_permissions_when_error_non_retriable():
    ws = create_autospec(WorkspaceClient)
    ws.permissions.update.side_effect = PermissionDenied(...)

    sup = GenericPermissionsSupport(ws=ws, listings=[], verify_timeout=timedelta(seconds=1))

    result = sup._safe_update_permissions(
        object_type="clusters",
        object_id="cluster_id",
        acl=[iam.AccessControlRequest(group_name="group", permission_level=iam.PermissionLevel.CAN_USE)],
    )
    assert result is None


def test_safe_update_permissions_when_error_retriable():
    ws = create_autospec(WorkspaceClient)
    ws.permissions.update.side_effect = InternalError(...)

    sup = GenericPermissionsSupport(ws=ws, listings=[], verify_timeout=timedelta(seconds=1))

    with pytest.raises(DatabricksError) as e:
        sup._safe_update_permissions(
            object_type="clusters",
            object_id="cluster_id",
            acl=[iam.AccessControlRequest(group_name="group", permission_level=iam.PermissionLevel.CAN_USE)],
        )
    assert e.type == InternalError


def test_load_as_dict():
    ws = create_autospec(WorkspaceClient)

    cluster_id = "cluster_test"
    group_name = "group_test"
    user_name = "user_test"
    sp_name = "sp_test"

    ws.clusters.list.return_value = [
        compute.ClusterDetails(
            cluster_id=cluster_id,
        )
    ]

    sample_permission = iam.ObjectPermissions(
        object_id=cluster_id,
        object_type="clusters",
        access_control_list=[
            iam.AccessControlResponse(
                group_name=group_name,
                all_permissions=[iam.Permission(inherited=False, permission_level=iam.PermissionLevel.CAN_USE)],
            ),
            iam.AccessControlResponse(
                user_name=user_name,
                all_permissions=[iam.Permission(inherited=False, permission_level=iam.PermissionLevel.CAN_RUN)],
            ),
            iam.AccessControlResponse(
                service_principal_name=sp_name,
                all_permissions=[iam.Permission(inherited=False, permission_level=iam.PermissionLevel.CAN_MANAGE)],
            ),
            iam.AccessControlResponse(
                all_permissions=[iam.Permission(inherited=False, permission_level=iam.PermissionLevel.CAN_ATTACH_TO)],
            ),
        ],
    )

    ws.permissions.get.return_value = sample_permission
    sup = GenericPermissionsSupport(ws=ws, listings=[Listing(ws.clusters.list, "cluster_id", "clusters")])

    policy_permissions = sup.load_as_dict("clusters", cluster_id)

    assert iam.PermissionLevel.CAN_USE == policy_permissions[group_name]
    assert iam.PermissionLevel.CAN_RUN == policy_permissions[user_name]
    assert iam.PermissionLevel.CAN_MANAGE == policy_permissions[sp_name]
    assert iam.PermissionLevel.CAN_ATTACH_TO == policy_permissions["UNKNOWN"]


def test_load_as_dict_permissions_not_found():
    ws = create_autospec(WorkspaceClient)

    sup = GenericPermissionsSupport(
        ws=ws,
        listings=[],
    )

    ws.permissions.get.side_effect = Mock(side_effect=NotFound(...))

    policy_permissions = sup.load_as_dict("clusters", "cluster_test")

    assert len(policy_permissions) == 0


def test_load_as_dict_no_acls():
    ws = create_autospec(WorkspaceClient)

    cluster_id = "cluster_test"

    ws.clusters.list.return_value = [
        compute.ClusterDetails(
            cluster_id=cluster_id,
        )
    ]

    sample_permission = iam.ObjectPermissions(object_id=cluster_id, object_type="clusters", access_control_list=[])

    ws.permissions.get.return_value = sample_permission
    sup = GenericPermissionsSupport(ws=ws, listings=[Listing(ws.clusters.list, "cluster_id", "clusters")])

    policy_permissions = sup.load_as_dict("clusters", cluster_id)

    assert len(policy_permissions) == 0


def test_load_as_dict_handle_exception_when_getting_permissions():
    ws = create_autospec(WorkspaceClient)

    sup = GenericPermissionsSupport(
        ws=ws,
        listings=[],
    )

    ws.permissions.get.side_effect = Mock(side_effect=NotFound(...))

    policy_permissions = sup.load_as_dict("clusters", "cluster_test")

    assert len(policy_permissions) == 0


def test_load_as_dict_no_permissions():
    ws = create_autospec(WorkspaceClient)

    sup = GenericPermissionsSupport(
        ws=ws,
        listings=[],
    )

    ws.permissions.get.return_value = None

    policy_permissions = sup.load_as_dict("clusters", "cluster_test")

    assert len(policy_permissions) == 0


def test_load_as_dict_no_permission_level():
    ws = create_autospec(WorkspaceClient)

    cluster_id = "cluster_test"
    group_name = "group_test"

    ws.clusters.list.return_value = [
        compute.ClusterDetails(
            cluster_id=cluster_id,
        )
    ]

    sample_permission = iam.ObjectPermissions(
        object_id=cluster_id,
        object_type="clusters",
        access_control_list=[
            iam.AccessControlResponse(
                group_name=group_name,
                all_permissions=[iam.Permission(inherited=False)],
            )
        ],
    )

    ws.permissions.get.return_value = sample_permission
    sup = GenericPermissionsSupport(ws=ws, listings=[Listing(ws.clusters.list, "cluster_id", "clusters")])

    policy_permissions = sup.load_as_dict("clusters", cluster_id)

    assert len(policy_permissions) == 0


def test_workspaceobject_crawl():
    sample_objects = [
        ObjectInfo(
            object_type=ObjectType.NOTEBOOK,
            path="/notebook1",
            language=Language.PYTHON,
            object_id=123,
        ),
    ]
    ws = Mock()
    ws.workspace.list.return_value = sample_objects
    crawler = WorkspaceListing(ws, MockBackend(), "ucx").snapshot()
    result_set = list(crawler)

    assert len(result_set) == 2
    assert result_set[1] == WorkspaceObjectInfo("/notebook1", "NOTEBOOK", "123", "PYTHON")


def test_eligibles_assets_with_owner_should_be_accepted():
    ws = create_autospec(WorkspaceClient)
    ws.jobs.list.return_value = [BaseJob(job_id=13)]
    ws.pipelines.list_pipelines.return_value = [PipelineStateInfo(pipeline_id="12")]

    def perms(object_type: str, object_id: str):
        if object_type == "jobs":
            return ObjectPermissions(
                object_id=object_id,
                object_type=object_type,
                access_control_list=[
                    AccessControlResponse(
                        group_name="de", all_permissions=[Permission(permission_level=PermissionLevel.IS_OWNER)]
                    ),
                    AccessControlResponse(
                        group_name="ds", all_permissions=[Permission(permission_level=PermissionLevel.CAN_USE)]
                    ),
                ],
            )
        if object_type == "pipelines":
            return ObjectPermissions(
                object_id=object_id,
                object_type=object_type,
                access_control_list=[
                    AccessControlResponse(
                        group_name="de", all_permissions=[Permission(permission_level=PermissionLevel.IS_OWNER)]
                    ),
                    AccessControlResponse(
                        group_name="de", all_permissions=[Permission(permission_level=PermissionLevel.CAN_RUN)]
                    ),
                ],
            )
        return None

    ws.permissions.get.side_effect = perms

    sup = GenericPermissionsSupport(
        ws=ws,
        listings=[
            Listing(ws.jobs.list, "job_id", "jobs"),
            Listing(ws.pipelines.list_pipelines, "pipeline_id", "pipelines"),
        ],
    )
    tasks = []
    for executable in list(sup.get_crawler_tasks()):
        task = executable()
        if task is not None:
            tasks.append(task)
    assert len(tasks) == 2


def test_eligibles_assets_without_owner_should_be_ignored():
    ws = create_autospec(WorkspaceClient)
    ws.clusters.list.return_value = [ClusterDetails(cluster_id="1234")]
    ws.jobs.list.return_value = [BaseJob(job_id=13)]
    ws.pipelines.list_pipelines.return_value = [PipelineStateInfo(pipeline_id="12")]

    def perms(object_type: str, object_id: str):
        if object_type == "clusters":
            return ObjectPermissions(
                object_id=object_id,
                object_type=object_type,
                access_control_list=[
                    AccessControlResponse(
                        group_name="de", all_permissions=[Permission(permission_level=PermissionLevel.CAN_USE)]
                    )
                ],
            )
        if object_type == "pipelines":
            return ObjectPermissions(
                object_id=object_id,
                object_type=object_type,
                access_control_list=[
                    AccessControlResponse(
                        group_name="de", all_permissions=[Permission(permission_level=PermissionLevel.CAN_USE)]
                    )
                ],
            )
        if object_type == "jobs":
            return ObjectPermissions(
                object_id=object_id,
                object_type=object_type,
                access_control_list=[
                    AccessControlResponse(
                        group_name="ds", all_permissions=[Permission(permission_level=PermissionLevel.CAN_USE)]
                    ),
                ],
            )
        return None

    ws.permissions.get.side_effect = perms

    sup = GenericPermissionsSupport(
        ws=ws,
        listings=[
            Listing(ws.clusters.list, "cluster_id", "clusters"),
            Listing(ws.jobs.list, "job_id", "jobs"),
            Listing(ws.pipelines.list_pipelines, "pipeline_id", "pipelines"),
        ],
    )
    tasks = []
    for executable in list(sup.get_crawler_tasks()):
        task = executable()
        if task is not None:
            tasks.append(task)
    assert len(tasks) == 1
    assert tasks[0].object_type == "clusters"


def test_verify_task_should_return_true_if_permissions_applied():
    ws = create_autospec(WorkspaceClient)

    acl1 = iam.AccessControlResponse(
        all_permissions=[iam.Permission(permission_level=iam.PermissionLevel.CAN_USE)], group_name="test"
    )
    ws.permissions.get.return_value = iam.ObjectPermissions(access_control_list=[acl1])
    sup = GenericPermissionsSupport(ws=ws, listings=[])  # no listings since only verify is tested

    item = Permissions(
        object_id="test",
        object_type="clusters",
        raw=json.dumps(
            iam.ObjectPermissions(
                object_id="test",
                object_type="clusters",
                access_control_list=[
                    iam.AccessControlResponse(
                        group_name="test",
                        all_permissions=[iam.Permission(inherited=False, permission_level=iam.PermissionLevel.CAN_USE)],
                    )
                ],
            ).as_dict()
        ),
    )

    _task = sup.get_verify_task(item)
    result = _task()
    ws.permissions.get.assert_called_once_with("clusters", "test")

    assert result


def test_verify_task_should_return_false_if_permissions_not_found():
    ws = create_autospec(WorkspaceClient)

    ws.permissions.get.side_effect = NotFound(...)
    sup = GenericPermissionsSupport(ws=ws, listings=[])  # no listings since only verify is tested

    item = Permissions(
        object_id="test",
        object_type="clusters",
        raw=json.dumps(
            iam.ObjectPermissions(
                object_id="test",
                object_type="clusters",
                access_control_list=[
                    iam.AccessControlResponse(
                        group_name="test",
                        all_permissions=[iam.Permission(inherited=False, permission_level=iam.PermissionLevel.CAN_USE)],
                    )
                ],
            ).as_dict()
        ),
    )

    _task = sup.get_verify_task(item)
    result = _task()
    ws.permissions.get.assert_called_once_with("clusters", "test")

    assert not result


def test_verify_task_should_fail_if_permissions_missing():
    ws = create_autospec(WorkspaceClient)

    acl1 = iam.AccessControlResponse(
        all_permissions=[iam.Permission(permission_level=iam.PermissionLevel.CAN_MANAGE)], group_name="test"
    )
    ws.permissions.get.return_value = iam.ObjectPermissions(access_control_list=[acl1])
    sup = GenericPermissionsSupport(ws=ws, listings=[])  # no listings since only verify is tested

    item = Permissions(
        object_id="test",
        object_type="clusters",
        raw=json.dumps(
            iam.ObjectPermissions(
                object_id="test",
                object_type="clusters",
                access_control_list=[
                    iam.AccessControlResponse(
                        group_name="test",
                        all_permissions=[iam.Permission(inherited=False, permission_level=iam.PermissionLevel.CAN_USE)],
                    )
                ],
            ).as_dict()
        ),
    )

    _task = sup.get_verify_task(item)

    with pytest.raises(NotFound):
        _task()


def test_verify_task_should_fail_if_acls_missing():
    ws = create_autospec(WorkspaceClient)

    sup = GenericPermissionsSupport(ws=ws, listings=[])  # no listings since only verify is tested

    item = Permissions(
        object_id="test",
        object_type="clusters",
        raw=json.dumps(
            iam.ObjectPermissions(
                object_id="test",
                object_type="clusters",
                access_control_list=[],
            ).as_dict()
        ),
    )

    with pytest.raises(ValueError):
        sup.get_verify_task(item)
    ws.permissions.update.assert_not_called()


def test_feature_tables_listing():
    ws = create_autospec(WorkspaceClient)

    def do_api_side_effect(*_, query):
        if not query["page_token"]:
            return {"feature_tables": [{"id": "table1"}, {"id": "table2"}], "next_page_token": "token"}
        return {"feature_tables": [{"id": "table3"}, {"id": "table4"}]}

    ws.api_client.do.side_effect = do_api_side_effect

    wrapped = Listing(feature_store_listing(ws), id_attribute="object_id", object_type="feature-tables")
    result = list(wrapped)

    assert len(result) == 4
    assert result[0].object_id == "table1"
    assert result[0].request_type == "feature-tables"


def test_root_page_listing():
    ws = create_autospec(WorkspaceClient)

    basic_acl = [
        iam.AccessControlResponse(
            group_name="test",
            all_permissions=[iam.Permission(inherited=False, permission_level=iam.PermissionLevel.CAN_EDIT_METADATA)],
        )
    ]

    ws.permissions.get.side_effect = [
        iam.ObjectPermissions(object_id="/root", object_type="feature-tables", access_control_list=basic_acl),
    ]

    sup = GenericPermissionsSupport(ws=ws, listings=[Listing(feature_tables_root_page, "object_id", "feature-tables")])
    tasks = list(sup.get_crawler_tasks())
    assert len(tasks) == 1
    auth_items = [task() for task in tasks]
    for item in auth_items:
        assert item.object_id == "/root"
        assert item.object_type == "feature-tables"


def test_models_page_listing():
    ws = create_autospec(WorkspaceClient)

    basic_acl = [
        iam.AccessControlResponse(
            group_name="test",
            all_permissions=[iam.Permission(inherited=False, permission_level=iam.PermissionLevel.CAN_EDIT_METADATA)],
        )
    ]

    ws.permissions.get.side_effect = [
        iam.ObjectPermissions(object_id="/root", object_type="registered-models", access_control_list=basic_acl),
    ]

    sup = GenericPermissionsSupport(ws=ws, listings=[Listing(models_root_page, "object_id", "registered-models")])
    tasks = list(sup.get_crawler_tasks())
    assert len(tasks) == 1
    auth_items = [task() for task in tasks]
    for item in auth_items:
        assert item.object_id == "/root"
        assert item.object_type == "registered-models"

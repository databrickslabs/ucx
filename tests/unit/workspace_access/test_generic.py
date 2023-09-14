import json
from unittest.mock import MagicMock

import pytest
from databricks.sdk.core import DatabricksError
from databricks.sdk.service import compute, iam, ml

from databricks.labs.ucx.workspace_access.generic import (
    GenericPermissionsSupport,
    Permissions,
    RequestObjectType,
    authorization_listing,
    experiments_listing,
    listing_wrapper,
    models_listing,
)


def test_crawler():
    ws = MagicMock()
    ws.clusters.list.return_value = [
        compute.ClusterDetails(
            cluster_id="test",
        )
    ]

    sample_permission = iam.ObjectPermissions(
        object_id="test",
        object_type=str(RequestObjectType.CLUSTERS),
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
            listing_wrapper(ws.clusters.list, "cluster_id", RequestObjectType.CLUSTERS),
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
    assert json.loads(item.raw_object_permissions) == sample_permission.as_dict()


def test_apply(migration_state):
    ws = MagicMock()
    sup = GenericPermissionsSupport(ws=ws, listings=[])  # no listings since only apply is tested

    item = Permissions(
        object_id="test",
        object_type="clusters",
        raw_object_permissions=json.dumps(
            iam.ObjectPermissions(
                object_id="test",
                object_type=str(RequestObjectType.CLUSTERS),
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

    _task = sup.get_apply_task(item, migration_state, "backup")
    _task()
    ws.permissions.update.assert_called_once()

    expected_acl_payload = [
        iam.AccessControlRequest(
            group_name="db-temp-test",
            permission_level=iam.PermissionLevel.CAN_USE,
        )
    ]

    ws.permissions.update.assert_called_with(
        request_object_type=RequestObjectType.CLUSTERS,
        request_object_id="test",
        access_control_list=expected_acl_payload,
    )


def test_relevance():
    sup = GenericPermissionsSupport(ws=MagicMock(), listings=[])  # no listings since only apply is tested
    result = sup.is_item_relevant(
        item=Permissions(object_id="passwords", object_type="passwords", raw_object_permissions="some-stuff"),
        migration_state=MagicMock(),
    )
    assert result is True


def test_safe_get():
    ws = MagicMock()
    ws.permissions.get.side_effect = DatabricksError(error_code="RESOURCE_DOES_NOT_EXIST")
    sup = GenericPermissionsSupport(ws=ws, listings=[])
    result = sup._safe_get_permissions(ws, RequestObjectType.CLUSTERS, "test")
    assert result is None

    ws.permissions.get.side_effect = DatabricksError(error_code="SOMETHING_UNEXPECTED")
    with pytest.raises(DatabricksError):
        sup._safe_get_permissions(ws, RequestObjectType.CLUSTERS, "test")


def test_no_permissions():
    ws = MagicMock()
    ws.clusters.list.return_value = [
        compute.ClusterDetails(
            cluster_id="test",
        )
    ]
    ws.permissions.get.side_effect = DatabricksError(error_code="RESOURCE_DOES_NOT_EXIST")
    sup = GenericPermissionsSupport(
        ws=ws,
        listings=[
            listing_wrapper(ws.clusters.list, "cluster_id", RequestObjectType.CLUSTERS),
        ],
    )
    tasks = list(sup.get_crawler_tasks())
    assert len(tasks) == 1
    ws.clusters.list.assert_called_once()
    _task = tasks[0]
    item = _task()
    assert item is None


def test_passwords_tokens_crawler(migration_state):
    ws = MagicMock()

    basic_acl = [
        iam.AccessControlResponse(
            group_name="test",
            all_permissions=[iam.Permission(inherited=False, permission_level=iam.PermissionLevel.CAN_USE)],
        )
    ]

    ws.permissions.get.side_effect = [
        iam.ObjectPermissions(
            object_id="passwords", object_type=RequestObjectType.AUTHORIZATION, access_control_list=basic_acl
        ),
        iam.ObjectPermissions(
            object_id="tokens", object_type=RequestObjectType.AUTHORIZATION, access_control_list=basic_acl
        ),
    ]

    sup = GenericPermissionsSupport(ws=ws, listings=[authorization_listing()])
    tasks = list(sup.get_crawler_tasks())
    assert len(tasks) == 2
    auth_items = [task() for task in tasks]
    for item in auth_items:
        assert item.object_id in ["tokens", "passwords"]
        assert item.object_type in ["tokens", "passwords"]
        applier = sup.get_apply_task(item, migration_state, "backup")
        new_acl = sup._prepare_new_acl(
            permissions=iam.ObjectPermissions.from_dict(json.loads(item.raw_object_permissions)),
            migration_state=migration_state,
            destination="backup",
        )
        applier()
        ws.permissions.update.assert_called_once_with(
            request_object_type=RequestObjectType.AUTHORIZATION,
            request_object_id=item.object_id,
            access_control_list=new_acl,
        )
        ws.permissions.update.reset_mock()


def test_models_listing():
    ws = MagicMock()
    ws.model_registry.list_models.return_value = [ml.Model(name="test")]
    ws.model_registry.get_model.return_value = ml.GetModelResponse(
        registered_model_databricks=ml.ModelDatabricks(
            id="some-id",
            name="test",
        )
    )

    wrapped = listing_wrapper(models_listing(ws), id_attribute="id", object_type=RequestObjectType.REGISTERED_MODELS)
    result = list(wrapped())
    assert len(result) == 1
    assert result[0].object_id == "some-id"
    assert result[0].request_type == RequestObjectType.REGISTERED_MODELS


def test_experiment_listing():
    ws = MagicMock()
    ws.experiments.list_experiments.return_value = [
        ml.Experiment(experiment_id="test"),
        ml.Experiment(experiment_id="test2", tags=[ml.ExperimentTag(key="whatever", value="SOMETHING")]),
        ml.Experiment(experiment_id="test3", tags=[ml.ExperimentTag(key="mlflow.experimentType", value="NOTEBOOK")]),
        ml.Experiment(
            experiment_id="test4", tags=[ml.ExperimentTag(key="mlflow.experiment.sourceType", value="REPO_NOTEBOOK")]
        ),
    ]
    wrapped = listing_wrapper(
        experiments_listing(ws), id_attribute="experiment_id", object_type=RequestObjectType.EXPERIMENTS
    )
    results = list(wrapped())
    assert len(results) == 2
    for res in results:
        assert res.request_type == RequestObjectType.EXPERIMENTS
        assert res.object_id in ["test", "test2"]

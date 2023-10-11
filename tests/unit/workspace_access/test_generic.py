import json
from unittest.mock import MagicMock

from databricks.sdk.core import DatabricksError
from databricks.sdk.service import compute, iam, ml

from databricks.labs.ucx.workspace_access.generic import (
    GenericPermissionsSupport,
    Listing,
    Permissions,
    experiments_listing,
    models_listing,
    tokens_and_passwords,
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
    ws = MagicMock()
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

    _task = sup.get_apply_task(item, migration_state, "backup")
    _task()
    ws.permissions.update.assert_called_once()

    expected_acl_payload = [
        iam.AccessControlRequest(
            group_name="db-temp-test",
            permission_level=iam.PermissionLevel.CAN_USE,
        )
    ]

    ws.permissions.update.assert_called_with("clusters", "test", access_control_list=expected_acl_payload)


def test_relevance():
    sup = GenericPermissionsSupport(ws=MagicMock(), listings=[])  # no listings since only apply is tested
    result = sup._is_item_relevant(
        item=Permissions(object_id="passwords", object_type="passwords", raw="some-stuff"),
        migration_state=MagicMock(),
    )
    assert result is True


def test_safe_get():
    ws = MagicMock()
    ws.permissions.get.side_effect = DatabricksError(error_code="RESOURCE_DOES_NOT_EXIST")
    sup = GenericPermissionsSupport(ws=ws, listings=[])
    result = sup._safe_get_permissions("clusters", "test")
    assert result is None

    # TODO uncomment after ES-892977 is fixed. The code now is retried.
    # ws.permissions.get.side_effect = DatabricksError(error_code="SOMETHING_UNEXPECTED")
    # with pytest.raises(DatabricksError):
    #     sup._safe_get_permissions("clusters", "test")


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
    ws = MagicMock()

    basic_acl = [
        iam.AccessControlResponse(
            group_name="test",
            all_permissions=[iam.Permission(inherited=False, permission_level=iam.PermissionLevel.CAN_USE)],
        )
    ]

    ws.permissions.get.side_effect = [
        iam.ObjectPermissions(object_id="passwords", object_type="authorization", access_control_list=basic_acl),
        iam.ObjectPermissions(object_id="tokens", object_type="authorization", access_control_list=basic_acl),
    ]

    sup = GenericPermissionsSupport(ws=ws, listings=[Listing(tokens_and_passwords, "object_id", "authorization")])
    tasks = list(sup.get_crawler_tasks())
    assert len(tasks) == 2
    auth_items = [task() for task in tasks]
    for item in auth_items:
        assert item.object_id in ["tokens", "passwords"]
        assert item.object_type == "authorization"
        applier = sup.get_apply_task(item, migration_state, "backup")
        applier()
        ws.permissions.update.assert_called_once_with(
            item.object_type,
            item.object_id,
            access_control_list=[
                iam.AccessControlRequest(group_name="db-temp-test", permission_level=iam.PermissionLevel.CAN_USE)
            ],
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

    wrapped = Listing(models_listing(ws), id_attribute="id", object_type="registered-models")
    result = list(wrapped)
    assert len(result) == 1
    assert result[0].object_id == "some-id"
    assert result[0].request_type == "registered-models"


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
    wrapped = Listing(experiments_listing(ws), id_attribute="experiment_id", object_type="experiments")
    results = list(wrapped)
    assert len(results) == 2
    for res in results:
        assert res.request_type == "experiments"
        assert res.object_id in ["test", "test2"]

import json
import logging

from databricks.sdk.service.ml import (
    ExperimentAccessControlRequest,
    ExperimentPermissionLevel,
    RegisteredModelAccessControlRequest,
    RegisteredModelPermissionLevel,
)

from databricks.labs.ucx.inventory.inventorizer import MlArtifactsInventorizer

_LOG = logging.getLogger(__name__)


def test_ml_inventorizer_should_fetch_permission_for_experiments(make_user, make_group, make_experiment, ws):
    user = make_user()
    group = make_group(display_name="data_engineers", members=[user.id])
    experiment = make_experiment()

    req = ExperimentAccessControlRequest(
        group_name=group.display_name, permission_level=ExperimentPermissionLevel.CAN_EDIT
    )
    req2 = ExperimentAccessControlRequest(
        user_name=user.user_name, permission_level=ExperimentPermissionLevel.CAN_MANAGE
    )
    ws.experiments.set_experiment_permissions(experiment_id=experiment.experiment_id, access_control_list=[req, req2])

    inventorizer = MlArtifactsInventorizer(ws)
    inventorizer.inventorize()
    item = inventorizer._permissions[0]

    assert item.raw_object_permissions == json.dumps(
        ws.experiments.get_experiment_permissions(experiment_id=experiment.experiment_id).as_dict()
    )
    assert item.object_id == experiment.experiment_id


def test_ml_inventorizer_should_fetch_permission_for_models(make_user, make_group, make_model, ws):
    user = make_user()
    group = make_group(display_name="data_engineers", members=[user.id])
    model = make_model()

    model_id = ws.model_registry.get_model(model.name).registered_model_databricks.id
    req = RegisteredModelAccessControlRequest(
        group_name=group.display_name, permission_level=RegisteredModelPermissionLevel.CAN_MANAGE_PRODUCTION_VERSIONS
    )
    ws.model_registry.set_registered_model_permissions(registered_model_id=model_id, access_control_list=[req])

    inventorizer = MlArtifactsInventorizer(ws)
    inventorizer.inventorize()
    item = inventorizer._permissions[0]

    assert item.raw_object_permissions == json.dumps(
        ws.model_registry.get_registered_model_permissions(registered_model_id=model_id).as_dict()
    )
    assert item.object_id == model_id

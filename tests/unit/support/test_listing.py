import datetime as dt
from unittest.mock import MagicMock, patch

from databricks.sdk.service import ml, workspace

from databricks.labs.ucx.inventory.types import RequestObjectType
from databricks.labs.ucx.support.listing import (
    WorkspaceListing,
    experiments_listing,
    logger,
    models_listing,
    workspace_listing,
)
from databricks.labs.ucx.support.permissions import listing_wrapper


def test_logging_calls():
    workspace_listing = WorkspaceListing(ws=MagicMock(), num_threads=1)
    workspace_listing.start_time = dt.datetime.now()
    workspace_listing._counter = 9
    with patch.object(logger, "info") as mock_info:
        workspace_listing._progress_report(None)
        mock_info.assert_called_once()


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


def test_workspace_listing():
    listing = MagicMock(spec=WorkspaceListing)
    listing.walk.return_value = [
        workspace.ObjectInfo(object_id=1, object_type=workspace.ObjectType.NOTEBOOK),
        workspace.ObjectInfo(object_id=2, object_type=workspace.ObjectType.DIRECTORY),
        workspace.ObjectInfo(object_id=3, object_type=workspace.ObjectType.LIBRARY),
        workspace.ObjectInfo(object_id=4, object_type=workspace.ObjectType.REPO),
        workspace.ObjectInfo(object_id=5, object_type=workspace.ObjectType.FILE),
        workspace.ObjectInfo(object_id=6, object_type=None),  # MLflow Experiment
    ]

    with patch("databricks.labs.ucx.support.listing.WorkspaceListing", return_value=listing):
        results = workspace_listing(ws=MagicMock())()
        assert len(list(results)) == 4
        listing.walk.assert_called_once()
        for res in results:
            assert res.request_type in [
                RequestObjectType.NOTEBOOKS,
                RequestObjectType.DIRECTORIES,
                RequestObjectType.REPOS,
                RequestObjectType.FILES,
            ]
            assert res.object_id in [1, 2, 4, 5]

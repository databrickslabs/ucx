import datetime as dt
from unittest.mock import MagicMock, patch

from databricks.sdk.service import ml

from databricks.labs.ucx.inventory.types import RequestObjectType
from databricks.labs.ucx.support.listing import WorkspaceListing, logger, models_listing
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

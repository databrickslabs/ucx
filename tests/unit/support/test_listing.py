import datetime as dt
from unittest.mock import MagicMock, patch

from databricks.labs.ucx.support.listing import WorkspaceListing, logger


def test_listing():
    workspace_listing = WorkspaceListing(ws=MagicMock(), num_threads=1)
    workspace_listing.start_time = dt.datetime.now()
    workspace_listing._counter = 9
    with patch.object(logger, "info") as mock_info:
        workspace_listing._progress_report(None)
        mock_info.assert_called_once()

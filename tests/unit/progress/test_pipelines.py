import json
from unittest.mock import create_autospec

import pytest

from databricks.labs.ucx.assessment.pipelines import PipelineInfo
from databricks.labs.ucx.framework.owners import Ownership
from databricks.labs.ucx.framework.utils import escape_sql_identifier
from databricks.labs.ucx.progress.pipelines import PipelineProgressEncoder


@pytest.mark.parametrize(
    "pipeline_info",
    [
        PipelineInfo("id", 1, failures=""),
    ],
)
def test_cluster_progress_encoder_no_failures(mock_backend, pipeline_info: PipelineInfo) -> None:
    ownership = create_autospec(Ownership)
    ownership.owner_of.return_value = "user"
    encoder = PipelineProgressEncoder(
        mock_backend,
        ownership,
        PipelineInfo,
        run_id=1,
        workspace_id=123456789,
        catalog="test",
    )

    encoder.append_inventory_snapshot([pipeline_info])

    rows = mock_backend.rows_written_for(escape_sql_identifier(encoder.full_name), "append")
    assert len(rows) > 0, f"No rows written for: {encoder.full_name}"
    assert len(rows[0].failures) == 0
    ownership.owner_of.assert_called_once()


@pytest.mark.parametrize(
    "pipeline_info",
    [
        PipelineInfo("id", 1, failures='["using DBFS mount in configuration: /mnt/mount"]'),
    ],
)
def test_cluster_progress_encoder_failures(mock_backend, pipeline_info: PipelineInfo) -> None:
    ownership = create_autospec(Ownership)
    ownership.owner_of.return_value = "user"
    encoder = PipelineProgressEncoder(
        mock_backend,
        ownership,
        PipelineInfo,
        run_id=1,
        workspace_id=123456789,
        catalog="test",
    )

    encoder.append_inventory_snapshot([pipeline_info])

    rows = mock_backend.rows_written_for(escape_sql_identifier(encoder.full_name), "append")
    assert len(rows) > 0, f"No rows written for: {encoder.full_name}"
    assert rows[0].failures == json.loads(pipeline_info.failures)
    ownership.owner_of.assert_called_once()

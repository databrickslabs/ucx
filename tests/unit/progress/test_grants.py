from unittest.mock import create_autospec

import pytest
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.framework.owners import AdministratorLocator
from databricks.labs.ucx.framework.utils import escape_sql_identifier
from databricks.labs.ucx.hive_metastore.grants import Grant, GrantOwnership
from databricks.labs.ucx.progress.grants import GrantsProgressEncoder


@pytest.mark.parametrize(
    "grant, failure",
    [
        (Grant("principal", "DENY"), "Grant without object identifier"),
    ]
)
def test_grants_progress_encoder_failures(mock_backend, grant, failure) -> None:
    ws = create_autospec(WorkspaceClient)
    encoder = GrantsProgressEncoder(
        mock_backend,
        GrantOwnership(AdministratorLocator(ws)),
        Grant,
        run_id=1,
        workspace_id=123456789,
        catalog="test",
    )

    encoder.append_inventory_snapshot([grant])

    rows = mock_backend.rows_written_for(escape_sql_identifier(encoder.full_name), "append")
    assert len(rows) > 0, f"No rows written for: {encoder.full_name}"
    assert rows[0].failures == [failure]

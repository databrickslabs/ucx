from unittest.mock import create_autospec

import pytest

from databricks.labs.ucx.framework.owners import Ownership
from databricks.labs.ucx.framework.utils import escape_sql_identifier
from databricks.labs.ucx.hive_metastore.grants import Grant
from databricks.labs.ucx.progress.grants import GrantProgressEncoder


@pytest.mark.parametrize(
    "grant",
    [
        Grant("principal", "SELECT", "hive_metastore", "schema", udf="function"),
        Grant("principal", "OWN", "hive_metastore", "schema", "table"),
        Grant("principal", "SELECT", "hive_metastore", "schema"),
        Grant("principal", "USAGE", "catalog"),
    ],
)
def test_grants_progress_encoder_no_failures(mock_backend, grant: Grant) -> None:
    ownership = create_autospec(Ownership)
    ownership.owner_of.return_value = "user"
    encoder = GrantProgressEncoder(
        mock_backend,
        ownership,
        Grant,
        run_id=1,
        workspace_id=123456789,
        catalog="test",
    )

    encoder.append_inventory_snapshot([grant])

    rows = mock_backend.rows_written_for(escape_sql_identifier(encoder.full_name), "append")
    assert len(rows) > 0, f"No rows written for: {encoder.full_name}"
    assert len(rows[0].failures) == 0
    ownership.owner_of.assert_called_once()


@pytest.mark.parametrize(
    "grant, failure",
    [
        (
            Grant("principal", "DENY", "hive_metastore", "schema"),
            "Action 'DENY' on DATABASE 'hive_metastore.schema' unmappable to Unity Catalog",
        ),
        (
            Grant("principal", "READ_METADATA", "hive_metastore", "schema", "table"),
            "Action 'READ_METADATA' on TABLE 'hive_metastore.schema.table' unmappable to Unity Catalog",
        ),
    ],
)
def test_grants_progress_encoder_failures(mock_backend, grant: Grant, failure: str) -> None:
    ownership = create_autospec(Ownership)
    ownership.owner_of.return_value = "user"
    encoder = GrantProgressEncoder(
        mock_backend,
        ownership,
        Grant,
        run_id=1,
        workspace_id=123456789,
        catalog="test",
    )

    encoder.append_inventory_snapshot([grant])

    rows = mock_backend.rows_written_for(escape_sql_identifier(encoder.full_name), "append")
    assert len(rows) > 0, f"No rows written for: {encoder.full_name}"
    assert rows[0].failures == [failure]
    ownership.owner_of.assert_called_once()

from unittest.mock import create_autospec

import pytest

from databricks.labs.ucx.framework.owners import Ownership
from databricks.labs.ucx.framework.utils import escape_sql_identifier
from databricks.labs.ucx.progress.queries import QueryProblemProgressEncoder
from databricks.labs.ucx.source_code.queries import QueryProblem


@pytest.mark.parametrize(
    "query_problem, failure",
    [
        (
            QueryProblem(
                dashboard_id="12345",
                dashboard_parent="dashbards/parent",
                dashboard_name="my_dashboard",
                query_id="23456",
                query_parent="queries/parent",
                query_name="my_query",
                code="sql-parse-error",
                message="Could not parse SQL",
            ),
            "[sql-parse-error] Could not parse SQL",
        )
    ],
)
def test_query_problem_progress_encoder_failures(mock_backend, query_problem: QueryProblem, failure: str) -> None:
    """A query problem is a failure by definition as it is a result of linting code for Unity Catalog compatibility."""
    ownership = create_autospec(Ownership)
    ownership.owner_of.return_value = "user"
    encoder = QueryProblemProgressEncoder(
        mock_backend,
        ownership,
        run_id=1,
        workspace_id=123456789,
        catalog="test",
    )

    encoder.append_inventory_snapshot([query_problem])

    rows = mock_backend.rows_written_for(escape_sql_identifier(encoder.full_name), "append")
    assert len(rows) > 0, f"No rows written for: {encoder.full_name}"
    assert rows[0].failures == [failure]
    ownership.owner_of.assert_called_once()

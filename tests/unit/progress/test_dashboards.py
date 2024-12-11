from unittest.mock import create_autospec

from databricks.labs.lsql.backends import MockBackend, Row

from databricks.labs.ucx.assessment.dashboards import Dashboard
from databricks.labs.ucx.framework.owners import Ownership
from databricks.labs.ucx.framework.utils import escape_sql_identifier
from databricks.labs.ucx.progress.dashboards import DashboardProgressEncoder
from databricks.labs.ucx.source_code.directfs_access import DirectFsAccessCrawler
from databricks.labs.ucx.source_code.used_table import UsedTablesCrawler


def test_dashboard_progress_encoder_no_failures() -> None:
    mock_backend = MockBackend(
        rows={
            "SELECT \\* FROM query_problems": [
                Row(
                    dashboard_id="12345",  # Not a match with dashboard below, hence no failure
                    dashboard_parent="dashbards/parent",
                    dashboard_name="my_dashboard",
                    query_id="23456",
                    query_parent="queries/parent",
                    query_name="my_query",
                    code="sql-parse-error",
                    message="Could not parse SQL",
                )
            ]
        }
    )
    ownership = create_autospec(Ownership)
    ownership.owner_of.return_value = "user"
    direct_fs_access_crawler = create_autospec(DirectFsAccessCrawler)
    used_tables_crawler = create_autospec(UsedTablesCrawler)
    encoder = DashboardProgressEncoder(
        mock_backend,
        ownership,
        [direct_fs_access_crawler],
        [used_tables_crawler],
        inventory_database="inventory",
        run_id=1,
        workspace_id=123456789,
        catalog="test",
    )
    dashboard = Dashboard("did")

    encoder.append_inventory_snapshot([dashboard])

    rows = mock_backend.rows_written_for(escape_sql_identifier(encoder.full_name), "append")
    assert rows, f"No rows written for: {encoder.full_name}"
    assert len(rows[0].failures) == 0
    ownership.owner_of.assert_called_once()
    direct_fs_access_crawler.snapshot.assert_called_once()
    used_tables_crawler.snapshot.assert_called_once()


def test_dashboard_progress_encoder_query_problem_as_failure() -> None:
    failures = ["[sql-parse-error] my_query (12345/23456) : Could not parse SQL"]

    mock_backend = MockBackend(
        rows={
            # The Mockbackend.fetch ignores the catalog and schema keyword arguments
            "SELECT \\* FROM query_problems": [
                Row(
                    dashboard_id="12345",
                    dashboard_parent="dashbards/parent",
                    dashboard_name="my_dashboard",
                    query_id="23456",
                    query_parent="queries/parent",
                    query_name="my_query",
                    code="sql-parse-error",
                    message="Could not parse SQL",
                )
            ]
        }
    )
    ownership = create_autospec(Ownership)
    ownership.owner_of.return_value = "user"
    direct_fs_access_crawler = create_autospec(DirectFsAccessCrawler)
    used_tables_crawler = create_autospec(UsedTablesCrawler)
    encoder = DashboardProgressEncoder(
        mock_backend,
        ownership,
        [direct_fs_access_crawler],
        [used_tables_crawler],
        inventory_database="inventory",
        run_id=1,
        workspace_id=123456789,
        catalog="test",
    )
    dashboard = Dashboard("12345")

    encoder.append_inventory_snapshot([dashboard])

    rows = mock_backend.rows_written_for(escape_sql_identifier(encoder.full_name), "append")
    assert rows, f"No rows written for: {encoder.full_name}"
    assert rows[0].failures == failures
    ownership.owner_of.assert_called_once()
    direct_fs_access_crawler.snapshot.assert_called_once()
    used_tables_crawler.snapshot.assert_called_once()

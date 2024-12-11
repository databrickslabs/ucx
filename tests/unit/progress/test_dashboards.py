from unittest.mock import create_autospec

from databricks.labs.ucx.assessment.dashboards import Dashboard
from databricks.labs.ucx.framework.owners import Ownership
from databricks.labs.ucx.framework.utils import escape_sql_identifier
from databricks.labs.ucx.progress.dashboards import DashboardProgressEncoder
from databricks.labs.ucx.source_code.directfs_access import DirectFsAccessCrawler
from databricks.labs.ucx.source_code.used_table import UsedTablesCrawler


def test_dashboard_progress_encoder_no_failures(mock_backend) -> None:
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

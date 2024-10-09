import pytest


@pytest.mark.parametrize("table_name", ["workflow_runs", "historical_records"])
def test_progress_tracking_installer_creates_table(az_cli_ctx, table_name) -> None:
    az_cli_ctx.progress_tracking_installation.run()
    query = (
        f"SELECT 1 FROM tables WHERE table_catalog = '{az_cli_ctx.config.ucx_catalog}' "
        f"AND table_schema = 'multiworkspace' AND table_name = '{table_name}'"
    )
    assert any(az_cli_ctx.sql_backend.fetch(query, catalog="system", schema="information_schema"))

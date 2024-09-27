def test_progress_tracking_installer_creates_workflow_runs_table(az_cli_ctx) -> None:
    az_cli_ctx.progress_tracking_installer.run()
    query = (
        f"SELECT 1 FROM tables WHERE table_catalog = '{az_cli_ctx.config.ucx_catalog}' "
        "AND table_schema = 'multiworkspace' AND table_name = 'workflow_runs'"
    )
    assert any(az_cli_ctx.sql_backend.fetch(query, catalog="system", schema="information_schema"))

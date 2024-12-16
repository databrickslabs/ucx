from databricks.labs.ucx.assessment.dashboards import Dashboard
from databricks.labs.ucx.framework.utils import escape_sql_identifier


def test_dashboard_progress_encoder_table_no_failures(runtime_ctx, az_cli_ctx) -> None:
    az_cli_ctx.progress_tracking_installation.run()
    runtime_ctx = runtime_ctx.replace(
        parent_run_id=1,
        sql_backend=az_cli_ctx.sql_backend,
        ucx_catalog=az_cli_ctx.ucx_catalog,
    )
    sdk_dashboard = runtime_ctx.make_dashboard()
    dashboard = Dashboard.from_sdk_redash_dashboard(sdk_dashboard)
    runtime_ctx.query_linter.refresh_report()

    runtime_ctx.dashboards_progress.append_inventory_snapshot([dashboard])

    history_table_name = escape_sql_identifier(runtime_ctx.tables_progress.full_name)
    records = list(runtime_ctx.sql_backend.fetch(f"SELECT * FROM {history_table_name}"))

    assert len(records) == 1, "Expected one historical record"
    assert records[0].failures == []

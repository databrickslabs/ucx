from databricks.labs.lsql.backends import Row

from databricks.labs.ucx.__about__ import __version__ as ucx_version
from databricks.labs.ucx.framework.utils import escape_sql_identifier


def test_dashboard_progress_encoder_table_failures(runtime_ctx, az_cli_ctx) -> None:
    row = Row(
        workspace_id=123456789,
        job_run_id=1,
        object_type="Dashboard",
        object_id=["did1"],
        data={"id": "did1"},
        failures=[],
        owner="cor",
        ucx_version=ucx_version,
    )
    az_cli_ctx.progress_tracking_installation.run()
    runtime_ctx = runtime_ctx.replace(
        parent_run_id=1,
        sql_backend=az_cli_ctx.sql_backend,
        ucx_catalog=az_cli_ctx.ucx_catalog,
    )
    dashboard = az_cli_ctx.make_dashboard()

    runtime_ctx.tables_progress.append_inventory_snapshot([dashboard])

    history_table_name = escape_sql_identifier(runtime_ctx.tables_progress.full_name)
    records = list(runtime_ctx.sql_backend.fetch(f"SELECT * FROM {history_table_name}"))

    assert records == [row]

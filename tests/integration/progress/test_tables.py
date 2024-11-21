from databricks.labs.ucx.hive_metastore.tables import Table
from databricks.labs.ucx.framework.utils import escape_sql_identifier


def test_table_progress_encoder_table_pending_migration(runtime_ctx, az_cli_ctx) -> None:
    az_cli_ctx.progress_tracking_installation.run()
    runtime_ctx = runtime_ctx.replace(
        parent_run_id=1,
        sql_backend=az_cli_ctx.sql_backend,
        ucx_catalog=az_cli_ctx.ucx_catalog,
    )
    table = Table("hive_metastore", "schema", "name", "UNKNOWN", "UNKNOWN")
    runtime_ctx.tables_progress.append_inventory_snapshot([table])

    history_table_name = escape_sql_identifier(runtime_ctx.tables_progress.full_name)
    records = list(runtime_ctx.sql_backend.fetch(f"SELECT * FROM {history_table_name}"))

    assert len(records) == 1
    assert records[0].failures == ["Pending migration"]

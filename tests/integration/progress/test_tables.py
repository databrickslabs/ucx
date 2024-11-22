import datetime as dt

from databricks.labs.ucx.framework.utils import escape_sql_identifier
from databricks.labs.ucx.hive_metastore.tables import Table
from databricks.labs.ucx.source_code.base import LineageAtom, UsedTable


def test_table_progress_encoder_table_failures(runtime_ctx, az_cli_ctx) -> None:
    az_cli_ctx.progress_tracking_installation.run()
    runtime_ctx = runtime_ctx.replace(
        parent_run_id=1,
        sql_backend=az_cli_ctx.sql_backend,
        ucx_catalog=az_cli_ctx.ucx_catalog,
    )
    table_info = runtime_ctx.make_table()
    table = Table(
        table_info.catalog_name,
        table_info.schema_name,
        table_info.name,
        table_info.table_type.value,
        table_info.data_source_format.value,
    )
    used_table = UsedTable(
        catalog_name=table.catalog,
        schema_name=table.database,
        table_name=table.name,
        source_id="test/test.py",
        source_timestamp=dt.datetime.now(tz=dt.timezone.utc),
        source_lineage=[LineageAtom(object_type="NOTEBOOK", object_id="test/test.py")],
        assessment_start_timestamp=dt.datetime.now(tz=dt.timezone.utc),
        assessment_end_timestamp=dt.datetime.now(tz=dt.timezone.utc),
    )
    runtime_ctx.used_tables_crawler_for_paths.dump_all([used_table])

    runtime_ctx.tables_progress.append_inventory_snapshot([table])

    history_table_name = escape_sql_identifier(runtime_ctx.tables_progress.full_name)
    records = list(runtime_ctx.sql_backend.fetch(f"SELECT * FROM {history_table_name}"))

    assert len(records) == 1
    assert records[0].failures == ["Pending migration", "Used by NOTEBOOK: test/test.py"]

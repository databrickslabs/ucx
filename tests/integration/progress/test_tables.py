import datetime as dt

import pytest

from databricks.labs.ucx.framework.utils import escape_sql_identifier
from databricks.labs.ucx.hive_metastore.tables import Table
from databricks.labs.ucx.source_code.base import LineageAtom, UsedTable


@pytest.mark.parametrize("is_migrated_table", [True, False])
@pytest.mark.parametrize("is_used_table", [True, False])
def test_table_progress_encoder_table_failures(
    runtime_ctx,
    az_cli_ctx,
    make_catalog,
    is_migrated_table: bool,
    is_used_table: bool,
) -> None:
    failures = []
    if not is_migrated_table:
        failures.append("Pending migration")
    if is_used_table:
        failures.append("Used by NOTEBOOK: test/test.py")

    az_cli_ctx.progress_tracking_installation.run()
    runtime_ctx = runtime_ctx.replace(
        parent_run_id=1,
        sql_backend=az_cli_ctx.sql_backend,
        ucx_catalog=az_cli_ctx.ucx_catalog,
    )
    # To set both the `upgraded_to` and `upgraded_from` table property values during table creation is not possible
    # The below works because the `upgraded_to` value is not used for matching, the property only needs to be present
    hive_tbl_properties = {"upgraded_to": "upgraded_to.name_does.not_matter"} if is_migrated_table else {}
    hive_table_info = runtime_ctx.make_table(tbl_properties=hive_tbl_properties)
    uc_tbl_properties = {"upgraded_from": hive_table_info.full_name} if is_migrated_table else {}
    runtime_ctx.make_table(catalog_name=make_catalog().name, tbl_properties=uc_tbl_properties)
    hive_used_table = UsedTable(
        catalog_name="hive_metastore" if is_used_table else "catalog",
        schema_name=hive_table_info.schema_name,
        table_name=hive_table_info.name,
        source_id="test/test.py",
        source_timestamp=dt.datetime.now(tz=dt.timezone.utc),
        source_lineage=[LineageAtom(object_type="NOTEBOOK", object_id="test/test.py")],
        assessment_start_timestamp=dt.datetime.now(tz=dt.timezone.utc),
        assessment_end_timestamp=dt.datetime.now(tz=dt.timezone.utc),
    )
    runtime_ctx.used_tables_crawler_for_paths.dump_all([hive_used_table])

    hive_table = Table(
        hive_table_info.catalog_name,
        hive_table_info.schema_name,
        hive_table_info.name,
        hive_table_info.table_type.value,
        hive_table_info.data_source_format.value,
    )
    runtime_ctx.tables_progress.append_inventory_snapshot([hive_table])

    history_table_name = escape_sql_identifier(runtime_ctx.tables_progress.full_name)
    records = list(runtime_ctx.sql_backend.fetch(f"SELECT * FROM {history_table_name}"))

    assert len(records) == 1, "Expected one historical entry"
    assert records[0].failures == failures

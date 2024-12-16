import datetime as dt
import pytest

from databricks.labs.ucx.assessment.dashboards import Dashboard
from databricks.labs.ucx.framework.utils import escape_sql_identifier
from databricks.labs.ucx.hive_metastore.tables import Table


@pytest.mark.parametrize(
    "query, failures",
    [
        ("SELECT 1", []),
        (
            "SELECT * FROM csv.`dbfs://some_folder/some_file.csv`",
            [
                "[direct-filesystem-access-in-sql-query] {query_name} ({dashboard_id}/{query_id}) : The use of direct filesystem references is deprecated: dbfs://some_folder/some_file.csv"
            ],
        ),
        (
            "SELECT * FROM {table_name}",
            ["Pending migration: {table_name}"],
        ),
    ],
)
def test_dashboard_progress_encoder_table_failures(runtime_ctx, az_cli_ctx, query: str, failures: list[str]) -> None:
    az_cli_ctx.progress_tracking_installation.run()
    runtime_ctx = runtime_ctx.replace(
        parent_run_id=1,
        named_parameters={
            "workflow": "migration-progress-experimental",
            "job_id": "2",
            "start_time": dt.datetime.now(tz=dt.timezone.utc).replace(microsecond=0).isoformat(),
        },
        sql_backend=az_cli_ctx.sql_backend,
        ucx_catalog=az_cli_ctx.ucx_catalog,
    )
    table_info = runtime_ctx.make_table()
    legacy_query = runtime_ctx.make_query(sql_query=query.format(table_name=table_info.full_name))
    sdk_dashboard = runtime_ctx.make_dashboard(query=legacy_query)
    dashboard = Dashboard.from_sdk_redash_dashboard(sdk_dashboard)

    # Below is a minimal subset of the migration progress workflow tasks
    runtime_ctx.query_linter.refresh_report()
    runtime_ctx.tables_progress.append_inventory_snapshot([Table.from_table_info(table_info)])
    runtime_ctx.workflow_run_recorder.record()

    runtime_ctx.dashboards_progress.append_inventory_snapshot([dashboard])

    history_table_name = escape_sql_identifier(runtime_ctx.tables_progress.full_name)
    records = list(runtime_ctx.sql_backend.fetch(f"SELECT * FROM {history_table_name} WHERE object_type = 'Dashboard'"))

    assert len(records) == 1, "Expected one historical dashboard record"

    expected = []
    for failure in failures:
        message = failure.format(
            query_id=legacy_query.id,
            query_name=legacy_query.name,
            dashboard_id=dashboard.id,
            table_name=table_info.full_name,
        )
        expected.append(message)
    assert records[-1].failures == expected

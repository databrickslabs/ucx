import datetime as dt


def test_workflow_run_recorder_records_workflow_run(az_cli_ctx, runtime_ctx) -> None:
    az_cli_ctx.progress_tracking_installer.run()
    query = f"SELECT * FROM {az_cli_ctx.ucx_catalog}.multiworkspace.workflow_runs"
    assert not any(az_cli_ctx.sql_backend.fetch(query))

    start_time = dt.datetime.now(tz=dt.timezone.utc).replace(microsecond=0)
    named_parameters = {
        "workspace_id": 123456789,
        "workflow": "test",
        "job_id": "123",
        "parent_run_id": "456",
        "workflow_start_time": start_time.isoformat(),
    }
    ctx = runtime_ctx.replace(named_parameters=named_parameters, ucx_catalog=az_cli_ctx.ucx_catalog)
    ctx.workflow_run_recorder.record()

    rows = list(az_cli_ctx.sql_backend.fetch(query))
    assert len(rows) == 1
    assert rows[0].started_at == start_time
    assert start_time < rows[0].finished_at < dt.datetime.now(tz=dt.timezone.utc)
    assert rows[0].workspace_id == 123456789
    assert rows[0].workflow_name == "test"
    assert rows[0].workflow_id == 123
    assert rows[0].workflow_run_id == 456
    assert rows[0].workflow_run_attempt == 0

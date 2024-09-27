def test_workflow_run_recorder_records_workflow_run(az_cli_ctx, runtime_ctx) -> None:
    az_cli_ctx.progress_tracking_installer.run()
    query = f"SELECT 1 FROM {az_cli_ctx.ucx_catalog}.multiworkspace.workflow_runs"
    assert not any(az_cli_ctx.sql_backend.fetch(query))

    named_parameters = {
        "job_id": "123",
        "parent_run_id": "456",
        "workflow_start_time": "2024-10-11T01:42:02.000000",
    }
    ctx = runtime_ctx.replace(named_parameters=named_parameters, ucx_catalog=az_cli_ctx.ucx_catalog)
    ctx.workflow_run_recorder.record(workflow_name="test")

    assert any(az_cli_ctx.sql_backend.fetch(query))

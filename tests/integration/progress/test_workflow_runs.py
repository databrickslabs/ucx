import datetime as dt


def test_workflow_run_recorder_records_workflow_run(installation_ctx) -> None:
    """Ensure that the workflow run recorder records a workflow run"""
    start_time = dt.datetime.now(tz=dt.timezone.utc).replace(microsecond=0)
    named_parameters = {
        "workspace_id": 123456789,
        "workflow": "test",
        "job_id": "123",
        "parent_run_id": "456",
        "start_time": start_time.isoformat(),
    }
    ctx = installation_ctx.replace(named_parameters=named_parameters)
    ctx.progress_tracking_installation.run()
    select_workflow_runs_query = f"SELECT * FROM {ctx.ucx_catalog}.multiworkspace.workflow_runs"
    # Be confident that we are not selecting any workflow runs before the to-be-tested code
    assert not any(ctx.sql_backend.fetch(select_workflow_runs_query))

    ctx.workflow_run_recorder.record()

    rows = list(ctx.sql_backend.fetch(select_workflow_runs_query))
    assert len(rows) == 1
    assert rows[0].started_at == start_time
    assert start_time <= rows[0].finished_at <= dt.datetime.now(tz=dt.timezone.utc)
    assert rows[0].workspace_id == 123456789
    assert rows[0].workflow_name == "test"
    assert rows[0].workflow_id == 123
    assert rows[0].workflow_run_id == 456
    assert rows[0].workflow_run_attempt == 0

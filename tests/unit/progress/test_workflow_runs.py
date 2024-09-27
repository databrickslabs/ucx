import datetime as dt

from databricks.labs.ucx.progress.workflow_runs import WorkflowRunRecorder


def test_workflow_run_record_records_workflow_run(mock_backend) -> None:
    """Ensure that the workflow run recorder records a workflow run"""
    start_time = dt.datetime.now(tz=dt.timezone.utc).replace(microsecond=0)
    workflow_run_recorder = WorkflowRunRecorder(
        mock_backend,
        ucx_catalog="ucx",
        workspace_id=123456789,
        workflow_name="workflow",
        workflow_id=123,
        workflow_run_id=456,
        workflow_run_attempt=0,
        workflow_start_time=start_time.isoformat(),
    )

    workflow_run_recorder.record()

    rows = mock_backend.rows_written_for("ucx.multiworkspace.workflow_runs", "append")
    assert len(rows) == 1
    assert rows[0].started_at == start_time
    assert start_time <= rows[0].finished_at <= dt.datetime.now(tz=dt.timezone.utc)
    assert rows[0].workspace_id == 123456789
    assert rows[0].workflow_name == "workflow"
    assert rows[0].workflow_id == 123
    assert rows[0].workflow_run_id == 456
    assert rows[0].workflow_run_attempt == 0


from unittest.mock import create_autospec

from databricks.labs.lsql import Row
from databricks.labs.lsql.backends import MockBackend

from databricks.labs.ucx.assessment.jobs import JobOwnership, JobInfo
from databricks.labs.ucx.progress.jobs import JobsProgressEncoder
from databricks.labs.ucx import __version__


def test_jobs_progress_encoder() -> None:
    common = {
        'message': 'some failure',
        'job_name': 'job_name',
        'start_line': 1,
        'start_col': 2,
        'end_line': 3,
        'end_col': 4,
    }
    sql_backend = MockBackend(
        rows={
            "workflow_problems": [
                Row(job_id=1, code="cannot-autofix-table-reference", task_key="a", path="/some/path", **common),
                Row(job_id=1, code="catalog-api-in-shared-clusters", task_key="b", path="/some/other", **common),
                Row(job_id=2, code="catalog-api-in-shared-clusters", task_key="c", path="/x", **common),
            ],
        }
    )
    job_ownership = create_autospec(JobOwnership)
    job_ownership.owner_of.return_value = "some_owner"
    jobs_progress_encoder = JobsProgressEncoder(
        sql_backend,
        job_ownership,
        "inventory",
        2,
        3,
        "ucx",
    )

    jobs_progress_encoder.append_inventory_snapshot(
        [
            JobInfo(
                job_id='1',
                success=0,
                failures='["some failure from config"]',
            )
        ]
    )

    rows = sql_backend.rows_written_for('`ucx`.`multiworkspace`.`historical`', 'append')
    assert rows == [
        Row(
            workspace_id=3,
            job_run_id=2,
            object_type='JobInfo',
            object_id=['1'],
            data={'job_id': '1', 'success': '0'},
            failures=[
                'some failure from config',
                'cannot-autofix-table-reference: a task: /some/path: some failure',
                'catalog-api-in-shared-clusters: b task: /some/other: some failure',
            ],
            owner='some_owner',
            ucx_version=__version__,
        )
    ]

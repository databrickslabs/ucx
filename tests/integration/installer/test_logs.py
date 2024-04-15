from pathlib import Path

import pytest
from databricks.labs.lsql.backends import StatementExecutionBackend

from databricks.labs.ucx.installer import logs


WORKFLOW = "assessment"
JOB_ID = 123
JOB_RUN_ID = 456
WARNING_MESSAGE = "Missing permissions for grants"
LOG = f"""
12:33:01 INFO [databricks.labs.ucx] {{MainThread}} UCX v0.21.1 After job finishes, see debug logs at /Workspace/Users/user.name@databricks.com/.installation/logs/assessment/run-766/crawl_grants.log
12:33:01 DEBUG [databricks.labs.ucx.framework.crawlers] {{MainThread}} [hive_metastore.ucx_si10.grants] fetching grants inventory
12:33:01 DEBUG [databricks.labs.lsql.backends] {{MainThread}} [spark][fetch] SELECT * FROM hive_metastore.ucx_si10.grants
12:33:02 DEBUG [databricks.labs.ucx.framework.crawlers] {{MainThread}} [hive_metastore.ucx_si10t.grants] crawling new batch for grants
12:33:02 WARNING [databricks.labs.ucx.framework.crawlers] {{MainThread}} {WARNING_MESSAGE}
""".lstrip()


@pytest.fixture
def log_file(tmp_path: Path) -> Path:
    log_file_path = tmp_path / "logs" / WORKFLOW / f"run-{JOB_RUN_ID}-0" / "crawl_grants.log"
    log_file_path.parent.mkdir(parents=True, exist_ok=True)
    with log_file_path.open("w") as f:
        f.write(LOG)
    return log_file_path


def test_log_recorder_record_number_of_records(
    tmp_path: Path,
    log_file: Path,
    sql_backend: StatementExecutionBackend,
    inventory_schema: str,
) -> None:
    _ = log_file
    log_recorder = logs.TaskRunWarningRecorder(
        tmp_path,
        workflow=WORKFLOW,
        job_id=JOB_ID,
        job_run_id=JOB_RUN_ID,
        sql_backend=sql_backend,
        schema=inventory_schema,
    )
    log_recorder.snapshot()

    row = next(sql_backend.fetch(f"SELECT COUNT(*) AS value FROM {log_recorder.full_name}"))
    assert row.value == 1

    row = next(sql_backend.fetch(f"SELECT level, message FROM {log_recorder.full_name}"))
    assert row.level == "WARNING"
    assert row.message == WARNING_MESSAGE

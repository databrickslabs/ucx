import datetime as dt
import logging
import pytest
from pathlib import Path

from databricks.labs.lsql.backends import StatementExecutionBackend

from databricks.labs.ucx.installer import logs


LOG = """
12:33:01 INFO [databricks.labs.ucx] {MainThread} UCX v0.21.1 After job finishes, see debug logs at /Workspace/Users/user.name@databricks.com/.installation/logs/assessment/run-766/crawl_grants.log
12:33:01 DEBUG [databricks.labs.ucx.framework.crawlers] {MainThread} [hive_metastore.ucx_si10.grants] fetching grants inventory
12:33:01 DEBUG [databricks.labs.lsql.backends] {MainThread} [spark][fetch] SELECT * FROM hive_metastore.ucx_si10.grants
12:33:02 DEBUG [databricks.labs.ucx.framework.crawlers] {MainThread} [hive_metastore.ucx_si10t.grants] crawling new batch for grants
""".lstrip()


@pytest.fixture
def log_file(tmp_path: Path) -> Path:
    log_file_path = tmp_path / "crawl_grants.log"
    with log_file_path.open("w") as f:
        f.write(LOG)
    return log_file_path


def test_log_recorder_record_number_of_records(
    log_file: Path,
    sql_backend: StatementExecutionBackend,
    inventory_schema: str,
) -> None:
    log_recorder = logs.TaskRunWarningRecorder(
        log_file.parent,
        workflow="assessment",
        job_id=123,
        job_run_id=456,
        sql_backend=sql_backend,
        schema=inventory_schema,
        minimum_log_level=logging.DEBUG,
    )

    with log_file.open("r") as f:
        log_recorder.record(
            log_file.stem,
            f,
            dt.datetime.now(),
        )

    row = next(sql_backend.fetch(f"SELECT COUNT(*) AS value FROM {log_recorder.full_name}"))
    assert row.value == 4

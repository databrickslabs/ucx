from databricks.labs.lsql.backends import MockBackend

from databricks.labs.ucx.source_code.base import DirectFsAccessInPath
from databricks.labs.ucx.source_code.directfs_access_crawler import DirectFsAccessCrawlers


def test_crawler_appends_dfsas():
    backend = MockBackend()
    crawler = DirectFsAccessCrawlers(backend, "schema").for_paths()
    dfsas = list(
        DirectFsAccessInPath(
            path=path,
            is_read=False,
            is_write=False,
            source_id="ID",
            source_timestamp=7452,
            source_lineage="LINEAGE",
            assessment_start_timestamp=123,
            assessment_end_timestamp=234,
            job_id=222,
            job_name="JOB",
            task_key="TASK",
        )
        for path in ("a", "b", "c")
    )
    crawler.append(dfsas)
    rows = backend.rows_written_for(crawler.full_name, "append")
    assert len(rows) == 3

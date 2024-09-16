from datetime import datetime

from databricks.labs.lsql.backends import MockBackend

from databricks.labs.ucx.source_code.directfs_access import DirectFsAccess, DirectFsAccessCrawlers, LineageAtom


def test_crawler_appends_dfsas():
    backend = MockBackend()
    crawler = DirectFsAccessCrawlers(backend, "schema").for_paths()
    dfsas = list(
        DirectFsAccess(
            path=path,
            is_read=False,
            is_write=False,
            source_id="ID",
            source_timestamp=datetime.now(),
            source_lineage=[LineageAtom(object_type="LINEAGE", object_id="ID")],
            job_id=222,
            job_name="JOB",
            task_key="TASK",
            assessment_start_timestamp=datetime.now(),
            assessment_end_timestamp=datetime.now(),
        )
        for path in ("a", "b", "c")
    )
    crawler.dump_all(dfsas)
    rows = backend.rows_written_for(crawler.full_name, "append")
    assert len(rows) == 3

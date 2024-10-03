from datetime import datetime

from databricks.labs.lsql.backends import MockBackend

from databricks.labs.ucx.source_code.base import LineageAtom
from databricks.labs.ucx.source_code.directfs_access import (
    DirectFsAccessCrawler,
    DirectFsAccess,
)


def test_crawler_appends_dfsas(ws):
    backend = MockBackend()
    crawler = DirectFsAccessCrawler.for_paths(ws, backend, "schema")
    existing = list(crawler.snapshot())
    assert not existing
    dfsas = list(
        DirectFsAccess(
            path=path,
            is_read=False,
            is_write=False,
            source_id="ID",
            source_timestamp=datetime.now(),
            source_lineage=[LineageAtom(object_type="LINEAGE", object_id="ID")],
            assessment_start_timestamp=datetime.now(),
            assessment_end_timestamp=datetime.now(),
        )
        for path in ("a", "b", "c")
    )
    crawler.dump_all(dfsas)
    rows = backend.rows_written_for(crawler.full_name, "append")
    assert len(rows) == 3

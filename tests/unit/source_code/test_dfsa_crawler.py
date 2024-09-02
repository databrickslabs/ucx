from databricks.labs.lsql.backends import MockBackend

from databricks.labs.ucx.source_code.base import DFSA


def test_crawler_appends_dfsas():
    backend = MockBackend()
    crawler = DfsaCrawler(backend, "schema")
    for path in ("a", "b", "c"):
        dfsa = DFSA(source_type="SOURCE", source_id="ID", path=path, is_read=False, is_write=False)
        crawler.append(dfsa)
    rows = backend.rows_written_for(crawler.full_name, "append")
    assert len(rows) == 3


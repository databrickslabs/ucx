from datetime import datetime

from databricks.labs.lsql.backends import MockBackend

from databricks.labs.ucx.source_code.base import LineageAtom, UsedTable
from databricks.labs.ucx.source_code.used_table import UsedTablesCrawler


def test_crawler_appends_tables() -> None:
    backend = MockBackend()
    crawler = UsedTablesCrawler.for_paths(backend, "schema")
    existing = list(crawler.snapshot())
    assert not existing
    dfsas = list(
        UsedTable(
            catalog_name="catalog",
            schema_name="schema",
            table_name=name,
            source_timestamp=datetime.now(),
            source_lineage=[LineageAtom(object_type="LINEAGE", object_id="ID")],
            assessment_start_timestamp=datetime.now(),
            assessment_end_timestamp=datetime.now(),
        )
        for name in ("a", "b", "c")
    )
    crawler.dump_all(dfsas)
    rows = backend.rows_written_for(crawler.full_name, "append")
    assert len(rows) == 3

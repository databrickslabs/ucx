import pytest
from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.source_code.linters.context import LinterContext


@pytest.mark.parametrize(
    'code,expected',
    [
        ('spark.table("a.b").count()', {'r:a.b'}),
        ('spark.getTable("a.b")', {'r:a.b'}),
        ('spark.cacheTable("a.b")', {'r:a.b'}),
        ('spark.range(10).saveAsTable("a.b")', {'w:a.b'}),
        ('spark.sql("SELECT * FROM b.c LEFT JOIN c.d USING (e)")', {'r:b.c', 'r:c.d'}),
        ('spark.sql("SELECT * FROM delta.`/foo/bar`")', set()),
    ],
)
def test_collector_walker_from_python(code, expected, migration_index) -> None:
    used = set()
    ctx = LinterContext(migration_index)
    collector = ctx.tables_collector(Language.PYTHON)
    for used_table in collector.collect_tables(code):
        prefix = 'r' if used_table.is_read else 'w'
        used.add(f'{prefix}:{used_table.schema_name}.{used_table.table_name}')
    assert used == expected

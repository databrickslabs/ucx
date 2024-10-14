from databricks.labs.ucx.source_code.sql.sql_parser import SqlParser


def test_does_not_raise_exception_with_unsupported_sql() -> None:
    assert len(list(SqlParser.walk_expressions("XSELECT * from nowhere", lambda _: []))) == 0

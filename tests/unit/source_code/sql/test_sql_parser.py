import pytest
from sqlglot import ParseError

from databricks.labs.ucx.source_code.sql.sql_parser import SqlParser


def test_raises_exception_with_unsupported_sql():
    with pytest.raises(ParseError):
        list(SqlParser.walk_expressions("XSELECT * from nowhere", lambda _: []))

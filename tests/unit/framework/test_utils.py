import pytest

from databricks.labs.ucx.framework.utils import escape_sql_identifier


@pytest.mark.parametrize(
    "path,expected",
    [
        ("a", "`a`"),
        ("a.b", "`a`.`b`"),
        ("a.b.c", "`a`.`b`.`c`"),
        ("`a`.b.c", "`a`.`b`.`c`"),
        ("a.`b`.c", "`a`.`b`.`c`"),
        ("a.b.`c`", "`a`.`b`.`c`"),
        ("`a.b`.c", "`a`.`b`.`c`"),
        ("a.`b.c`", "`a`.`b`.`c`"),
        ("`a.b`.`c`", "`a`.`b`.`c`"),
        ("`a`.`b.c`", "`a`.`b`.`c`"),
        ("`a`.`b`.`c`", "`a`.`b`.`c`"),
        ("a.b.c.d", "`a`.`b`.`c.d`"),
        ("a-b.c.d", "`a-b`.`c`.`d`"),
        ("a.b-c.d", "`a`.`b-c`.`d`"),
        ("a.b.c-d", "`a`.`b`.`c-d`"),
        ("a.b.c`d", "`a`.`b`.`c``d`"),
        ("✨.🍰.✨", "`✨`.`🍰`.`✨`"),
        ("", ""),
    ],
)
def test_escaped_path(path: str, expected: str) -> None:
    assert escape_sql_identifier(path) == expected


def test_escaped_when_column_contains_period() -> None:
    expected = "`column.with.periods`"
    path = "column.with.periods"
    assert escape_sql_identifier(path, maxsplit=0) == expected

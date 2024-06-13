from __future__ import annotations

import pytest

from databricks.labs.ucx.source_code.base import Advice
from databricks.labs.ucx.source_code.linters.table_creation import DBRv8d0Linter


METHOD_NAMES = [
    "writeTo",
    "table",
    "saveAsTable",
]
ASSIGN = [True, False]
DBR_VERSIONS = [  # version as tuple of ints: (major, minor)
    {
        "version": None,
        "suppress": False,
    },
    {
        "version": (7, 9),
        "suppress": False,
    },
    {
        "version": (8, 0),
        "suppress": True,
    },
    {
        "version": (9, 0),
        "suppress": True,
    },
]


def get_code(assign: bool, stmt: str) -> str:
    """Return code snippet to be linted, with customizable statement"""
    assign_str = 'df = ' if assign else ''
    return f"""
spark.read.csv("s3://bucket/path")
for i in range(10):
    {assign_str}{stmt}
    do_stuff_with_df(df)
"""


def get_advice(assign: bool, method_name: str, args_len: int) -> Advice:
    """Repeated boilerplate Advice constructing"""
    return Advice(
        code="table-migrate",
        message="The default format changed in Databricks Runtime 8.0, from Parquet to Delta",
        start_line=3,
        start_col=(9 if assign else 4),
        end_line=3,
        end_col=(29 if assign else 24) + len(method_name) + args_len,
    )


def lint(
    code: str,
    dbr_version: tuple[int, int] | None = (7, 9),
) -> list[Advice]:
    """Invoke linting for the given dbr version"""
    return list(DBRv8d0Linter(dbr_version).lint(code))


@pytest.mark.parametrize("method_name", METHOD_NAMES)
@pytest.mark.parametrize("assign", ASSIGN)
def test_has_format_call(migration_index, method_name, assign):
    """Tests that calling "format" doesn't yield advice"""
    old_code = get_code(assign, f'spark.foo().format("delta").bar().{method_name}("catalog.db.table").baz()')
    assert not lint(old_code)


@pytest.mark.parametrize("method_name", METHOD_NAMES)
@pytest.mark.parametrize("assign", ASSIGN)
def test_no_format(migration_index, method_name, assign):
    """Tests that not setting "format" yields advice (both in assignment or standalone callchain)"""
    old_code = get_code(assign, f'spark.foo().bar().{method_name}("catalog.db.table").baz()')
    assert [get_advice(assign, method_name, 18)] == lint(old_code)


@pytest.mark.parametrize(
    "params",
    [
        {"stmt": 'spark.foo().bar().table().baz()', "expected": False},
        {"stmt": 'spark.foo().bar().table("catalog.db.table").baz()', "expected": True},
        {"stmt": 'spark.foo().bar().table("catalog.db.table", "xyz").baz()', "expected": False},
        {"stmt": 'spark.foo().bar().table("catalog.db.table", fmt="xyz").baz()', "expected": False},
    ],
)
def test_no_format_args_count(migration_index, params):
    """Tests that the number of arguments to table creation call is considered in matching"""
    old_code = get_code(False, params["stmt"])
    assert (not params["expected"]) == (not lint(old_code))


@pytest.mark.parametrize("assign", ASSIGN)
def test_has_format_arg(migration_index, assign):
    """Tests that setting "format" positional arg doesn't yield advice"""
    old_code = get_code(assign, 'spark.foo().format("delta").bar().saveAsTable("catalog.db.table", "csv").baz()')
    assert not lint(old_code)


@pytest.mark.parametrize("assign", ASSIGN)
def test_has_format_kwarg(migration_index, assign):
    """Tests that setting "format" kwarg doesn't yield advice"""
    old_code = get_code(assign, 'spark.foo().format("delta").bar().saveAsTable("catalog.db.table", format="csv").baz()')
    assert not lint(old_code)


@pytest.mark.parametrize("assign", ASSIGN)
def test_has_format_arg_none(migration_index, assign):
    """Tests that explicitly setting "format" parameter to None yields advice"""
    old_code = get_code(assign, 'spark.foo().bar().saveAsTable("catalog.db.table", format=None).baz()')
    assert [get_advice(assign, "saveAsTable", 31)] == lint(old_code)


@pytest.mark.parametrize("dbr_version", DBR_VERSIONS)
def test_dbr_version_filter(migration_index, dbr_version):
    """Tests the DBR version cutoff filter"""
    old_code = get_code(False, 'spark.foo().bar().table("catalog.db.table").baz()')
    expected = [] if dbr_version["suppress"] else [get_advice(False, 'table', 18)]
    actual = lint(old_code, dbr_version["version"])
    assert actual == expected

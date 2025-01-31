import dataclasses
from pathlib import Path

import pytest

from databricks.labs.ucx.source_code.base import (
    Advice,
    Advisory,
    Convention,
    Deprecation,
    LocatedAdvice,
    Failure,
    UsedTable,
)


def test_message_initialization() -> None:
    message = Advice('code1', 'This is a message', 1, 1, 2, 2)
    assert message.code == 'code1'
    assert message.message == 'This is a message'
    assert message.start_line == 1
    assert message.start_col == 1
    assert message.end_line == 2
    assert message.end_col == 2


def test_warning_initialization() -> None:
    warning = Advisory('code2', 'This is a warning', 1, 1, 2, 2)

    copy_of = dataclasses.replace(warning, code='code3')
    assert copy_of.code == 'code3'
    assert isinstance(copy_of, Advisory)


def test_error_initialization() -> None:
    error = Failure('code3', 'This is an error', 1, 1, 2, 2)
    assert isinstance(error, Advice)


def test_deprecation_initialization() -> None:
    deprecation = Deprecation('code4', 'This is a deprecation', 1, 1, 2, 2)
    assert isinstance(deprecation, Advice)


def test_convention_initialization() -> None:
    convention = Convention('code5', 'This is a convention', 1, 1, 2, 2)
    assert isinstance(convention, Advice)


@pytest.mark.parametrize(
    "used_table, expected_name",
    [
        (UsedTable(), "unknown.unknown.unknown"),
        (UsedTable(catalog_name="catalog", schema_name="schema", table_name="table"), "catalog.schema.table"),
    ],
)
def test_used_table_full_name(used_table: UsedTable, expected_name: str) -> None:
    assert used_table.full_name == expected_name


def test_located_advice_message() -> None:
    advice = Advice(
        code="code",
        message="message",
        start_line=0,
        start_col=0,  # Zero based line number is incremented with one to create the message
        end_line=1,
        end_col=1,
    )
    located_advice = LocatedAdvice(advice, Path("test.py"))

    assert str(located_advice) == "test.py:1:0: [code] message"

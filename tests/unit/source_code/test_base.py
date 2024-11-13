import dataclasses

from databricks.labs.ucx.hive_metastore.tables import Table
from databricks.labs.ucx.source_code.base import (
    Advice,
    Advisory,
    Convention,
    Deprecation,
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


def test_used_table_from_table() -> None:
    table = Table("catalog", "schema", "table", "MANAGED", "DELTA")

    used_table = UsedTable.from_table(table, is_read=False, is_write=True)

    assert used_table.catalog_name == "catalog"
    assert used_table.schema_name == "schema"
    assert used_table.table_name == "table"
    assert not used_table.is_read
    assert used_table.is_write

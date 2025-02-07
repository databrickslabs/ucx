import dataclasses
import logging
import os
from pathlib import Path
from unittest.mock import create_autospec, patch

import pytest

from databricks.labs.ucx.source_code.base import (
    Advice,
    Advisory,
    Convention,
    Deprecation,
    Failure,
    LocatedAdvice,
    UsedTable,
    back_up_path,
    revert_back_up_path,
)
from databricks.labs.ucx.source_code.linters.base import Fixer


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


def test_fixer_is_supported_for_diagnostic_test_code() -> None:
    class TestFixer(Fixer):
        @property
        def diagnostic_code(self) -> str:
            return "test"

        def apply(self, code) -> str:
            return "not-implemented"

    fixer = TestFixer()

    assert fixer.is_supported("test")
    assert not fixer.is_supported("other-code")


def test_fixer_is_never_supported_for_diagnostic_empty_code() -> None:
    class TestFixer(Fixer):
        @property
        def diagnostic_code(self) -> str:
            return ""

        def apply(self, code) -> str:
            return "not-implemented"

    fixer = TestFixer()

    assert not fixer.is_supported("test")
    assert not fixer.is_supported("other-code")


def test_back_up_path(tmp_path) -> None:
    path = tmp_path / "file.txt"
    path.touch()
    path_backed_up = back_up_path(path)

    assert path_backed_up.as_posix().endswith("file.txt.bak")
    assert path_backed_up.exists()
    assert path.exists()


def test_back_up_non_existing_file_path(tmp_path) -> None:
    path = tmp_path / "file.txt"
    path_backed_up = back_up_path(path)
    assert not path_backed_up
    assert not path.exists()


def test_back_up_path_with_permission_error(caplog) -> None:
    path = create_autospec(Path)
    with (
        patch("shutil.copyfile", side_effect=PermissionError("Permission denied")),
        caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.source_code.base"),
    ):
        path_backed_up = back_up_path(path)
    assert not path_backed_up
    assert f"Cannot back up file: {path}" in caplog.messages
    assert path.exists()


def test_back_up_and_revert_back_up_path(tmp_path) -> None:
    path = tmp_path / "file.txt"
    path.write_text("content")

    path_backed_up = back_up_path(path)
    is_successfully_reverted_backup = revert_back_up_path(path)

    assert is_successfully_reverted_backup
    assert not path_backed_up.exists()
    assert path.exists()
    assert path.read_text() == "content"


def test_revert_back_up_without_backup_file(tmp_path, caplog) -> None:
    path = tmp_path / "file.txt"
    path.touch()

    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.source_code.base"):
        is_successfully_reverted_backup = revert_back_up_path(path)

    assert is_successfully_reverted_backup is None
    assert f"Backup is missing: {path.with_suffix('.txt.bak')}"
    assert path.exists()


def test_revert_back_up_with_permission_error(caplog) -> None:
    path = create_autospec(Path)

    with (
        patch("shutil.copyfile", side_effect=PermissionError("Permission denied")),
        caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.source_code.base"),
    ):
        is_successfully_reverted_backup = revert_back_up_path(path)

    assert not is_successfully_reverted_backup
    assert f"Cannot revert backup: {path}"


def test_revert_back_up_when_backup_file_cannot_be_deleted(caplog) -> None:
    path = create_autospec(Path)
    path_backed_up = create_autospec(Path)
    path.with_suffix.return_value = path_backed_up

    with (
        patch("shutil.copyfile") as copyfile,
        patch("os.unlink", side_effect=PermissionError("Permission denied")) as unlink,
        caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.source_code.base"),
    ):
        is_successfully_reverted_backup = revert_back_up_path(path)

        copyfile.assert_called_once_with(path_backed_up, path)
        unlink.assert_called_once_with(path_backed_up)
    assert is_successfully_reverted_backup
    assert f"Cannot remove backup file: {path_backed_up}"

import dataclasses
import logging
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
    safe_write_text,
    safe_read_text,
    write_text,
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


def test_safe_read_text(tmp_path) -> None:
    """Should read the contents of a file."""
    path = tmp_path / "file.txt"
    path.write_text("contents")

    contents = safe_read_text(path)

    assert contents == "contents"


def test_safe_read_text_handles_non_existing_file() -> None:
    """Should return None when the file does not exist."""
    contents = safe_read_text(Path("non-existing-file.txt"))
    assert contents is None


def test_safe_read_text_handles_directory(tmp_path) -> None:
    """Should return None when the path is a directory."""
    contents = safe_read_text(tmp_path)
    assert contents is None


@pytest.mark.parametrize(
    "os_error",
    [
        PermissionError("Permission denied"),
        FileNotFoundError("File not found"),
        UnicodeDecodeError("utf-8", b"\x80\x81\x82", 0, 1, "invalid start byte"),
    ],
)
def test_safe_read_text_handles_os_errors(os_error: OSError) -> None:
    """Should return None when an OSError occurs."""
    path = create_autospec(Path)
    path.open.side_effect = os_error

    contents = safe_read_text(path)

    assert contents is None


def test_write_text_to_non_existing_file(tmp_path) -> None:
    path = tmp_path / "file.txt"

    number_of_characters_written = write_text(path, "content")

    assert path.exists()
    assert path.read_text() == "content"
    assert number_of_characters_written == len("content")


def test_write_text_to_existing_file(tmp_path) -> None:
    path = tmp_path / "file.txt"
    path.touch()

    number_of_characters_written = write_text(path, "content")

    assert path.exists()
    assert path.read_text() == "content"
    assert number_of_characters_written == len("content")


def test_write_text_with_permission_error(tmp_path) -> None:
    path = create_autospec(Path)
    path.write_text.side_effect = PermissionError("Permission denied")

    # func:`safe_write_text` handles the errors
    with pytest.raises(PermissionError, match="Permission denied"):
        write_text(path, "content", encoding="utf-8")

    path.write_text.assert_called_once_with("content", encoding="utf-8")


def test_safe_write_text_with_permission_error(tmp_path, caplog) -> None:
    path = create_autospec(Path)
    path.write_text.side_effect = PermissionError("Permission denied")

    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.source_code.base"):
        numbers_of_character_written = safe_write_text(path, "content", encoding="utf-8")

    assert f"Cannot write to file: {path}" in caplog.messages
    assert numbers_of_character_written is None
    path.write_text.assert_called_once_with("content", encoding="utf-8")


def test_back_up_path(tmp_path) -> None:
    path = tmp_path / "file.txt"
    path.touch()

    path_backed_up = back_up_path(path)

    assert path_backed_up
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
    path_backed_up = create_autospec(Path)
    path.with_suffix.return_value = path_backed_up

    with (
        patch("shutil.copyfile", side_effect=PermissionError("Permission denied")) as copyfile,
        caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.source_code.base"),
    ):
        assert not back_up_path(path)
        copyfile.assert_called_once_with(path, path_backed_up)
    assert f"Cannot back up file: {path}" in caplog.messages
    assert path.exists()
    path_backed_up.assert_not_called()


def test_back_up_and_revert_back_up_path(tmp_path) -> None:
    path = tmp_path / "file.txt"
    path.write_text("content")

    path_backed_up = back_up_path(path)
    is_successfully_reverted_backup = revert_back_up_path(path)

    assert is_successfully_reverted_backup
    assert path_backed_up
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
    path_backed_up = create_autospec(Path)
    path.with_suffix.return_value = path_backed_up

    with (
        patch("shutil.copyfile", side_effect=PermissionError("Permission denied")) as copyfile,
        caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.source_code.base"),
    ):
        is_successfully_reverted_backup = revert_back_up_path(path)

        copyfile.assert_called_once_with(path_backed_up, path)
    assert not is_successfully_reverted_backup
    assert f"Cannot revert backup: {path}"
    path_backed_up.assert_not_called()


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
    path_backed_up.assert_not_called()

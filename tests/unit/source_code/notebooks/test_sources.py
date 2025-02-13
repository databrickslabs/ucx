from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.source_code.notebooks.sources import Notebook


def test_notebook_flush_migrated_code(tmp_path) -> None:
    """Happy path of flushing and backing up fixed code"""
    source_code = "# Databricks notebook source\nprint(1)"
    migrated_code = "# Databricks notebook source\nprint(2)"

    path = tmp_path / "test.py"
    path.write_text(source_code)
    notebook = Notebook.parse(path, source_code, Language.PYTHON)
    notebook.cells[0].migrated_code = "print(2)"

    number_of_characters_written = notebook.back_up_original_and_flush_migrated_code()

    assert number_of_characters_written == len(migrated_code)
    assert notebook.original_code == source_code
    assert notebook.migrated_code == migrated_code
    assert path.with_suffix(".py.bak").read_text() == source_code
    assert path.read_text() == migrated_code


def test_notebook_flush_migrated_code_with_empty_cell_contents(tmp_path) -> None:
    """Verify the method handles a cell without contents."""
    source_code = "# Databricks notebook source\nprint(1)"
    migrated_code = "# Databricks notebook source\n"

    path = tmp_path / "test.py"
    path.write_text(source_code)
    notebook = Notebook.parse(path, source_code, Language.PYTHON)
    notebook.cells[0].migrated_code = ""

    number_of_characters_written = notebook.back_up_original_and_flush_migrated_code()

    assert number_of_characters_written == len(migrated_code)
    assert notebook.original_code == source_code
    assert notebook.migrated_code == migrated_code
    assert path.with_suffix(".py.bak").read_text() == source_code
    assert path.read_text() == migrated_code


def test_notebook_flush_non_migrated_code(tmp_path) -> None:
    """No-op in case the code is not migrated"""
    source_code = "# Databricks notebook source\nprint(1)"

    path = tmp_path / "test.py"
    path.write_text(source_code)
    notebook = Notebook.parse(path, "print(1)", Language.PYTHON)

    number_of_characters_written = notebook.back_up_original_and_flush_migrated_code()

    assert number_of_characters_written == len(source_code)
    assert notebook.original_code == source_code
    assert notebook.migrated_code == source_code
    assert not path.with_suffix(".py.bak").is_file()
    assert path.read_text() == source_code


def test_notebook_does_not_flush_migrated_code_when_backup_fails(tmp_path) -> None:
    """If backup fails the method should not flush the migrated code"""

    class _Notebook(Notebook):
        def _back_up_path(self) -> None:
            # Simulate an error, back_up_path handles the error, no return signals an error
            pass

    source_code = "# Databricks notebook source\nprint(1)"
    path = tmp_path / "test.py"
    path.write_text(source_code)
    notebook = _Notebook.parse(path, source_code, Language.PYTHON)
    notebook.cells[0].migrated_code = "print(2)"

    number_of_characters_written = notebook.back_up_original_and_flush_migrated_code()

    assert number_of_characters_written is None
    assert not path.with_suffix(".py.bak").is_file()
    assert path.read_text() == source_code


def test_notebook_flush_migrated_code_with_error(tmp_path) -> None:
    """If flush fails, the method should revert the backup"""

    class _Notebook(Notebook):
        def _safe_write_text(self, contents: str) -> None:
            # Simulate an error, safe_write_text handles the error, no returns signals an error
            _ = contents

    source_code = "# Databricks notebook source\nprint(1)"
    path = tmp_path / "test.py"
    path.write_text(source_code)
    notebook = _Notebook.parse(path, source_code, Language.PYTHON)
    notebook.cells[0].migrated_code = "print(2)"

    number_of_characters_written = notebook.back_up_original_and_flush_migrated_code()

    assert number_of_characters_written is None
    assert not path.with_suffix(".py.bak").is_file()
    assert path.read_text() == source_code

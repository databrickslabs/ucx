import codecs
import locale
from pathlib import Path
from unittest.mock import create_autospec

import pytest

from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.linters.context import LinterContext
from databricks.labs.ucx.source_code.notebooks.sources import FileLinter


@pytest.mark.parametrize("path, content", [("xyz.py", "a = 3"), ("xyz.sql", "select * from dual")])
def test_file_linter_lints_supported_language(path, content, migration_index, mock_path_lookup) -> None:
    linter = FileLinter(
        LinterContext(migration_index), mock_path_lookup, CurrentSessionState(), Path(path), None, content
    )
    advices = list(linter.lint())
    assert not advices


@pytest.mark.parametrize(
    "bom, encoding",
    [
        (codecs.BOM_UTF8, "utf-8"),
        (codecs.BOM_UTF16_LE, "utf-16-le"),
        (codecs.BOM_UTF16_BE, "utf-16-be"),
        (codecs.BOM_UTF32_LE, "utf-32-le"),
        (codecs.BOM_UTF32_BE, "utf-32-be"),
    ],
)
def test_file_linter_lints_supported_language_encoded_file_with_bom(
    tmp_path, migration_index, mock_path_lookup, bom, encoding
) -> None:
    path = tmp_path / "file.py"
    path.write_bytes(bom + "a = 12".encode(encoding))
    linter = FileLinter(LinterContext(migration_index), mock_path_lookup, CurrentSessionState(), path, None)

    advices = list(linter.lint())

    assert not advices


@pytest.mark.parametrize("path", ["xyz.scala", "xyz.r", "xyz.sh"])
def test_file_linter_lints_not_yet_supported_language(tmp_path, path, migration_index, mock_path_lookup) -> None:
    path = tmp_path / path
    path.touch()
    linter = FileLinter(LinterContext(migration_index), mock_path_lookup, CurrentSessionState(), Path(path), None, "")
    advices = list(linter.lint())
    assert [advice.code for advice in advices] == ["unsupported-language"]


@pytest.mark.parametrize(
    "path",
    [
        "xyz.json",
        "xyz.xml",
        "xyz.yml",
        "xyz.cfg",
        "xyz.md",
        "xyz.txt",
        "xyz.gif",
        "xyz.png",
        "xyz.jpg",
        "xyz.jpeg",
        "xyz.tif",
        "xyz.bmp",
        "xyz.toml",
        ".DS_Store",  # on MacOS
    ],
)
def test_file_linter_lints_ignorable_language(tmp_path, path, migration_index, mock_path_lookup) -> None:
    path = tmp_path / path
    path.touch()
    linter = FileLinter(LinterContext(migration_index), mock_path_lookup, CurrentSessionState(), Path(path), None)
    advices = list(linter.lint())
    assert not advices


def test_file_linter_lints_non_ascii_encoded_file(migration_index, mock_path_lookup) -> None:
    preferred_encoding = locale.getpreferredencoding(False)
    non_ascii_encoded_file = Path(__file__).parent.parent / "samples" / "nonascii.py"
    linter = FileLinter(LinterContext(migration_index), mock_path_lookup, CurrentSessionState(), non_ascii_encoded_file)

    advices = list(linter.lint())

    assert len(advices) == 1
    assert advices[0].code == "unsupported-file-encoding"
    assert advices[0].message == f"File without {preferred_encoding} encoding is not supported {non_ascii_encoded_file}"


def test_file_linter_lints_file_with_missing_file(migration_index, mock_path_lookup) -> None:
    path = create_autospec(Path)
    path.suffix = ".py"
    path.open.side_effect = FileNotFoundError("No such file or directory: 'test.py'")
    linter = FileLinter(LinterContext(migration_index), mock_path_lookup, CurrentSessionState(), path)

    advices = list(linter.lint())

    assert len(advices) == 1
    assert advices[0].code == "file-not-found"
    assert advices[0].message == f"File not found: {path}"


def test_file_linter_lints_file_with_missing_read_permission(migration_index, mock_path_lookup) -> None:
    path = create_autospec(Path)
    path.suffix = ".py"
    path.open.side_effect = PermissionError("Permission denied")
    linter = FileLinter(LinterContext(migration_index), mock_path_lookup, CurrentSessionState(), path)

    advices = list(linter.lint())

    assert len(advices) == 1
    assert advices[0].code == "file-permission"
    assert advices[0].message == f"Missing read permission for {path}"

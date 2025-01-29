import codecs
from pathlib import Path
from unittest.mock import create_autospec

import pytest
from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.source_code.files import FileLoader, LocalFile
from databricks.labs.ucx.source_code.graph import Dependency
from databricks.labs.ucx.source_code.path_lookup import PathLookup


def test_local_file_language() -> None:
    local_file = LocalFile(Path(), "source code", Language.PYTHON)
    assert local_file.language == Language.PYTHON


def test_local_file_content() -> None:
    local_file = LocalFile(Path(), "source code", Language.PYTHON)
    assert local_file.content == "source code"


def test_file_loader_loads_non_ascii_file(mock_path_lookup) -> None:
    dependency = Dependency(FileLoader(), Path("nonascii.py"))

    local_file = dependency.load(mock_path_lookup)

    # TODO: Test specific error while loading: https://github.com/databrickslabs/ucx/issues/3584
    assert local_file is None
    assert Path("nonascii.py") in mock_path_lookup.successfully_resolved_paths


def test_file_loader_loads_non_existing_file(migration_index) -> None:
    path = create_autospec(Path)
    path.suffix = ".py"
    path.open.side_effect = FileNotFoundError("No such file or directory: 'test.py'")
    dependency = Dependency(FileLoader(), path)
    path_lookup = create_autospec(PathLookup)
    path_lookup.resolve.return_value = path

    local_file = dependency.load(path_lookup)

    # TODO: Test specific error while loading: https://github.com/databrickslabs/ucx/issues/3584
    assert local_file is None
    path.open.assert_called_once()
    path_lookup.resolve.assert_called_once_with(path)


def test_file_loader_loads_file_without_permission(migration_index, mock_path_lookup) -> None:
    path = create_autospec(Path)
    path.suffix = ".py"
    path.open.side_effect = PermissionError("Permission denied")
    dependency = Dependency(FileLoader(), path)
    path_lookup = create_autospec(PathLookup)
    path_lookup.resolve.return_value = path

    local_file = dependency.load(path_lookup)

    # TODO: Test specific error while loading: https://github.com/databrickslabs/ucx/issues/3584
    assert local_file is None
    path.open.assert_called_once()
    path_lookup.resolve.assert_called_once_with(path)


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
def test_file_loader_loads_file_with_bom(tmp_path, bom, encoding) -> None:
    path = tmp_path / "file.py"
    path.write_bytes(bom + "a = 12".encode(encoding))
    dependency = Dependency(FileLoader(), path)
    path_lookup = create_autospec(PathLookup)
    path_lookup.resolve.return_value = path

    local_file = dependency.load(path_lookup)

    # TODO: Test specific error while loading: https://github.com/databrickslabs/ucx/issues/3584
    assert isinstance(local_file, LocalFile)
    assert local_file.content == "a = 12"
    path_lookup.resolve.assert_called_once_with(path)

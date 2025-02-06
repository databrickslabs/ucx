import codecs
import logging
from pathlib import Path
from unittest.mock import create_autospec

import pytest
from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.source_code.base import NOTEBOOK_HEADER
from databricks.labs.ucx.source_code.graph import Dependency
from databricks.labs.ucx.source_code.files import StubContainer
from databricks.labs.ucx.source_code.notebooks.loaders import NotebookLoader
from databricks.labs.ucx.source_code.notebooks.sources import Notebook
from databricks.labs.ucx.source_code.path_lookup import PathLookup


def test_detects_language() -> None:

    class NotebookLoaderForTesting(NotebookLoader):

        @classmethod
        def detect_language(cls, path: Path, content: str):
            return cls._detect_language(path, content)

    assert NotebookLoaderForTesting.detect_language(Path("hi.py"), "stuff") == Language.PYTHON
    assert NotebookLoaderForTesting.detect_language(Path("hi.sql"), "stuff") == Language.SQL
    assert NotebookLoaderForTesting.detect_language(Path("hi"), "# Databricks notebook source") == Language.PYTHON
    assert NotebookLoaderForTesting.detect_language(Path("hi"), "-- Databricks notebook source") == Language.SQL
    assert not NotebookLoaderForTesting.detect_language(Path("hi"), "stuff")


@pytest.mark.parametrize(
    "error",
    [
        PermissionError("Permission denied"),
        FileNotFoundError("File not found"),
        UnicodeDecodeError("utf-8", b"\x80\x81\x82", 0, 1, "invalid start byte"),
    ],
)
def test_notebook_loader_loads_dependency_raises_error(caplog, error: Exception) -> None:
    path = create_autospec(Path)
    path.suffix = ".py"
    path.open.side_effect = error
    path_lookup = create_autospec(PathLookup)
    path_lookup.resolve.return_value = path
    dependency = Dependency(NotebookLoader(), path)

    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.source_code.notebooks.loaders"):
        notebook = dependency.load(path_lookup)

    assert f"Could not read file: {path}" in caplog.text
    assert notebook is None


def test_notebook_loader_ignores_loading_non_ascii_file(mock_path_lookup) -> None:
    dependency = Dependency(NotebookLoader(), Path("nonascii.py"))

    notebook = dependency.load(mock_path_lookup)

    # TODO: Test specific error while loading: https://github.com/databrickslabs/ucx/issues/3584
    assert notebook is None
    assert Path("nonascii.py") in mock_path_lookup.successfully_resolved_paths


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
def test_notebook_loader_loads_file_with_bom(tmp_path, bom, encoding) -> None:
    path = tmp_path / "file.py"
    path.write_bytes(bom + f"# {NOTEBOOK_HEADER}\na = 12".encode(encoding))
    dependency = Dependency(NotebookLoader(), path)
    path_lookup = create_autospec(PathLookup)
    path_lookup.resolve.return_value = path

    notebook = dependency.load(path_lookup)

    # TODO: Test specific error while loading: https://github.com/databrickslabs/ucx/issues/3584
    assert isinstance(notebook, Notebook)
    assert notebook.original_code == f"# {NOTEBOOK_HEADER}\na = 12"
    path_lookup.resolve.assert_called_once_with(path)


def test_notebook_loader_cannot_load_empty_file(tmp_path) -> None:
    """Empty file fails because it misses the notebook header."""
    path = tmp_path / "empty.py"
    path.write_text("")
    dependency = Dependency(NotebookLoader(), path)
    path_lookup = create_autospec(PathLookup)
    path_lookup.resolve.return_value = path

    notebook = dependency.load(path_lookup)

    # TODO: Test specific error while loading: https://github.com/databrickslabs/ucx/issues/3584
    assert not notebook
    path_lookup.resolve.assert_called_once_with(path)


def test_notebook_loader_ignores_path_by_loading_it_as_stub_container(tmp_path) -> None:
    path = tmp_path / "path.py"
    dependency = Dependency(NotebookLoader(exclude_paths={path}), path)
    path_lookup = create_autospec(PathLookup)
    path_lookup.resolve.return_value = path

    stub = dependency.load(path_lookup)

    assert isinstance(stub, StubContainer)
    path_lookup.resolve.assert_called_once_with(path)

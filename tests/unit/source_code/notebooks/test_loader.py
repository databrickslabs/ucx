import logging
from pathlib import Path
from unittest.mock import create_autospec

from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.source_code.graph import Dependency
from databricks.labs.ucx.source_code.notebooks.loaders import NotebookLoader
from databricks.labs.ucx.source_code.path_lookup import PathLookup


def test_detects_language():

    class NotebookLoaderForTesting(NotebookLoader):

        @classmethod
        def detect_language(cls, path: Path, content: str):
            return cls._detect_language(path, content)

    assert NotebookLoaderForTesting.detect_language(Path("hi.py"), "stuff") == Language.PYTHON
    assert NotebookLoaderForTesting.detect_language(Path("hi.sql"), "stuff") == Language.SQL
    assert NotebookLoaderForTesting.detect_language(Path("hi"), "# Databricks notebook source") == Language.PYTHON
    assert NotebookLoaderForTesting.detect_language(Path("hi"), "-- Databricks notebook source") == Language.SQL
    assert not NotebookLoaderForTesting.detect_language(Path("hi"), "stuff")


def test_notebook_loader_loads_dependency_with_permission_error(caplog):
    path = create_autospec(Path)
    path.read_text.side_effect = PermissionError("Permission denied")
    path_lookup = create_autospec(PathLookup)
    path_lookup.resolve.return_value = path
    dependency = create_autospec(Dependency)
    dependency.path.return_value = path

    with caplog.at_level(logging.WARNING, logger="databricks.labs.ucx.source_code.notebooks.loaders"):
        found = NotebookLoader().load_dependency(path_lookup, dependency)

    assert f"Permission error while reading notebook from workspace: {path}" in caplog.text
    assert found is None

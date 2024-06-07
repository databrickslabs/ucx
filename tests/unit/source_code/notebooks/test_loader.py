from pathlib import Path

from databricks.labs.ucx.source_code.notebooks.loaders import NotebookLoader
from databricks.sdk.service.workspace import Language


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

from pathlib import Path

from databricks.labs.ucx.source_code.notebooks.loaders import NotebookLoader
from databricks.sdk.service.workspace import Language


def test_detects_language():
    assert NotebookLoader.detect_language(Path("hi.py"), "stuff") == Language.PYTHON
    assert NotebookLoader.detect_language(Path("hi.sql"), "stuff") == Language.SQL
    assert NotebookLoader.detect_language(Path("hi"), "# Databricks notebook source") == Language.PYTHON
    assert NotebookLoader.detect_language(Path("hi"), "-- Databricks notebook source") == Language.SQL
    assert not NotebookLoader.detect_language(Path("hi"), "stuff")

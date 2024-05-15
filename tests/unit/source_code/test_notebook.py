from pathlib import Path
import re

import pytest
from databricks.sdk.service.workspace import Language, ObjectType, ObjectInfo

from databricks.labs.ucx.source_code.base import Advisory
from databricks.labs.ucx.source_code.graph import DependencyGraph, SourceContainer, DependencyResolver
from databricks.labs.ucx.source_code.notebooks.sources import Notebook
from databricks.labs.ucx.source_code.notebooks.loaders import (
    NotebookResolver,
    NotebookLoader,
)
from databricks.labs.ucx.source_code.python_linter import PythonLinter
from tests.unit import _load_sources

# fmt: off
# the following samples are real samples from https://github.com/databricks-industry-solutions
# please keep them untouched, we want our unit tests to run against genuinely representative data
PYTHON_NOTEBOOK_SAMPLE = (
    "00_var_context.py.txt",
    Language.PYTHON,
    ['md', 'md', 'md', 'python', 'python', 'python', 'md', 'python', 'md'],
)
PYTHON_NOTEBOOK_WITH_RUN_SAMPLE = (
    "01_var_market_etl.py.txt",
    Language.PYTHON,
    ['md', 'run', 'md', 'python', 'md', 'python', 'python', 'python', 'python', 'md', 'python', 'python',
     'md', 'python', 'python', 'python', 'md', 'python', 'md', 'python', 'python', 'md', 'python'],
)
SCALA_NOTEBOOK_SAMPLE = (
    "01_HL7Streaming.scala.txt",
    Language.SCALA,
    ['md', 'md', 'scala', 'sql', 'md', 'scala', 'scala', 'md', 'md', 'scala', 'md', 'scala', 'sql', 'sql',
     'md', 'scala', 'md', 'scala', 'sql', 'sql', 'sql', 'sql', 'sql', 'sql', 'sql', 'md', 'scala', 'md', 'md'],
)
R_NOTEBOOK_SAMPLE = (
    "3_SparkR_Fine Grained Demand Forecasting.r.txt",
    Language.R,
    ['md', 'md', 'md', 'r', 'r', 'md', 'run', 'r', 'md', 'sql', 'md', 'sql', 'md', 'sql', 'md', 'md', 'r', 'md',
     'r', 'md', 'r', 'md', 'r', 'md', 'r', 'md', 'md', 'r', 'md', 'md', 'r', 'md', 'r', 'md', 'r', 'md', 'md',
     'r', 'md', 'r', 'md', 'r', 'md', 'r', 'md', 'sql', 'md', 'sql', 'md'],
)
SQL_NOTEBOOK_SAMPLE = (
    "chf-pqi-scoring.sql.txt",
    Language.SQL,
    ['md', 'sql', 'sql', 'md', 'sql', 'python', 'sql', 'sql', 'sql', 'md', 'sql',
     'sql', 'md', 'sql', 'sql', 'md', 'sql'],
)
SHELL_NOTEBOOK_SAMPLE = (
    "notebook-with-shell-cell.py.txt",
    Language.PYTHON,
    ['python', 'sh'],
)
PIP_NOTEBOOK_SAMPLE = (
    "notebook-with-pip-cell.py.txt",
    Language.PYTHON,
    ['python', 'pip'],
)
# fmt: on


@pytest.mark.parametrize(
    "source",
    [
        PYTHON_NOTEBOOK_SAMPLE,
        PYTHON_NOTEBOOK_WITH_RUN_SAMPLE,
        SCALA_NOTEBOOK_SAMPLE,
        R_NOTEBOOK_SAMPLE,
        SQL_NOTEBOOK_SAMPLE,
        SHELL_NOTEBOOK_SAMPLE,
        PIP_NOTEBOOK_SAMPLE,
    ],
)
def test_notebook_splits_source_into_cells(source: tuple[str, Language, list[str]]):
    path = source[0]
    sources: list[str] = _load_sources(SourceContainer, path)
    assert len(sources) == 1
    notebook = Notebook.parse(Path(path), sources[0], source[1])
    assert notebook is not None
    languages = [cell.language.magic_name for cell in notebook.cells]
    assert languages == source[2]


@pytest.mark.parametrize(
    "source",
    [
        PYTHON_NOTEBOOK_SAMPLE,
        PYTHON_NOTEBOOK_WITH_RUN_SAMPLE,
        SCALA_NOTEBOOK_SAMPLE,
        R_NOTEBOOK_SAMPLE,
        SQL_NOTEBOOK_SAMPLE,
    ],
)
def test_notebook_rebuilds_same_code(source: tuple[str, Language, list[str]]):
    path = source[0]
    sources: list[str] = _load_sources(SourceContainer, path)
    assert len(sources) == 1
    notebook = Notebook.parse(Path(path), sources[0], source[1])
    assert notebook is not None
    new_source = notebook.to_migrated_code()
    # ignore trailing whitespaces
    actual_purified = re.sub(r'\s+$', '', new_source, flags=re.MULTILINE)
    expected_purified = re.sub(r'\s+$', '', sources[0], flags=re.MULTILINE)
    assert actual_purified == expected_purified


@pytest.mark.parametrize(
    "source",
    [
        PYTHON_NOTEBOOK_SAMPLE,
        PYTHON_NOTEBOOK_WITH_RUN_SAMPLE,
        SCALA_NOTEBOOK_SAMPLE,
        R_NOTEBOOK_SAMPLE,
        SQL_NOTEBOOK_SAMPLE,
    ],
)
def test_notebook_generates_runnable_cells(source: tuple[str, Language, list[str]]):
    path = source[0]
    sources: list[str] = _load_sources(SourceContainer, path)
    assert len(sources) == 1
    notebook = Notebook.parse(Path(path), sources[0], source[1])
    assert notebook is not None
    for cell in notebook.cells:
        assert cell.is_runnable()


def test_notebook_builds_leaf_dependency_graph(path_lookup):
    notebook_loader = NotebookLoader()
    notebook_resolver = NotebookResolver(notebook_loader)
    dependency_resolver = DependencyResolver(notebook_resolver, [], path_lookup)
    maybe = dependency_resolver.resolve_notebook(path_lookup, Path("leaf1.py.txt"))
    graph = DependencyGraph(maybe.dependency, None, dependency_resolver, path_lookup)
    container = maybe.dependency.load(path_lookup)
    problems = container.build_dependency_graph(graph)
    assert not problems
    assert graph.all_paths == {path_lookup.cwd / "leaf1.py.txt"}


def get_status_side_effect(*args):
    path = args[0]
    return ObjectInfo(path=path, object_type=ObjectType.NOTEBOOK, language=Language.PYTHON)


def test_notebook_builds_depth1_dependency_graph(path_lookup):
    paths = ["root1.run.py.txt", "leaf1.py.txt", "leaf2.py.txt"]
    notebook_loader = NotebookLoader()
    notebook_resolver = NotebookResolver(notebook_loader)
    dependency_resolver = DependencyResolver(notebook_resolver, [], path_lookup)
    maybe = dependency_resolver.resolve_notebook(path_lookup, Path(paths[0]))
    graph = DependencyGraph(maybe.dependency, None, dependency_resolver, path_lookup)
    container = maybe.dependency.load(path_lookup)
    problems = container.build_dependency_graph(graph)
    assert not problems
    assert graph.all_paths == {path_lookup.cwd / path for path in paths}


def test_notebook_builds_depth2_dependency_graph(path_lookup):
    paths = ["root2.run.py.txt", "root1.run.py.txt", "leaf1.py.txt", "leaf2.py.txt"]
    notebook_loader = NotebookLoader()
    notebook_resolver = NotebookResolver(notebook_loader)
    dependency_resolver = DependencyResolver(notebook_resolver, [], path_lookup)
    maybe = dependency_resolver.resolve_notebook(path_lookup, Path(paths[0]))
    graph = DependencyGraph(maybe.dependency, None, dependency_resolver, path_lookup)
    container = maybe.dependency.load(path_lookup)
    problems = container.build_dependency_graph(graph)
    assert not problems
    assert graph.all_paths == {path_lookup.cwd / path for path in paths}


def test_notebook_builds_dependency_graph_avoiding_duplicates(path_lookup):
    paths = ["root3.run.py.txt", "root1.run.py.txt", "leaf1.py.txt", "leaf2.py.txt"]
    notebook_loader = NotebookLoader()
    notebook_resolver = NotebookResolver(notebook_loader)
    dependency_resolver = DependencyResolver(notebook_resolver, [], path_lookup)
    maybe = dependency_resolver.resolve_notebook(path_lookup, Path(paths[0]))
    graph = DependencyGraph(maybe.dependency, None, dependency_resolver, path_lookup)
    container = maybe.dependency.load(path_lookup)
    problems = container.build_dependency_graph(graph)
    assert not problems
    # if visited once only, set and list will have same len
    assert graph.all_paths == {path_lookup.cwd / path for path in paths}


def test_notebook_builds_cyclical_dependency_graph(path_lookup):
    paths = ["cyclical1.run.py.txt", "cyclical2.run.py.txt"]
    notebook_loader = NotebookLoader()
    notebook_resolver = NotebookResolver(notebook_loader)
    dependency_resolver = DependencyResolver(notebook_resolver, [], path_lookup)
    maybe = dependency_resolver.resolve_notebook(path_lookup, Path(paths[0]))
    graph = DependencyGraph(maybe.dependency, None, dependency_resolver, path_lookup)
    container = maybe.dependency.load(path_lookup)
    problems = container.build_dependency_graph(graph)
    assert not problems
    assert graph.all_paths == {path_lookup.cwd / path for path in paths}


def test_notebook_builds_python_dependency_graph(path_lookup):
    paths = ["root4.py.txt", "leaf3.py.txt"]
    notebook_loader = NotebookLoader()
    notebook_resolver = NotebookResolver(notebook_loader)
    dependency_resolver = DependencyResolver(notebook_resolver, [], path_lookup)
    maybe = dependency_resolver.resolve_notebook(path_lookup, Path(paths[0]))
    graph = DependencyGraph(maybe.dependency, None, dependency_resolver, path_lookup)
    container = maybe.dependency.load(path_lookup)
    problems = container.build_dependency_graph(graph)
    assert not problems
    assert graph.all_paths == {path_lookup.cwd / path for path in paths}


def test_detects_manual_migration_in_dbutils_notebook_run_in_python_code_():
    sources: list[str] = _load_sources(SourceContainer, "run_notebooks.py.txt")
    linter = PythonLinter()
    advices = list(linter.lint(sources[0]))
    assert [
        Advisory(
            code='dbutils-notebook-run-dynamic',
            message="Path for 'dbutils.notebook.run' is not a constant and requires adjusting the notebook path",
            start_line=14,
            start_col=13,
            end_line=14,
            end_col=50,
        )
    ] == advices


def test_detects_automatic_migration_in_dbutils_notebook_run_in_python_code_():
    sources: list[str] = _load_sources(SourceContainer, "root4.py.txt")
    linter = PythonLinter()
    advices = list(linter.lint(sources[0]))
    assert [
        Advisory(
            code='dbutils-notebook-run-literal',
            message="Call to 'dbutils.notebook.run' will be migrated automatically",
            start_line=2,
            start_col=0,
            end_line=2,
            end_col=38,
        )
    ] == advices


def test_detects_multiple_calls_to_dbutils_notebook_run_in_python_code_():
    source = """
import stuff
do_something_with_stuff(stuff)
stuff2 = dbutils.notebook.run("where is notebook 1?")
stuff3 = dbutils.notebook.run("where is notebook 2?")
"""
    linter = PythonLinter()
    advices = list(linter.lint(source))
    assert len(advices) == 2


def test_does_not_detect_partial_call_to_dbutils_notebook_run_in_python_code_():
    source = """
import stuff
do_something_with_stuff(stuff)
stuff2 = notebook.run("where is notebook 1?")
"""
    linter = PythonLinter()
    advices = list(linter.lint(source))
    assert len(advices) == 0

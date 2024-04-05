from collections.abc import Callable

import pytest
from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.source_code.notebook import Notebook, DependencyGraph
from tests.unit import _load_sources

# fmt: off
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
    ['md', 'sql', 'sql', 'md', 'sql', 'python', 'sql', 'sql', 'sql', 'md', 'sql', 'sql', 'md', 'sql', 'sql', 'md', 'sql'],
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
    ],
)
def test_notebook_splits_source_into_cells(source: tuple[str, Language, list[str]]):
    path = source[0]
    sources: list[str] = _load_sources(Notebook, path)
    assert len(sources) == 1
    notebook = Notebook.parse(path, sources[0], source[1])
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
    sources: list[str] = _load_sources(Notebook, path)
    assert len(sources) == 1
    notebook = Notebook.parse(path, sources[0], source[1])
    assert notebook is not None
    new_source = notebook.to_migrated_code()
    # ignore trailing whitespaces
    actual_purified = new_source.replace(' \n', '\n')
    expected_purified = sources[0].replace(' \n', '\n')
    assert actual_purified == expected_purified


@pytest.mark.skip("for now")
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
    sources: list[str] = _load_sources(Notebook, path)
    assert len(sources) == 1
    notebook = Notebook.parse(path, sources[0], source[1])
    assert notebook is not None
    for cell in notebook.cells:
        assert cell.is_runnable()


def notebook_locator(
    paths: list[str], sources: list[str], languages: list[Language]
) -> Callable[[str], Notebook | None]:
    def locator(path: str) -> Notebook | None:
        local_path = path[2:] if path.startswith('./') else path
        index = paths.index(local_path)
        if index < 0:
            raise ValueError(f"Can't locate notebook {path}")
        return Notebook.parse(paths[index], sources[index], languages[index])

    return locator


def test_notebook_builds_leaf_dependency_graph():
    paths = ["leaf1.py.txt"]
    sources: list[str] = _load_sources(Notebook, *paths)
    languages = [Language.PYTHON] * len(paths)
    locator = notebook_locator(paths, sources, languages)
    notebook = locator(paths[0])
    graph = DependencyGraph(paths[0], None, locator)
    notebook.build_dependency_graph(graph)
    assert graph.paths == {"leaf1.py.txt"}


def test_notebook_builds_depth1_dependency_graph():
    paths = ["root1.run.py.txt", "leaf1.py.txt", "leaf2.py.txt"]
    sources: list[str] = _load_sources(Notebook, *paths)
    languages = [Language.PYTHON] * len(paths)
    locator = notebook_locator(paths, sources, languages)
    notebook = locator(paths[0])
    graph = DependencyGraph(paths[0], None, locator)
    notebook.build_dependency_graph(graph)
    actual = {path[2:] if path.startswith('./') else path for path in graph.paths}
    assert actual == set(paths)


def test_notebook_builds_depth2_dependency_graph():
    paths = ["root2.run.py.txt", "root1.run.py.txt", "leaf1.py.txt", "leaf2.py.txt"]
    sources: list[str] = _load_sources(Notebook, *paths)
    languages = [Language.PYTHON] * len(paths)
    locator = notebook_locator(paths, sources, languages)
    notebook = locator(paths[0])
    graph = DependencyGraph(paths[0], None, locator)
    notebook.build_dependency_graph(graph)
    actual = {path[2:] if path.startswith('./') else path for path in graph.paths}
    assert actual == set(paths)


def test_notebook_builds_dependency_graph_avoiding_duplicates():
    paths = ["root3.run.py.txt", "root1.run.py.txt", "leaf1.py.txt", "leaf2.py.txt"]
    sources: list[str] = _load_sources(Notebook, *paths)
    languages = [Language.PYTHON] * len(paths)
    locator = notebook_locator(paths, sources, languages)
    notebook = locator(paths[0])
    visited: list[str] = []

    def registering_locator(path: str):
        visited.append(path)
        return locator(path)

    graph = DependencyGraph(paths[0], None, registering_locator)
    notebook.build_dependency_graph(graph)
    # if visited once only, set and list will have same len
    assert len(set(visited)) == len(visited)


def test_notebook_builds_cyclical_dependency_graph():
    paths = ["cyclical1.run.py.txt", "cyclical2.run.py.txt"]
    sources: list[str] = _load_sources(Notebook, *paths)
    languages = [Language.PYTHON] * len(paths)
    locator = notebook_locator(paths, sources, languages)
    notebook = locator(paths[0])
    graph = DependencyGraph(paths[0], None, locator)
    notebook.build_dependency_graph(graph)
    actual = {path[2:] if path.startswith('./') else path for path in graph.paths}
    assert actual == set(paths)

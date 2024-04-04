import pytest
from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.source_code.notebook import Notebook, NotebookDependencyGraph
from tests.unit import _load_sources

PYTHON_NOTEBOOK_SAMPLE = (
    "00_var_context.py.sample",
    Language.PYTHON,
    ['md', 'md', 'md', 'python', 'python', 'python', 'md', 'python', 'md'],
)
SCALA_NOTEBOOK_SAMPLE = (
    "01_HL7Streaming.scala",
    Language.SCALA,
    [
        'md',
        'md',
        'scala',
        'sql',
        'md',
        'scala',
        'scala',
        'md',
        'md',
        'scala',
        'md',
        'scala',
        'scala',
        'sql',
        'md',
        'scala',
        'md',
        'scala',
        'sql',
        'sql',
        'sql',
        'sql',
        'sql',
        'sql',
        'sql',
        'md',
        'scala',
        'md',
        'md',
    ],
)
R_NOTEBOOK_SAMPLE = (
    "3_SparkR_Fine Grained Demand Forecasting.r",
    Language.R,
    [
        'md',
        'r',
        'md',
        'r',
        'r',
        'r',
        'r',
        'r',
        'r',
        'r',
        'r',
        'r',
        'r',
        'r',
        'r',
        'r',
        'r',
        'r',
        'r',
        'r',
        'r',
        'r',
        'r',
        'r',
        'r',
        'r',
        'r',
        'r',
        'r',
        'r',
        'r',
        'r',
        'r',
        'r',
        'r',
        'r',
        'r',
        'r',
        'r',
        'r',
        'r',
        'r',
        'r',
        'r',
        'r',
        'r',
        'r',
        'r',
        'md',
    ],
)
SQL_NOTEBOOK_SAMPLE = (
    "chf-pqi-scoring.sql",
    Language.SQL,
    ['md', 'sql', 'sql', 'md', 'sql', 'sql', 'sql', 'sql', 'sql', 'md', 'sql', 'sql', 'md', 'sql', 'sql', 'md', 'sql'],
)


@pytest.mark.parametrize(
    "source", [PYTHON_NOTEBOOK_SAMPLE, SCALA_NOTEBOOK_SAMPLE, R_NOTEBOOK_SAMPLE, SQL_NOTEBOOK_SAMPLE]
)
def test_notebook_splits_source_into_cells(source: tuple[str, Language, list[str]]):
    path = source[0]
    sources: list[str] = _load_sources(Notebook, path)
    assert len(sources) == 1
    notebook = Notebook.parse(path, sources[0], source[1])
    assert notebook is not None
    languages = [cell.language.magic_name[1:] for cell in notebook.cells]
    assert languages == source[2]


@pytest.mark.parametrize(
    "source", [PYTHON_NOTEBOOK_SAMPLE, SCALA_NOTEBOOK_SAMPLE, R_NOTEBOOK_SAMPLE, SQL_NOTEBOOK_SAMPLE]
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


def test_notebook_builds_leaf_dependency_graph():
    path = "leaf.py.sample"
    sources: list[str] = _load_sources(Notebook, path)
    notebook = Notebook.parse(path, sources[0], Language.PYTHON)
    graph = NotebookDependencyGraph(None)
    notebook.build_dependency_graph(graph)
    assert graph.paths == [ "leaf.py.sample" ]

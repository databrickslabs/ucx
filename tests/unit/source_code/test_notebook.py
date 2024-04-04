import pytest
from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.source_code.notebook import Notebook
from tests.unit import _load_sources

PYTHON_NOTEBOOK_SAMPLE = (
    "00_var_context.py.sample",
    Language.PYTHON,
    ['md', 'md', 'md', 'python', 'python', 'python', 'md', 'python'],
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
    ],
)
SQL_NOTEBOOK_SAMPLE = (
    "chf-pqi-scoring.sql",
    Language.SQL,
    ['md', 'sql', 'sql', 'md', 'sql', 'sql', 'sql', 'sql', 'sql', 'md', 'sql', 'sql', 'md', 'sql', 'sql', 'md'],
)


@pytest.mark.parametrize(
    "source", [PYTHON_NOTEBOOK_SAMPLE, SCALA_NOTEBOOK_SAMPLE, R_NOTEBOOK_SAMPLE, SQL_NOTEBOOK_SAMPLE]
)
def test_notebook_splits_source_into_cells(source: tuple[str, Language, list[str]]):
    sources: list[str] = _load_sources(Notebook, source[0])
    assert len(sources) == 1
    notebook = Notebook.parse(sources[0], source[1])
    assert notebook is not None
    languages = [cell.language.magic_name[1:] for cell in notebook.cells]
    assert languages == source[2]

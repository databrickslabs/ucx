import tempfile
from unittest.mock import Mock

import pytest

from pylsp import uris  # type: ignore
from pylsp.workspace import Document, Workspace  # type: ignore
from pylsp.config.config import Config  # type: ignore

from databricks.labs.ucx.source_code import lsp_plugin


@pytest.fixture
def workspace(tmp_path) -> Workspace:
    uri = tmp_path.absolute().as_uri()
    config = Config(uri, {}, 0, {})
    ws = Workspace(uri, Mock(), config=config)
    return ws


def temp_document(doc_text, ws):
    with tempfile.NamedTemporaryFile(mode="w", dir=ws.root_path, delete=False) as temp_file:
        name = temp_file.name
        temp_file.write(doc_text)
    doc = Document(uris.from_fs_path(name), ws)
    return name, doc


def test_pylsp_lint(workspace):
    code = 'sc.emptyRDD()'
    _, doc = temp_document(code, workspace)
    diagnostics = sorted(lsp_plugin.pylsp_lint(workspace, doc), key=lambda d: d['code'])
    assert diagnostics == [
        {
            'range': {'start': {'line': 0, 'character': 0}, 'end': {'line': 0, 'character': 11}},
            'code': 'legacy-context-in-shared-clusters',
            'source': 'databricks.labs.ucx',
            'message': 'sc is not supported on UC Shared Clusters. Rewrite it using spark',
            'severity': 1,
            'tags': [],
        },
        {
            'range': {'start': {'line': 0, 'character': 0}, 'end': {'line': 0, 'character': 13}},
            'code': 'rdd-in-shared-clusters',
            'source': 'databricks.labs.ucx',
            'message': 'RDD APIs are not supported on UC Shared Clusters. Rewrite it using DataFrame API',
            'severity': 1,
            'tags': [],
        },
    ]

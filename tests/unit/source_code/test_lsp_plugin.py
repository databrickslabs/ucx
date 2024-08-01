import tempfile
from unittest.mock import Mock

import pytest

from pylsp import uris  # type: ignore
from pylsp.workspace import Document, Workspace  # type: ignore
from pylsp.config.config import Config  # type: ignore

from databricks.labs.ucx.source_code import lsp_plugin


@pytest.fixture
def config(tmp_path) -> Config:
    uri = tmp_path.absolute().as_uri()
    return Config(uri, {}, 0, {})


@pytest.fixture
def workspace(config) -> Workspace:
    ws = Workspace(config.root_uri, Mock(), config=config)
    return ws


def temp_document(doc_text, ws):
    with tempfile.NamedTemporaryFile(mode="w", dir=ws.root_path, delete=False) as temp_file:
        name = temp_file.name
        temp_file.write(doc_text)
    doc = Document(uris.from_fs_path(name), ws)
    return name, doc


def test_pylsp_lint(workspace, config):
    code = 'sc.emptyRDD()\ndf.groupby("id").applyInPandas(udf)'
    _, doc = temp_document(code, workspace)

    config.update(
        {
            'plugins': {
                'pylsp_ucx': {
                    'enabled': True,
                    'dbrVersion': '14.2',
                    'isServerless': False,
                    'dataSecurityMode': 'USER_ISOLATION',
                },
            }
        }
    )

    diagnostics = sorted(lsp_plugin.pylsp_lint(config, doc), key=lambda d: d['code'])
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
            'range': {'end': {'character': 35, 'line': 1}, 'start': {'character': 0, 'line': 1}},
            'code': 'python-udf-in-shared-clusters',
            'message': 'applyInPandas require DBR 14.3 LTS or above on UC Shared Clusters',
            'severity': 1,
            'source': 'databricks.labs.ucx',
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


def test_pylsp_lint_no_dbr_version(workspace, config):
    code = 'sc.emptyRDD()\ndf.groupby("id").applyInPandas(udf)'
    _, doc = temp_document(code, workspace)
    config.update(
        {
            'plugins': {
                'pylsp_ucx': {'enabled': True, 'isServerless': False, 'dataSecurityMode': 'USER_ISOLATION'},
            }
        }
    )

    diagnostics = sorted(lsp_plugin.pylsp_lint(workspace._config, doc), key=lambda d: d['code'])
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


def test_pylsp_no_config(workspace):
    code = 'sc.emptyRDD()'
    _, doc = temp_document(code, workspace)
    diagnostics = sorted(lsp_plugin.pylsp_lint(workspace._config, doc), key=lambda d: d['code'])
    assert diagnostics == []


def test_pylsp_invalid_config(workspace, config):
    code = 'sc.emptyRDD()\ndf.groupby("id").applyInPandas(udf)'
    _, doc = temp_document(code, workspace)

    config.update(
        {
            'plugins': {
                'pylsp_ucx': {
                    'enabled': True,
                    'dbrVersion': 'invalid-version',
                    'isServerless': False,
                    'dataSecurityMode': 'unknown',
                },
            }
        }
    )

    diagnostics = sorted(lsp_plugin.pylsp_lint(workspace._config, doc), key=lambda d: d['code'])
    assert diagnostics == []


def test_pylsp_lint_single_user_cluster(workspace, config):
    code = 'sc.emptyRDD()'
    _, doc = temp_document(code, workspace)

    config.update(
        {
            'plugins': {
                'pylsp_ucx': {
                    'enabled': True,
                    'dbrVersion': '14.3',
                    'isServerless': False,
                    'dataSecurityMode': 'SINGLE_USER',
                },
            }
        }
    )

    diagnostics = sorted(lsp_plugin.pylsp_lint(workspace._config, doc), key=lambda d: d['code'])
    assert diagnostics == []


def test_with_migration_index(workspace, config):
    code = 'result = spark.sql(args=[1], sqlQuery = "SELECT * FROM old.things").collect()'
    _, doc = temp_document(code, workspace)

    migration_index = [
        {'src_schema': 'old', 'src_table': 'things', 'dst_catalog': 'brand', 'dst_schema': 'new', 'dst_table': 'stuff'}
    ]
    config.update(
        {
            'plugins': {
                'pylsp_ucx': {'migration_index': migration_index},
            }
        }
    )

    diagnostics = sorted(lsp_plugin.pylsp_lint(workspace._config, doc), key=lambda d: d['code'])
    assert diagnostics == [
        {
            'range': {'end': {'character': 67, 'line': 0}, 'start': {'character': 9, 'line': 0}},
            'code': 'table-migrated-to-uc',
            'source': 'databricks.labs.ucx',
            'message': 'Table old.things is migrated to brand.new.stuff in Unity Catalog',
            'severity': 2,
            'tags': [2],
        }
    ]

import os
from pathlib import Path
import sys
import threading
from unittest.mock import patch, create_autospec

import pytest
from databricks.labs.blueprint.installation import MockInstallation
from databricks.labs.lsql.backends import MockBackend
from databricks.labs.ucx.source_code.graph import BaseNotebookResolver
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.sdk import WorkspaceClient, AccountClient
from databricks.sdk.config import Config

from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.contexts.workflow_task import RuntimeContext

pytest.register_assert_rewrite('databricks.labs.blueprint.installation')

# Lock to prevent concurrent execution of tests that patch the environment
_lock = threading.Lock()


def mock_installation() -> MockInstallation:
    return MockInstallation(
        {
            'config.yml': {
                'connect': {
                    'host': 'adb-9999999999999999.14.azuredatabricks.net',
                    'token': '...',
                },
                'inventory_database': 'ucx',
                'warehouse_id': 'abc',
            },
            'mapping.csv': [
                {
                    'catalog_name': 'catalog',
                    'dst_schema': 'schema',
                    'dst_table': 'table',
                    'src_schema': 'schema',
                    'src_table': 'table',
                    'workspace_name': 'workspace',
                },
            ],
        }
    )


@pytest.fixture
def run_workflow(mocker):
    def inner(cb, **replace) -> RuntimeContext:
        with _lock, patch.dict(os.environ, {"DATABRICKS_RUNTIME_VERSION": "14.0"}):
            pyspark_sql_session = mocker.Mock()
            sys.modules["pyspark.sql.session"] = pyspark_sql_session
            installation = mock_installation()
            if 'installation' not in replace:
                replace['installation'] = installation
            if 'workspace_client' not in replace:
                ws = create_autospec(WorkspaceClient)
                ws.api_client.do.return_value = {}
                ws.permissions.get.return_value = {}
                replace['workspace_client'] = ws
            if 'sql_backend' not in replace:
                replace['sql_backend'] = MockBackend()
            if 'config' not in replace:
                replace['config'] = installation.load(WorkspaceConfig)

            module = __import__(cb.__module__, fromlist=[cb.__name__])
            klass, method = cb.__qualname__.split('.', 1)
            workflow = getattr(module, klass)()
            current_task = getattr(workflow, method)

            ctx = RuntimeContext().replace(**replace)
            current_task(ctx)

            return ctx

    yield inner


@pytest.fixture
def acc_client():
    acc = create_autospec(AccountClient)  # pylint: disable=mock-no-usage
    acc.config = Config(host="https://accounts.cloud.databricks.com", account_id="123", token="123")
    return acc


class MockPathLookup(PathLookup):
    def __init__(self, cwd='source_code/samples', sys_paths: list[Path] | None = None):
        super().__init__(Path(__file__).parent / cwd, sys_paths or [])

    def change_directory(self, new_working_directory: Path) -> 'MockPathLookup':
        return MockPathLookup(new_working_directory, self._sys_paths)

    def resolve(self, path: Path) -> Path | None:
        candidates = [path]
        if not path.name.endswith('.txt'):
            candidates.append(Path(f"{path}.txt"))
        for candidate in candidates:
            absolute_path = super().resolve(candidate)
            if not absolute_path:
                continue
            return absolute_path
        return None

    def __repr__(self):
        return f"<MockPathLookup {self._cwd}>"


@pytest.fixture
def path_lookup():
    return MockPathLookup()


@pytest.fixture
def notebook_resolver():
    resolver = create_autospec(BaseNotebookResolver)
    resolver.resolve_notebook.return_value = None
    return resolver

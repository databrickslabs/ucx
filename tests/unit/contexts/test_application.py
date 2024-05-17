from unittest.mock import create_autospec

import pytest

from databricks.labs.ucx.contexts.application import GlobalContext
from databricks.labs.ucx.contexts.workspace_cli import LocalContext
from databricks.labs.ucx.hive_metastore.migration_status import MigrationIndex
from databricks.labs.ucx.hive_metastore.table_migrate import TablesMigrator
from databricks.labs.ucx.source_code.languages import Languages
from tests.unit import mock_workspace_client


@pytest.mark.parametrize("attribute", ["dependency_resolver", "pip_resolver", "site_packages_path", "notebook_loader"])
def test_global_context_attributes_not_none(attribute: str):
    """Attributes should be not None"""
    # Goal is to improve test coverage
    ctx = GlobalContext()
    assert hasattr(ctx, attribute)
    assert getattr(ctx, attribute) is not None


@pytest.mark.parametrize("attribute", ["local_file_migrator", "local_file_linter"])
def test_local_context_attributes_not_none(attribute: str):
    """Attributes should be not None"""
    # Goal is to improve test coverage
    client = mock_workspace_client()
    ctx = LocalContext(client)
    tables_migrator = create_autospec(TablesMigrator)
    tables_migrator.index.return_value = None
    ctx.replace(languages=Languages(MigrationIndex([])), tables_migrator=tables_migrator)
    assert hasattr(ctx, attribute)
    assert getattr(ctx, attribute) is not None

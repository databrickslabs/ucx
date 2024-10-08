from unittest.mock import create_autospec

import pytest
from databricks.labs.lsql.backends import MockBackend

from databricks.labs.ucx.contexts.application import GlobalContext
from databricks.labs.ucx.contexts.workspace_cli import LocalCheckoutContext
from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationIndex
from databricks.labs.ucx.hive_metastore.table_migrate import TablesMigrator
from databricks.labs.ucx.source_code.linters.context import LinterContext
from tests.unit import mock_workspace_client


@pytest.mark.parametrize(
    "attribute",
    [
        "dependency_resolver",
        "pip_resolver",
        "site_packages_path",
        "notebook_loader",
        "folder_loader",
        "workflow_linter",
        "used_tables_crawler_for_paths",
        "used_tables_crawler_for_queries",
        "verify_has_ucx_catalog",
    ],
)
def test_global_context_attributes_not_none(attribute: str) -> None:
    """Attributes should be not None"""
    # Goal is to improve test coverage
    ctx = GlobalContext().replace(workspace_client=mock_workspace_client(), sql_backend=MockBackend())
    assert hasattr(ctx, attribute)
    assert getattr(ctx, attribute) is not None


@pytest.mark.parametrize("attribute", ["local_file_migrator", "local_code_linter"])
def test_local_context_attributes_not_none(attribute: str) -> None:
    """Attributes should be not None"""
    # Goal is to improve test coverage
    client = mock_workspace_client()
    ctx = LocalCheckoutContext(client)
    tables_migrator = create_autospec(TablesMigrator)
    tables_migrator.index.return_value = None
    ctx.replace(languages=LinterContext(TableMigrationIndex([])), tables_migrator=tables_migrator)
    assert hasattr(ctx, attribute)
    assert getattr(ctx, attribute) is not None

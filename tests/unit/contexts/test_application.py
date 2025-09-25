from unittest.mock import create_autospec

import pytest
from databricks.labs.blueprint.paths import WorkspacePath
from databricks.labs.lsql.backends import MockBackend

from databricks.labs.ucx.contexts.application import GlobalContext
from databricks.labs.ucx.contexts.workspace_cli import LocalCheckoutContext
from databricks.labs.ucx.framework.owners import Ownership
from databricks.labs.ucx.hive_metastore.grants import Grant
from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationIndex, TableMigrationStatus
from databricks.labs.ucx.hive_metastore.table_migrate import TablesMigrator
from databricks.labs.ucx.hive_metastore.tables import Table
from databricks.labs.ucx.hive_metastore.udfs import Udf
from databricks.labs.ucx.source_code.base import DirectFsAccess
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
        "ownership_factory",
    ],
)
def test_global_context_attributes_not_none(attribute: str) -> None:
    """Attributes should be not None"""
    # Goal is to improve test coverage
    ctx = GlobalContext().replace(workspace_client=mock_workspace_client(), sql_backend=MockBackend())
    assert hasattr(ctx, attribute)
    assert getattr(ctx, attribute) is not None


@pytest.mark.parametrize("attribute", ["local_code_linter"])
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


@pytest.mark.parametrize(
    "record",
    [
        DirectFsAccess(),
        WorkspacePath(mock_workspace_client()),
        Grant("x", "y"),
        Table("a", "b", "c", "d", "e"),
        Udf("a", "b", "c", "d", "e", "a", False, "c", "d", "e"),
        TableMigrationStatus("x", "y"),
    ],
)
def test_ownership_factory_succeeds(record: type):
    ctx = GlobalContext().replace(workspace_client=mock_workspace_client(), sql_backend=MockBackend())
    ownership = ctx.ownership_factory(record)
    assert isinstance(ownership, Ownership)

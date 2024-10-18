import webbrowser

import pytest
from databricks.sdk.service.catalog import SchemaInfo
from databricks.sdk import WorkspaceClient
from databricks.labs.blueprint.wheels import find_project_root
from databricks.labs.lsql.dashboards import DashboardMetadata, Dashboards

from databricks.labs.ucx.progress.install import Historical


@pytest.fixture
def schema_populated(ws: WorkspaceClient, sql_backend, make_catalog, make_schema) -> SchemaInfo:
    # Different to the other dashboards, the migration process dashboard uses data from a UC catalog,
    # not from the Hive metastore
    catalog = make_catalog()
    schema = make_schema(catalog_name=catalog.name)
    workspace_id = ws.get_workspace_id()
    historicals = [
        Historical(workspace_id, 1, "test", ["test"], {"test": "test"}, [], "Cor"),
        Historical(workspace_id, 1, "test", ["test"], {"test": "test"}, ["failure"], "Cor"),
    ]
    sql_backend.save_table(f"{schema.full_name}.historical", historicals, Historical, mode="overwrite")
    return schema


def test_migration_progress_dashboard(
    ws: WorkspaceClient,
    is_in_debug,
    env_or_skip,
    make_directory,
    schema_populated: SchemaInfo,
) -> None:
    """Check the dashboard visually."""
    warehouse_id = env_or_skip("TEST_DEFAULT_WAREHOUSE_ID")
    directory = make_directory()
    migration_progress_dashboard_path = find_project_root(__file__) / "src/databricks/labs/ucx/queries/progress"
    dashboard_metadata = DashboardMetadata.from_path(migration_progress_dashboard_path).replace_database(
        database=schema_populated.full_name, database_to_replace="inventory"
    )
    dashboard = Dashboards(ws).create_dashboard(
        dashboard_metadata,
        parent_path=str(directory),
        warehouse_id=warehouse_id,
    )
    dashboard_url = f"{ws.config.host}/sql/dashboardsv3/{dashboard.dashboard_id}"
    webbrowser.open(dashboard_url)
    assert True, "Dashboard deployment successful"

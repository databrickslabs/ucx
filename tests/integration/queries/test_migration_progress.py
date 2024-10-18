import dataclasses
import webbrowser

import pytest
from databricks.sdk.service.catalog import SchemaInfo
from databricks.sdk import WorkspaceClient
from databricks.labs.blueprint.wheels import find_project_root
from databricks.labs.lsql.dashboards import DashboardMetadata, Dashboards

from databricks.labs.ucx.progress.install import Historical
from databricks.labs.ucx.hive_metastore.tables import Table


@pytest.fixture
def tables() -> list[Table]:
    return [
        Table("hive_metastore", "foo", "bar", "MANAGED", "delta"),
        Table("hive_metastore", "foo", "bar", "MANAGED", "delta", location="s3://test_location/test1/table1"),
        Table("hive_metastore", "foo", "bar", "EXTERNAL", "delta", location="s3://test_location/test2/table2"),
        Table("hive_metastore", "foo", "bar", "EXTERNAL", "delta", location="dbfs:/mnt/foo/test3/table3"),
    ]


@pytest.fixture
def schema_populated(ws: WorkspaceClient, sql_backend, make_catalog, make_schema, tables) -> SchemaInfo:
    # Different to the other dashboards, the migration process dashboard uses data from a UC catalog,
    # not from the Hive metastore
    catalog = make_catalog()
    schema = make_schema(catalog_name=catalog.name)
    workspace_id = ws.get_workspace_id()
    historicals = []
    for table in tables:
        # TODO: Use historical encoder from https://github.com/databrickslabs/ucx/pull/2743/
        data = {
            field.name: str(getattr(table, field.name))
            for table in tables
            for field in dataclasses.fields(table)
            if getattr(table, field.name) is not None
        }
        historical = Historical(
            workspace_id,
            1,
            "tables",
            [table.catalog, table.database, table.name],
            data,
            [],
            "Cor",
        )
        historicals.append(historical)
    sql_backend.save_table(f"{schema.full_name}.historical", historicals, Historical, mode="overwrite")
    return schema


@pytest.fixture()
def dashboard_metadata(schema_populated: SchemaInfo) -> DashboardMetadata:
    migration_progress_dashboard_path = find_project_root(__file__) / "src/databricks/labs/ucx/queries/progress"
    dashboard_metadata = DashboardMetadata.from_path(migration_progress_dashboard_path).replace_database(
        database=schema_populated.full_name, database_to_replace="inventory"
    )
    return dashboard_metadata


def test_migration_progress_dashboard(
    ws: WorkspaceClient,
    is_in_debug,
    env_or_skip,
    make_directory,
    dashboard_metadata,
    schema_populated: SchemaInfo,
) -> None:
    """Check the dashboard visually."""
    warehouse_id = env_or_skip("TEST_DEFAULT_WAREHOUSE_ID")
    directory = make_directory()
    dashboard = Dashboards(ws).create_dashboard(
        dashboard_metadata,
        parent_path=str(directory),
        warehouse_id=warehouse_id,
    )
    dashboard_url = f"{ws.config.host}/sql/dashboardsv3/{dashboard.dashboard_id}"
    webbrowser.open(dashboard_url)
    assert True, "Dashboard deployment successful"

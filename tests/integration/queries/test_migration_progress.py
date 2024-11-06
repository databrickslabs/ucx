import datetime as dt
import webbrowser

import pytest
from databricks.sdk import WorkspaceClient
from databricks.labs.blueprint.wheels import find_project_root
from databricks.labs.lsql.backends import SqlBackend, Row
from databricks.labs.lsql.dashboards import DashboardMetadata, Dashboards

from databricks.labs.ucx.assessment.clusters import ClusterInfo, PolicyInfo
from databricks.labs.ucx.assessment.jobs import JobInfo
from databricks.labs.ucx.assessment.pipelines import PipelineInfo
from databricks.labs.ucx.hive_metastore.grants import Grant
from databricks.labs.ucx.hive_metastore.tables import Table
from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationStatus
from databricks.labs.ucx.hive_metastore.udfs import Udf
from databricks.labs.ucx.progress.install import ProgressTrackingInstallation
from databricks.labs.ucx.progress.workflow_runs import WorkflowRun
from databricks.labs.ucx.source_code.base import DirectFsAccess, LineageAtom
from databricks.labs.ucx.source_code.jobs import JobProblem
from databricks.labs.ucx.source_code.queries import QueryProblem
from databricks.labs.ucx.source_code.used_table import UsedTable

from ..conftest import MockRuntimeContext


@pytest.fixture
def workflow_runs(ws: WorkspaceClient) -> list[WorkflowRun]:
    now = dt.datetime.now(tz=dt.timezone.utc).replace(microsecond=0)
    workspace_id = ws.get_workspace_id()
    workflow_name = "migration-progress-experimental"
    workflow_id = 1
    return [
        WorkflowRun(
            started_at=now - dt.timedelta(days=1, minutes=30),
            finished_at=now - dt.timedelta(days=1),
            workspace_id=workspace_id,
            workflow_name=workflow_name,
            workflow_id=workflow_id,
            workflow_run_id=1,
            workflow_run_attempt=1,
        ),
        WorkflowRun(
            started_at=now - dt.timedelta(minutes=30),
            finished_at=now,
            workspace_id=workspace_id,
            workflow_name=workflow_name,
            workflow_id=workflow_id,
            workflow_run_id=2,
            workflow_run_attempt=1,
        ),
    ]


@pytest.fixture
def tables() -> list[Table]:
    records = []
    for schema in "schema1", "schema2":
        for table_name in "table1", "table2", "table3", "table4", "table5":
            table = Table("hive_metastore", schema, table_name, "MANAGED", "delta")
            records.append(table)
    return records


@pytest.fixture
def table_migration_status(tables: list[Table]) -> list[TableMigrationStatus]:
    records = []
    for table in tables:
        if table.database == "schema1":  # schema1 tables are migrated
            migration_status = TableMigrationStatus(table.database, table.name, "catalog", table.database, table.name)
        else:
            migration_status = TableMigrationStatus(table.database, table.name)
        records.append(migration_status)
    return records


@pytest.fixture
def udfs() -> list[Udf]:
    records = [
        Udf(
            "hive_metastore",
            "schema1",
            "custom_function",
            deterministic=True,
            data_access="UNKNOWN",
            body="UNKNOWN",
            comment="UNKNOWN",
            func_type="UNKNOWN",
            func_input="UNKNOWN",
            func_returns="UNKNOWN",
        ),
        Udf(
            "hive_metastore",
            "schema1",
            "invalid_function",
            deterministic=True,
            data_access="UNKNOWN",
            body="UNKNOWN",
            comment="UNKNOWN",
            failures="UDF not supported by UC",
            func_type="UNKNOWN",
            func_input="UNKNOWN",
            func_returns="UNKNOWN",
        ),
    ]
    return records


@pytest.fixture
def grants() -> list[Grant]:
    records = [
        Grant("service_principal", "USAGE", "hive_metastore"),
        Grant("Eric", "OWN", "hive_metastore", "sales"),
        Grant("Liran", "DENY", "hive_metastore", "sales"),  # DENY creates a failure
        # Set ownership of mocked tables above
        Grant("Andrew", "OWN", "hive_metastore", "schema1", "table1"),
        Grant("Eric", "OWN", "hive_metastore", "schema1", "table2"),
        Grant("Cor", "OWN", "hive_metastore", "schema1", "table3"),
        Grant("Cor", "OWN", "hive_metastore", "schema1", "table4"),
        Grant("Cor", "OWN", "hive_metastore", "schema1", "table5"),
        Grant("Andrew", "OWN", "hive_metastore", "schema2", "table1"),
        Grant("Cor", "OWN", "hive_metastore", "schema2", "table2"),
        Grant("Cor", "OWN", "hive_metastore", "schema2", "table3"),
        Grant("Cor", "OWN", "hive_metastore", "schema2", "table4"),
        Grant("Cor", "OWN", "hive_metastore", "schema2", "table5"),
    ]
    return records


@pytest.fixture
def jobs() -> list[JobInfo]:
    records = [
        JobInfo("1", success=1, failures=""),
        JobInfo("2", success=0, failures='["No isolation shared clusters not supported in UC"]'),
        JobInfo("3", success=0, failures=""),  # Failure from workflow problems below
    ]
    return records


@pytest.fixture
def workflow_problems() -> list[JobProblem]:
    """Workflow problems are detected by the linter"""
    records = [
        JobProblem(
            job_id=3,
            job_name="Job",
            task_key="4",
            path="file.py",
            code="sql-parse-error",
            message="Could not parse SQL",
            start_line=12,
            start_col=4,
            end_line=12,
            end_col=10,
        )
    ]
    return records


@pytest.fixture
def clusters() -> list[ClusterInfo]:
    records = [
        ClusterInfo("1", success=1, failures=""),
        ClusterInfo("2", success=0, failures='["Uses azure service principal credentials config in cluster"]'),
    ]
    return records


@pytest.fixture
def pipelines() -> list[PipelineInfo]:
    records = [
        PipelineInfo("1", success=1, failures=""),
        PipelineInfo(
            "2", success=0, failures='["Uses passthrough config: spark.databricks.passthrough.enabled in pipeline"]'
        ),
    ]
    return records


@pytest.fixture
def policies() -> list[PolicyInfo]:
    records = [
        PolicyInfo("1", "policy1", success=1, failures=""),
        PolicyInfo(
            "2",
            "policy2",
            success=0,
            failures='["Uses azure service principal credentials config in policy"]',
        ),
    ]
    return records


@pytest.fixture
def query_problems(make_dashboard, make_query) -> list[QueryProblem]:
    dashboard, query = make_dashboard(), make_query()
    records = [
        QueryProblem(
            dashboard.id,
            dashboard.parent,
            dashboard.name,
            query.id,
            query.parent,
            query.name,
            "sql-parse-error",
            "Could not parse SQL",
        )
    ]
    return records


@pytest.fixture
def dfsas(make_workspace_file, make_query) -> list[DirectFsAccess]:
    workspace_file = make_workspace_file(content=f'df = spark.read.csv("dbfs://folder/file.csv")')
    query = make_query(sql_query="SELECT * FROM csv.`dbfs://folder/file.csv`")
    records = [
        DirectFsAccess(
            path="dbfs://folder/file.csv",
            is_read=False,
            # Technically, the mocked code is reading the path, but marking it as write allows us to set the owner to
            # the current user, which we can test below.
            is_write=True,
            source_id=str(workspace_file),
            source_timestamp=dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=2.0),
            source_lineage=[
                LineageAtom(object_type="WORKFLOW", object_id="my_workflow_id", other={"name": "my_workflow"}),
                LineageAtom(object_type="TASK", object_id="my_workflow_id/my_task_id"),
                LineageAtom(object_type="NOTEBOOK", object_id="my_notebook_path"),
                LineageAtom(object_type="FILE", object_id=str(workspace_file)),
            ],
            assessment_start_timestamp=dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=5.0),
            assessment_end_timestamp=dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=2.0),
        ),
        DirectFsAccess(
            path="dbfs://folder/file.csv",
            is_read=False,
            # Technically, the mocked code is reading the path, but marking it as write allows us to set the owner to
            # the current user, which we can test below.
            is_write=True,
            source_id=query.id,
            source_timestamp=dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=2.0),
            source_lineage=[
                LineageAtom(object_type="DASHBOARD", object_id="my_dashboard_id", other={"name": "my_dashboard"}),
                LineageAtom(object_type="QUERY", object_id=f"my_dashboard_id/{query.id}", other={"name": "my_query"}),
            ],
            assessment_start_timestamp=dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=5.0),
            assessment_end_timestamp=dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=2.0),
        ),
    ]
    return records


@pytest.fixture
def used_tables(make_workspace_file, make_table) -> list[UsedTable]:
    table = make_table(catalog_name="hive_metastore")
    workspace_file = make_workspace_file(content=f'df = spark.read.table("{table.full_name}")\ndisplay(df)')
    records = [
        UsedTable(
            catalog_name=table.catalog_name,  # This table is pending migration
            schema_name=table.schema_name,
            table_name=table.name,
            is_read=False,
            # Technically, the mocked code is reading the table, but marking it as write allows us to set the owner to
            # the current user, which we can test below.
            is_write=True,
            source_id=str(workspace_file),
            source_timestamp=dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=2.0),
            source_lineage=[
                LineageAtom(object_type="WORKFLOW", object_id="my_workflow_id", other={"name": "my_workflow"}),
                LineageAtom(object_type="TASK", object_id="my_workflow_id/my_task_id"),
                LineageAtom(object_type="NOTEBOOK", object_id="my_notebook_path"),
                LineageAtom(object_type="FILE", object_id=str(workspace_file)),
            ],
            assessment_start_timestamp=dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=5.0),
            assessment_end_timestamp=dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=2.0),
        ),
        UsedTable(
            catalog_name="catalog",  # This table is migrated
            schema_name="staff_db",
            table_name="employees",
            is_read=False,
            is_write=True,
            source_id=str(make_workspace_file()),
            source_timestamp=dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=2.0),
            source_lineage=[
                LineageAtom(object_type="WORKFLOW", object_id="my_workflow_id", other={"name": "my_workflow"}),
                LineageAtom(object_type="TASK", object_id="my_workflow_id/my_task_id"),
                LineageAtom(object_type="NOTEBOOK", object_id="my_notebook_path"),
                LineageAtom(object_type="FILE", object_id="my file_path"),
            ],
            assessment_start_timestamp=dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=5.0),
            assessment_end_timestamp=dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=2.0),
        ),
    ]
    return records


@pytest.fixture
def catalog_populated(  # pylint: disable=too-many-arguments
    runtime_ctx: MockRuntimeContext,
    workflow_runs: list[WorkflowRun],
    tables: list[Table],
    table_migration_status: list[TableMigrationStatus],
    udfs: list[Udf],
    grants: list[Grant],
    jobs: list[JobInfo],
    workflow_problems: list[JobProblem],
    clusters: list[ClusterInfo],
    pipelines: list[PipelineInfo],
    policies: list[PolicyInfo],
    query_problems: list[QueryProblem],
    dfsas: list[DirectFsAccess],
    used_tables: list[UsedTable],
):
    """Populate the UCX catalog with multiworkspace tables.

    For optimization purposes, this fixture could be "module" (or "session") scoped. However, dependant fixtures are
    "function" scoped, thus one should first evaluate if those can be changed.
    """
    ProgressTrackingInstallation(runtime_ctx.sql_backend, runtime_ctx.ucx_catalog).run()
    runtime_ctx.sql_backend.save_table(
        f"{runtime_ctx.ucx_catalog}.multiworkspace.workflow_runs",
        workflow_runs,
        WorkflowRun,
        mode="overwrite",
    )
    # Persist workflow problems to propagate failures to jobs
    runtime_ctx.sql_backend.save_table(
        f'hive_metastore.{runtime_ctx.inventory_database}.workflow_problems',
        workflow_problems,
        JobProblem,
        mode='overwrite',
    )
    # Persists table migration status to propagate which tables are pending migration
    runtime_ctx.sql_backend.save_table(
        f'hive_metastore.{runtime_ctx.inventory_database}.migration_status',
        table_migration_status,
        TableMigrationStatus,
        mode='overwrite',
    )
    # Persists grant to propagate ownership to tables
    runtime_ctx.sql_backend.save_table(
        f'hive_metastore.{runtime_ctx.inventory_database}.grants',
        grants,
        Grant,
        mode='overwrite',
    )
    # Persist UsedTable to match when looking for UsedTable ownership to tables
    runtime_ctx.sql_backend.save_table(
        f'hive_metastore.{runtime_ctx.inventory_database}.used_tables_in_paths',
        used_tables,
        UsedTable,
        mode='overwrite',
    )
    for parent_run_id in range(1, 3):  # No changes in progress between the two runs
        runtime_ctx = runtime_ctx.replace(parent_run_id=parent_run_id)
        runtime_ctx.tables_progress.append_inventory_snapshot(tables)
        # The deletes below reset the cached parent run ids on the encoders
        del runtime_ctx.tables_progress
        runtime_ctx.udfs_progress.append_inventory_snapshot(udfs)
        del runtime_ctx.udfs_progress
        runtime_ctx.grants_progress.append_inventory_snapshot(grants)
        del runtime_ctx.grants_progress
        runtime_ctx.jobs_progress.append_inventory_snapshot(jobs)
        del runtime_ctx.jobs_progress
        runtime_ctx.clusters_progress.append_inventory_snapshot(clusters)
        del runtime_ctx.clusters_progress
        runtime_ctx.pipelines_progress.append_inventory_snapshot(pipelines)
        del runtime_ctx.pipelines_progress
        runtime_ctx.policies_progress.append_inventory_snapshot(policies)
        del runtime_ctx.policies_progress
        runtime_ctx.query_problem_progress.append_inventory_snapshot(query_problems)
        del runtime_ctx.query_problem_progress
        runtime_ctx.direct_filesystem_access_progress.append_inventory_snapshot(dfsas)
        del runtime_ctx.direct_filesystem_access_progress
        runtime_ctx.used_table_progress.append_inventory_snapshot(used_tables)
        del runtime_ctx.used_table_progress
    return runtime_ctx.ucx_catalog


@pytest.fixture()
def dashboard_metadata(catalog_populated: str) -> DashboardMetadata:
    dashboard_path = find_project_root(__file__) / "src/databricks/labs/ucx/queries/progress/main"
    metadata = DashboardMetadata.from_path(dashboard_path).replace_database(
        catalog=catalog_populated, catalog_to_replace="ucx_catalog"
    )
    metadata.validate()
    return metadata


def test_migration_progress_dashboard(
    ws: WorkspaceClient,
    is_in_debug,  # Skip test when not in debug
    env_or_skip,
    make_directory,
    dashboard_metadata: DashboardMetadata,
) -> None:
    """Inspect the dashboard visually."""
    _ = is_in_debug
    warehouse_id = env_or_skip("TEST_DEFAULT_WAREHOUSE_ID")
    directory = make_directory()
    dashboard = Dashboards(ws).create_dashboard(
        dashboard_metadata,
        parent_path=str(directory),
        warehouse_id=warehouse_id,
        publish=True,
    )
    dashboard_url = f"{ws.config.host}/dashboardsv3/{dashboard.dashboard_id}/published"
    webbrowser.open(dashboard_url)
    assert True, "Dashboard deployment successful"


@pytest.mark.parametrize(
    "query_name, rows",
    [
        ("01_00_percentage_migration_progress", [Row(percentage=round(100 * 23 / 39, 2))]),
        ("01_01_percentage_udf_migration_progress", [Row(percentage=round(100 * 1 / 2, 2))]),
        ("01_02_percentage_grant_migration_progress", [Row(percentage=round(100 * 12 / 13, 2))]),
        ("01_03_percentage_job_migration_progress", [Row(percentage=round(100 * 1 / 3, 2))]),
        ("01_04_percentage_cluster_migration_progress", [Row(percentage=round(100 * 1 / 2, 2))]),
        ("01_05_percentage_table_migration_progress", [Row(percentage=round(100 * 5 / 10, 2))]),
        ("01_06_percentage_used_table_progress", [Row(percentage=round(100 * 1 / 2, 2))]),
        ("01_07_count_direct_filesystem_access", [Row(counter=2)]),
        ("01_08_count_query_problem", [Row(counter=1)]),
        ("01_09_percentage_pipeline_migration_progress", [Row(percentage=round(100 * 1 / 2, 2))]),
        ("01_10_percentage_policy_migration_progress", [Row(percentage=round(100 * 1 / 2, 2))]),
        (
            "01_11_distinct_failures_per_object_type",
            [
                Row(
                    object_type="ClusterInfo",
                    count=1,
                    failure="Uses azure service principal credentials config in cluster",
                ),
                Row(
                    object_type="DirectFsAccess",
                    count=2,
                    failure="Direct filesystem access is not supported in Unity Catalog",
                ),
                Row(
                    object_type="Grant",
                    count=1,
                    failure="Action DENY on DATABASE hive_metastore.sales unmappable to Unity Catalog",
                ),
                Row(object_type="JobInfo", count=1, failure="No isolation shared clusters not supported in UC"),
                Row(
                    object_type="JobInfo",
                    count=1,
                    failure="sql-parse-error: 4 task: file.py: Could not parse SQL",
                ),
                Row(
                    object_type="PipelineInfo",
                    count=1,
                    failure="Uses passthrough config: spark.databricks.passthrough.enabled in pipeline",
                ),
                Row(
                    object_type="PolicyInfo",
                    count=1,
                    failure="Uses azure service principal credentials config in policy",
                ),
                Row(object_type="QueryProblem", count=1, failure="[sql-parse-error] Could not parse SQL"),
                Row(object_type="Table", count=5, failure="Pending migration"),
                Row(object_type="Udf", count=1, failure="UDF not supported by UC"),
                Row(object_type="UsedTable", count=1, failure="Pending migration"),
            ],
        ),
        (
            "02_1_pending_migration_data_objects",
            [Row(count=5)],
        ),
        (
            "02_2_migration_status_by_owner_bar_graph",
            [Row(owner="Andrew", count=1), Row(owner="Cor", count=4)],
        ),
        (
            "02_3_migrated_data_objects",
            [Row(count=5)],
        ),
        (
            "02_4_migration_status_by_owner_overview",
            [
                Row(owner="Andrew", percentage=round(100 * 1 / 2, 2), total=2, total_migrated=1, total_not_migrated=1),
                Row(owner="Cor", percentage=round(100 * 3 / 7, 2), total=7, total_migrated=3, total_not_migrated=4),
                Row(owner="Eric", percentage=round(100 * 1 / 1, 2), total=1, total_migrated=1, total_not_migrated=0),
            ],
        ),
        (
            "03_01_pending_migration_data_asset_references",
            [
                Row(count=3),
            ],
        ),
        (
            "03_03_migrated_data_asset_references",
            [
                Row(count=1),
            ],
        ),
    ],
)
def test_migration_progress_query(
    dashboard_metadata: DashboardMetadata,
    sql_backend: SqlBackend,
    query_name,
    rows,
) -> None:
    datasets = [d for d in dashboard_metadata.get_datasets() if d.name == query_name]
    assert len(datasets) == 1, f"Missing query: {query_name}"
    query_results = list(sql_backend.fetch(datasets[0].query))
    assert query_results == rows


def test_migration_progress_query_data_asset_references_by_owner_bar_graph(
    ws: WorkspaceClient,
    dashboard_metadata: DashboardMetadata,
    sql_backend: SqlBackend,
) -> None:
    """Separate test is required to set the owner of the used table at runtime"""
    query_name = "03_02_data_asset_references_by_owner_bar_graph"
    rows = [Row(owner=ws.current_user.me().user_name, count=1)]
    datasets = [d for d in dashboard_metadata.get_datasets() if d.name == query_name]
    assert len(datasets) == 1, f"Missing query: {query_name}"
    query_results = list(sql_backend.fetch(datasets[0].query))
    assert query_results == rows


def test_migration_progress_query_data_asset_references_pending_migration_overview(
    ws: WorkspaceClient,
    dashboard_metadata: DashboardMetadata,
    sql_backend: SqlBackend,
) -> None:
    """Separate test is required to set the owner of the used table at runtime"""
    query_name = "03_04_data_asset_references_pending_migration_overview"
    current_user = ws.current_user.me().user_name
    rows = [
        Row(
            owner=current_user,
            object_type="Direct filesystem access",
            percentage=0,
            total=2,
            total_migrated=0,
            total_not_migrated=2,
        ),
        Row(
            owner=current_user,
            object_type="Table or view reference",
            percentage=50,
            total=2,
            total_migrated=1,
            total_not_migrated=1,
        )
    ]
    datasets = [d for d in dashboard_metadata.get_datasets() if d.name == query_name]
    assert len(datasets) == 1, f"Missing query: {query_name}"
    query_results = list(sql_backend.fetch(datasets[0].query))
    assert query_results == rows


def test_migration_progress_query_data_asset_references_pending_migration(
    ws: WorkspaceClient,
    dashboard_metadata: DashboardMetadata,
    sql_backend: SqlBackend,
    dfsas: list[DirectFsAccess],
    used_tables: list[UsedTable],
) -> None:
    """Separate test is required to set the dfsas and used table dynamically"""
    query_name = "03_05_data_asset_references_pending_migration"
    workspace_id = ws.get_workspace_id()
    current_user = ws.current_user.me().user_name
    rows = []
    for dfsa in dfsas:
        link_prefix = "/sql/editor/" if dfsa.source_type == "QUERY" else "/#workspace"
        row = Row(
            workspace_id=workspace_id,
            owner=current_user,
            object_type="Direct filesystem access",
            object_id=dfsas[0].path,
            failure="Direct filesystem access is not supported in Unity Catalog",
            is_read=False,
            is_write=True,
            link=f"{link_prefix}{dfsa.source_id}",
        )
        rows.append(row)
    for used_table in used_tables:
        if used_table.catalog_name != "hive_metastore":
            continue
        row = Row(
            workspace_id=workspace_id,
            owner=current_user,
            object_type="Table or view reference",
            object_id=f"{used_table.catalog_name}.{used_table.schema_name}.{used_table.table_name}",
            failure="Pending migration",
            is_read=False,
            is_write=True,
            link=f"/#workspace{used_table.source_id}"
        )
        rows.append(row)
    datasets = [d for d in dashboard_metadata.get_datasets() if d.name == query_name]
    assert len(datasets) == 1, f"Missing query: {query_name}"
    query_results = list(sql_backend.fetch(datasets[0].query))
    assert query_results == rows


def test_migration_progress_code_compatibility_issues(
    ws: WorkspaceClient,
    dashboard_metadata: DashboardMetadata,
    sql_backend: SqlBackend,
    query_problems: list[QueryProblem],
) -> None:
    """Separate test is required to set the dashboard and query id dynamically"""
    query_name = "03_06_code_compatibility_issues"
    workspace_id = ws.get_workspace_id()
    rows = []
    for query_problem in query_problems:
        row = Row(
            workspace_id=workspace_id,
            code="sql-parse-error",
            message="Could not parse SQL",
            dashboard_name=query_problem.dashboard_name,
            query_name=query_problem.query_name,
            dashboard_id=query_problem.dashboard_id,
            query_id=query_problem.query_id,
        )
        rows.append(row)
    datasets = [d for d in dashboard_metadata.get_datasets() if d.name == query_name]
    assert len(datasets) == 1, f"Missing query: {query_name}"
    query_results = list(sql_backend.fetch(datasets[0].query))
    assert query_results == rows

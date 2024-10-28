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
from databricks.labs.ucx.source_code.jobs import JobProblem

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
        Grant("service_principal", "MODIFY", "hive_metastore"),
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
        ("01_0_percentage_migration_progress", [Row(percentage=round(100 * 22 / 34, 2))]),
        ("01_1_percentage_udf_migration_progress", [Row(percentage=round(100 * 1 / 2, 2))]),
        ("01_2_percentage_grant_migration_progress", [Row(percentage=round(100 * 12 / 13, 2))]),
        ("01_3_percentage_job_migration_progress", [Row(percentage=round(100 * 1 / 3, 2))]),
        ("01_4_percentage_cluster_migration_progress", [Row(percentage=round(100 * 1 / 2, 2))]),
        ("01_5_percentage_table_migration_progress", [Row(percentage=round(100 * 5 / 10, 2))]),
        ("01_6_percentage_pipeline_migration_progress", [Row(percentage=round(100 * 1 / 2, 2))]),
        ("01_7_percentage_policy_migration_progress", [Row(percentage=round(100 * 1 / 2, 2))]),
        (
            "01_8_distinct_failures_per_object_type",
            [
                Row(
                    object_type="ClusterInfo",
                    count=1,
                    failure="Uses azure service principal credentials config in cluster",
                ),
                Row(object_type="JobInfo", count=1, failure="No isolation shared clusters not supported in UC"),
                Row(
                    object_type="JobInfo",
                    count=1,
                    failure="sql-parse-error: 23456 task: parent/child.py: Could not parse SQL",
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
                Row(object_type="Table", count=5, failure="Pending migration"),
                Row(object_type="Udf", count=1, failure="UDF not supported by UC"),
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
    ],
)
def test_percentage_migration_progress(
    dashboard_metadata: DashboardMetadata,
    sql_backend: SqlBackend,
    query_name,
    rows,
) -> None:
    datasets = [d for d in dashboard_metadata.get_datasets() if d.name == query_name]
    assert len(datasets) == 1, f"Missing query: {query_name}"
    query_results = list(sql_backend.fetch(datasets[0].query))
    assert query_results == rows

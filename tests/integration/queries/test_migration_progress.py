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
from databricks.labs.ucx.framework.owners import AdministratorLocator, Ownership, Record
from databricks.labs.ucx.hive_metastore.grants import Grant
from databricks.labs.ucx.hive_metastore.tables import Table
from databricks.labs.ucx.hive_metastore.udfs import Udf
from databricks.labs.ucx.progress.install import ProgressTrackingInstallation
from databricks.labs.ucx.progress.workflow_runs import WorkflowRun
from databricks.labs.ucx.source_code.jobs import JobProblem

from ..conftest import MockRuntimeContext


class MockOwnership(Ownership):
    """Mock ownership to control who the owner is for test predictability."""

    def __init__(self, administrator_locator: AdministratorLocator, owner: str) -> None:
        super().__init__(administrator_locator)
        self._owner = owner

    def _maybe_direct_owner(self, record: Record) -> str | None:
        """Obtain the record-specific user-name associated with the given record, if any."""
        return self._owner


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
def tables():
    # TODO: Let schema 1 be migrated and schema 2 not
    # TODO: Set owners
    records = []
    for schema in "schema1", "schema2":
        for table in "table1", "table2", "table3", "table4", "table5":
            table = Table("hive_metastore", schema, table, "MANAGED", "delta")
            records.append(table)
    return records


@pytest.fixture
def udfs():
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
def grants():
    records = [
        Grant("service_principal", "MODIFY", "hive_metastore"),
        Grant("Eric", "OWN", "hive_metastore", "sales"),
        Grant("Liran", "DENY", "hive_metastore", "sales"),  # DENY creates a failure
    ]
    return records


@pytest.fixture
def jobs():
    records = [
        JobInfo("1", success=1, failures=""),
        JobInfo("2", success=0, failures="[No isolation shared clusters not supported in UC]"),
        JobInfo("3", success=0, failures=""),  #  Failure from workflow problems belwo
    ]
    return records

@pytest.fixture
def workflow_problems(installation_ctx):
    """Workflow problems are detected by the linter"""
    records = [
        JobProblem(
            job_id=3,
            job_name="Peter the Job",
            task_key="23456",
            path="parent/child.py",
            code="sql-parse-error",
            message="Could not parse SQL",
            start_line=1234,
            start_col=22,
            end_line=1234,
            end_col=32,
        )
    ]
    return records


@pytest.fixture
def clusters():
    records = [
        ClusterInfo("1", success=1, failures=""),
        ClusterInfo("2", success=0, failures="[Uses azure service principal credentials config in cluster]"),
    ]
    return records


@pytest.fixture
def pipelines():
    records = [
        PipelineInfo("1", success=1, failures=""),
        PipelineInfo(
            "2", success=0, failures="[Uses passthrough config: spark.databricks.passthrough.enabled in pipeline]"
        ),
    ]
    return records


@pytest.fixture
def policies():
    records = [
        PolicyInfo("1", "policy1", success=1, failures=""),
        PolicyInfo(
            "2",
            "policy2",
            success=0,
            failures="[Uses azure service principal credentials config in policy]",
        ),
    ]
    return records


@pytest.fixture
def catalog_populated(
    runtime_ctx: MockRuntimeContext,
    workflow_runs,
    workflow_problems,
    tables,
    udfs,
    grants,
    jobs,
    clusters,
    pipelines,
    policies,
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
    runtime_ctx.sql_backend.save_table(
        f'hive_metastore.{runtime_ctx.inventory_database}.workflow_problems',
        workflow_problems,
        JobProblem,
        mode='overwrite',
    )
    for parent_run_id in range(1, 3):  # No changes in progress between the two runs
        named_parameters = {
            "parent_run_id": parent_run_id,
        }
        runtime_ctx = runtime_ctx.replace(named_parameters=named_parameters)
        runtime_ctx.tables_progress.append_inventory_snapshot(tables)
        runtime_ctx.udfs_progress.append_inventory_snapshot(udfs)
        runtime_ctx.grants_progress.append_inventory_snapshot(grants)
        runtime_ctx.jobs_progress.append_inventory_snapshot(jobs)
        runtime_ctx.clusters_progress.append_inventory_snapshot(clusters)
        runtime_ctx.pipelines_progress.append_inventory_snapshot(pipelines)
        runtime_ctx.policies_progress.append_inventory_snapshot(policies)
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
    is_in_debug,
    env_or_skip,
    make_directory,
    dashboard_metadata: DashboardMetadata,
    catalog_populated: str,
) -> None:
    """Inspect the dashboard visually."""
    _ = catalog_populated  # Used implicitly by the dashboard
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
        ("01_0_percentage_migration_readiness", [Row(percentage=73.91)]),
        ("01_1_percentage_udf_migration_readiness", [Row(percentage=50.0)]),
        ("01_2_percentage_grant_migration_readiness", [Row(percentage=66.67)]),
        ("01_3_percentage_job_migration_readiness", [Row(percentage=50.0)]),
        ("01_4_percentage_cluster_migration_readiness", [Row(percentage=50.0)]),
        ("01_5_percentage_table_migration_readiness", [Row(percentage=100.0)]),
        ("01_6_percentage_pipeline_migration_readiness", [Row(percentage=50.0)]),
        ("01_7_percentage_policy_migration_readiness", [Row(percentage=50.0)]),
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
                Row(owner="Andrew", percentage=50.0, total=2, total_migrated=1, total_not_migrated=1),
                Row(owner="Cor", percentage=42.86, total=7, total_migrated=3, total_not_migrated=4),
                Row(owner="Eric", percentage=100.0, total=1, total_migrated=1, total_not_migrated=0),
            ],
        ),
    ],
)
def test_percentage_migration_readiness(
    dashboard_metadata: DashboardMetadata,
    sql_backend: SqlBackend,
    query_name,
    rows,
) -> None:
    datasets = [d for d in dashboard_metadata.get_datasets() if d.name == query_name]
    assert len(datasets) == 1, f"Missing query: {query_name}"
    query_results = list(sql_backend.fetch(datasets[0].query))
    assert query_results == rows

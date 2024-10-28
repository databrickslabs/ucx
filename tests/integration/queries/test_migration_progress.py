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
from databricks.labs.ucx.progress.history import HistoricalEncoder
from databricks.labs.ucx.progress.install import Historical
from databricks.labs.ucx.progress.workflow_runs import WorkflowRun

from ..conftest import MockInstallationContext


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
    records = []
    for schema in "schema1", "schema2":
        if schema == "schema1":
            failures = []   # schema1 is migrated
        else:
            failures = ["Pending migration"]
        for table in "table1", "table2", "table3", "table4", "table5":
            if table == "table1":
                owner = "Andrew"
            elif schema == "schema1" and table == "table2":
                owner = "Eric"
            else:
                owner = "Cor"
            table = Table("hive_metastore", schema, table, "MANAGED", "delta")
            record = ("tables", [table.catalog, table.database, table.name], table, failures, owner)
            records.append(record)
    return records


@pytest.fixture
def udfs():
    records = []
    for udf in (
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
    ):
        record = (
            "udfs",
            [udf.catalog, udf.database, udf.name],
            udf,
            [] if not udf.failures else udf.failures.split("\n"),
            "Cor",
        )
        records.append(record)
    return records


@pytest.fixture
def grants():
    records = []
    for grant in (
        Grant("service_principal", "MODIFY", "hive_metastore"),
        Grant("Eric", "OWN", "hive_metastore", "sales"),
        Grant("Liran", "DENY", "hive_metastore", "sales"),
    ):
        record = (
            "grants",
            [grant.principal, grant.action_type],
            grant,
            ["DENY is not supported by UC"] if grant.action_type == "DENY" else [],
            "Cor",
        )
        records.append(record)
    return records


@pytest.fixture
def jobs():
    records = []
    for job in (
        JobInfo("1", success=1, failures=""),
        JobInfo("2", success=0, failures="No isolation shared clusters not supported in UC"),
    ):
        record = (
            "jobs",
            [job.job_id],
            job,
            job.failures.split("\n") if job.failures else [],
            "Cor",
        )
        records.append(record)
    return records


@pytest.fixture
def clusters():
    records = []
    for cluster in (
        ClusterInfo("1", success=1, failures=""),
        ClusterInfo("2", success=0, failures="Uses azure service principal credentials config in cluster"),
    ):
        record = (
            "clusters",
            [cluster.cluster_id],
            cluster,
            cluster.failures.split("\n") if cluster.failures else [],
            "Cor",
        )
        records.append(record)
    return records


@pytest.fixture
def pipelines():
    records = []
    for pipeline in (
        PipelineInfo("1", success=1, failures=""),
        PipelineInfo(
            "2", success=0, failures="Uses passthrough config: spark.databricks.passthrough.enabled in pipeline"
        ),
    ):
        record = (
            "pipelines",
            [pipeline.pipeline_id],
            pipeline,
            pipeline.failures.split("\n") if pipeline.failures else [],
            "Cor",
        )
        records.append(record)
    return records


@pytest.fixture
def policies():
    records = []
    for policy in (
        PolicyInfo("1", "policy1", success=1, failures=""),
        PolicyInfo(
            "2",
            "policy2",
            success=0,
            failures="Uses azure service principal credentials config in policy",
        ),
    ):
        record = (
            "policies",
            [policy.policy_id],
            policy,
            policy.failures.split("\n") if policy.failures else [],
            "Cor",
        )
        records.append(record)
    return records


@pytest.fixture
def historical_objects(
    tables,
    udfs,
    grants,
    jobs,
    clusters,
    pipelines,
    policies,
):
    return tables + udfs + grants + jobs + clusters + pipelines + policies


@pytest.fixture
def catalog_populated(
    installation_ctx: MockInstallationContext,
    workflow_runs: list[WorkflowRun],
    historical_objects,
) -> str:
    """Populate the UCX catalog given the objects from the fixtures.

    For optimization purposes, this fixture could be "module" (or "session") scoped. However, dependant fixtures are
    "function" scoped, thus one should first evaluate if those can be changed.
    """
    installation_ctx.progress_tracking_installation.run()
    ucx_catalog = installation_ctx.ucx_catalog
    installation_ctx.sql_backend.save_table(
        f"{ucx_catalog}.multiworkspace.workflow_runs",
        workflow_runs,
        WorkflowRun,
        mode="overwrite",
    )
    workspace_id = installation_ctx.workspace_client.get_workspace_id()
    historicals = []
    for job_run_id in range(1, 3):  # No changes between migration progress run
        for table_name, id_, instance, failures, owner in historical_objects:
            encoder = HistoricalEncoder(
                job_run_id,
                workspace_id,
                MockOwnership(installation_ctx.administrator_locator, owner),
                type(instance),
            )
            historicals.append(encoder.to_historical(instance))
    installation_ctx.sql_backend.save_table(
        f"{ucx_catalog}.multiworkspace.historical",
        historicals,
        Historical,
        mode="overwrite",
    )
    return ucx_catalog


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

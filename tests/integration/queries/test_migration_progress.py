import dataclasses
import webbrowser

import pytest
from databricks.sdk.service.catalog import CatalogInfo
from databricks.sdk import WorkspaceClient
from databricks.labs.blueprint.wheels import find_project_root
from databricks.labs.lsql.backends import SqlBackend, Row
from databricks.labs.lsql.dashboards import DashboardMetadata, Dashboards

from databricks.labs.ucx.progress.install import Historical
from databricks.labs.ucx.hive_metastore.grants import Grant
from databricks.labs.ucx.assessment.jobs import JobInfo
from databricks.labs.ucx.assessment.clusters import ClusterInfo, PolicyInfo
from databricks.labs.ucx.assessment.pipelines import PipelineInfo
from databricks.labs.ucx.hive_metastore.tables import Table
from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationStatus
from databricks.labs.ucx.hive_metastore.udfs import Udf


@pytest.fixture
def tables():
    records = []
    for table in (
        Table("hive_metastore", schema, table, "MANAGED", "delta")
        for schema in ("schema1", "schema2")
        for table in ("table1", "table2", "table3", "table4", "table5")
    ):
        record = ("tables", [table.catalog, table.database, table.name], table, [], "Cor")
        records.append(record)
    return records


@pytest.fixture
def table_migration_statuses(tables):
    statuses = []
    for _, id_, table, _, owner in tables:
        table_migration_status = TableMigrationStatus(table.catalog, table.database, table.name)
        failures = ["not migrated"]
        owner = "Andrew" if table.name == "table1" else "Cor"
        if table.database == "schema1":  # Simulate one schema being migrated
            if table.name == "table2":  # An owner with only migrated objects
                owner = "Eric"
            table_migration_status.dst_catalog = "catalog1"
            table_migration_status.dst_schema = table.database
            table_migration_status.dst_table = table.name
            failures = []
        statuses.append(("migration_status", id_, table_migration_status, failures, owner))
    return statuses


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
    table_migration_statuses,
    udfs,
    grants,
    jobs,
    clusters,
    pipelines,
    policies,
):
    return tables + table_migration_statuses + udfs + grants + jobs + clusters + pipelines + policies


@pytest.fixture
def catalog_populated(
    ws: WorkspaceClient, sql_backend: SqlBackend, make_catalog, make_schema, historical_objects
) -> CatalogInfo:
    """Populate the UCX catalog given the objects from the fixtures.

    For optimization purposes, this fixture could be "module" (or "session") scoped. However, dependant fixtures are
    "function" scoped, thus one should first evaluate if those can be changed.
    """
    catalog = make_catalog()  # The migration progress dashboard uses a UC catalog, not a database in the Hive metastore
    schema = make_schema(catalog_name=catalog.name, name="multiworkspace")
    workspace_id = ws.get_workspace_id()
    historicals = []
    for table_name, id_, instance, failures, owner in historical_objects:
        # TODO: Use historical encoder from https://github.com/databrickslabs/ucx/pull/2743/
        data = {
            field.name: str(getattr(instance, field.name))
            for field in dataclasses.fields(instance)
            if getattr(instance, field.name) is not None
        }
        historical = Historical(workspace_id, 1, table_name, id_, data, failures, owner)
        historicals.append(historical)
    sql_backend.save_table(f"{schema.full_name}.historical", historicals, Historical, mode="overwrite")
    return catalog


@pytest.fixture()
def dashboard_metadata(catalog_populated: CatalogInfo) -> DashboardMetadata:
    dashboard_path = find_project_root(__file__) / "src/databricks/labs/ucx/queries/progress/main"
    metadata = DashboardMetadata.from_path(dashboard_path).replace_database(
        catalog=catalog_populated.name, catalog_to_replace="ucx_catalog"
    )
    metadata.validate()
    return metadata


def test_migration_progress_dashboard(
    ws: WorkspaceClient,
    is_in_debug,
    env_or_skip,
    make_directory,
    dashboard_metadata,
    catalog_populated: CatalogInfo,
) -> None:
    """Inspect the dashboard visually."""
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
        ("01_0_percentage_migration_readiness", [Row(percentage=73.91304347826087)]),
        ("01_1_percentage_udf_migration_readiness", [Row(percentage=50.0)]),
        ("01_2_percentage_grant_migration_readiness", [Row(percentage=66.66666666666667)]),
        ("01_3_percentage_job_migration_readiness", [Row(percentage=50.0)]),
        ("01_4_percentage_cluster_migration_readiness", [Row(percentage=50.0)]),
        ("01_5_percentage_table_migration_readiness", [Row(percentage=100.0)]),
        ("01_6_percentage_pipeline_migration_readiness", [Row(percentage=50.0)]),
        ("01_7_percentage_policy_migration_readiness", [Row(percentage=50.0)]),
        (
            "02_1_migration_status_by_owner_bar_graph",
            [Row(owner="Andrew", count=1), Row(owner="Cor", count=4)],
        ),
        (
            "02_2_migration_status_by_owner_overview",
            [
                Row(owner="Andrew", percentage=50.0, total=2, total_migrated=1, total_not_migrated=1),
                Row(owner="Cor", percentage=50.0, total=8, total_migrated=4, total_not_migrated=4),
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

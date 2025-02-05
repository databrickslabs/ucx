import datetime as dt
import webbrowser

import pytest
import sqlglot
from databricks.sdk import WorkspaceClient
from databricks.labs.blueprint.wheels import find_project_root
from databricks.labs.lsql.backends import SqlBackend, Row
from databricks.labs.lsql.dashboards import DashboardMetadata, Dashboards

from databricks.labs.ucx.assessment.clusters import ClusterInfo, PolicyInfo
from databricks.labs.ucx.assessment.dashboards import Dashboard
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
from databricks.labs.ucx.source_code.linters.queries import QueryProblem
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
def tables(make_schema, make_table) -> list[Table]:
    records = []
    for _ in range(2):
        schema = make_schema()
        for _ in range(5):
            table = Table.from_table_info(make_table(schema_name=schema.name))
            records.append(table)
    return records


@pytest.fixture
def table_migration_statuses(make_catalog, make_schema, make_table, tables: list[Table]) -> list[TableMigrationStatus]:
    catalog = make_catalog()
    schema = make_schema(catalog_name=catalog.name)
    records = []
    for table in tables[: int(len(tables) / 2)]:  # First half is migrated
        migrated_table = make_table(catalog_name=catalog.name, schema_name=schema.name, name=table.name)
        migration_status = TableMigrationStatus(
            table.database, table.name, migrated_table.catalog_name, migrated_table.schema_name, migrated_table.name
        )
        records.append(migration_status)
    for table in tables[int(len(tables) / 2) :]:  # Second half is pending migration
        migration_status = TableMigrationStatus(table.database, table.name)
        records.append(migration_status)
    return records


@pytest.fixture
def statuses_pending_migration(table_migration_statuses: list[TableMigrationStatus]) -> list[TableMigrationStatus]:
    records = [status for status in table_migration_statuses if status.dst_catalog is None]
    assert records, "Expecting a table pending migration"
    return records


@pytest.fixture
def statuses_migrated(table_migration_statuses: list[TableMigrationStatus]) -> list[TableMigrationStatus]:
    records = [status for status in table_migration_statuses if status.dst_catalog is not None]
    assert records, "Expecting a migrated table"
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
def grants(tables: list[Table]) -> list[Grant]:
    records = [
        Grant("service_principal", "USAGE", "hive_metastore"),
        Grant("Eric", "OWN", "hive_metastore", "sales"),
        Grant("Liran", "DENY", "hive_metastore", "sales"),  # DENY creates a failure
        Grant("Andrew", "OWN", tables[0].catalog, tables[0].database, tables[0].name),
        Grant("Eric", "OWN", tables[1].catalog, tables[1].database, tables[1].name),
        Grant("Andrew", "OWN", tables[-1].catalog, tables[-1].database, tables[-1].name),
    ]
    for table in tables[2:-1]:  # Remaining tables
        grant = Grant("Cor", "OWN", table.catalog, table.database, table.name)
        records.append(grant)
    return records


@pytest.fixture
def job_without_failures() -> JobInfo:
    return JobInfo("1", success=1, failures="")


@pytest.fixture
def job_with_failures() -> JobInfo:
    """A job with failures

    - See workflow_problems
    - See dfsa
    """
    return JobInfo("3", success=0, failures="")


@pytest.fixture
def jobs(job_without_failures: JobInfo, job_with_failures: JobInfo) -> list[JobInfo]:
    records = [
        job_without_failures,
        JobInfo("2", success=0, failures='["No isolation shared clusters not supported in UC"]'),
        job_with_failures,
    ]
    return records


@pytest.fixture
def workflow_problems(job_with_failures: JobInfo) -> list[JobProblem]:
    """Workflow problems are detected by the linter"""
    records = [
        JobProblem(
            job_id=int(job_with_failures.job_id),
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
def dashboard_with_hive_tables(
    make_query, make_dashboard, statuses_pending_migration: list[TableMigrationStatus]
) -> Dashboard:
    """A dashboard with all the Hive tables pending migration"""
    table_full_names = []
    for status in statuses_pending_migration:
        table_full_name = ".".join(["hive_metastore", status.src_schema, status.src_table])
        table_full_names.append(table_full_name)
    query_with_hive_table = make_query(sql_query=f"SELECT * FROM {', '.join(table_full_names)}")
    dashboard = Dashboard.from_sdk_redash_dashboard(make_dashboard(query=query_with_hive_table))
    return dashboard


@pytest.fixture
def dashboard_with_uc_tables(make_query, make_dashboard, statuses_migrated: list[TableMigrationStatus]) -> Dashboard:
    """A dashboard with all the UC migrated tables"""
    table_full_names = []
    for status in statuses_migrated:
        table_full_name = ".".join(["hive_metastore", status.src_schema, status.src_table])
        table_full_names.append(table_full_name)
    query_with_hive_table = make_query(sql_query=f"SELECT * FROM {', '.join(table_full_names)}")
    dashboard = Dashboard.from_sdk_redash_dashboard(make_dashboard(query=query_with_hive_table))
    return dashboard


@pytest.fixture
def dbfs_location() -> str:
    return "dbfs://folder/file.csv"


@pytest.fixture
def dashboard_with_dfsa(make_dashboard, make_query, dbfs_location) -> Dashboard:
    query_with_dfsa = make_query(sql_query=f"SELECT * FROM csv.`{dbfs_location}`")
    return Dashboard.from_sdk_redash_dashboard(make_dashboard(query=query_with_dfsa))


@pytest.fixture
def dashboards(
    make_dashboard,
    make_query,
    dashboard_with_hive_tables: Dashboard,
    dashboard_with_uc_tables: Dashboard,
    dashboard_with_dfsa: Dashboard,
    dbfs_location: str,
) -> list[Dashboard]:
    query_with_invalid_sql = make_query(sql_query="SELECT SUM(1")
    records = [
        dashboard_with_hive_tables,
        Dashboard.from_sdk_redash_dashboard(make_dashboard(query=query_with_invalid_sql)),
        dashboard_with_dfsa,
        dashboard_with_uc_tables,
    ]
    return records


@pytest.fixture
def query_problems(ws: WorkspaceClient, dashboards: list[Dashboard], dbfs_location: str) -> list[QueryProblem]:
    """Query problems

    Supported problem codes:
    - sql-parse-error
    - direct-filesystem-access-in-sql-query
    """
    records = []
    for dashboard in dashboards:
        if len(dashboard.query_ids) == 0:
            continue
        query = ws.queries.get(dashboard.query_ids[0])
        if query.id is None or query.query_text is None:
            continue
        try:
            sqlglot.parse_one(query.query_text, dialect="databricks")
        except sqlglot.ParseError:
            query_problem = QueryProblem(
                dashboard.id,
                dashboard.parent or "UNKNOWN",
                dashboard.name or "UNKNOWN",
                query.id,
                query.parent_path or "UNKNOWN",
                query.display_name or "UNKNOWN",
                "sql-parse-error",
                "Could not parse SQL",
            )
            records.append(query_problem)
        if dbfs_location in query.query_text:
            query_problem = QueryProblem(
                dashboard.id,
                dashboard.parent or "UNKNOWN",
                dashboard.name or "UNKNOWN",
                query.id,
                query.parent_path or "UNKNOWN",
                query.display_name or "UNKNOWN",
                "direct-filesystem-access-in-sql-query",
                f"The use of direct filesystem references is deprecated: {dbfs_location}",
            )
            records.append(query_problem)
    return records


@pytest.fixture
def dfsas(make_workspace_file, dbfs_location: str, dashboard_with_dfsa: Dashboard) -> list[DirectFsAccess]:
    workspace_file = make_workspace_file(content=f'df = spark.read.csv("{dbfs_location}")')
    records = [
        DirectFsAccess(  # TODO: Match with Job
            path=dbfs_location,
            is_read=False,
            # Technically, the mocked code is reading the path, but marking it as write allows us to set the owner to
            # the current user, which we can test below.
            is_write=True,
            source_id=str(workspace_file),
            source_timestamp=dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=2.0),
            source_lineage=[
                LineageAtom(object_type="WORKFLOW", object_id="4", other={"name": "my_workflow"}),
                LineageAtom(object_type="TASK", object_id="4/my_task_id"),
                LineageAtom(object_type="NOTEBOOK", object_id="my_notebook_path"),
                LineageAtom(object_type="FILE", object_id=str(workspace_file)),
            ],
            assessment_start_timestamp=dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=5.0),
            assessment_end_timestamp=dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=2.0),
        ),
        DirectFsAccess(
            path=dbfs_location,
            is_read=False,
            # Technically, the mocked code is reading the path, but marking it as write allows us to set the owner to
            # the current user, which we can test below.
            is_write=True,
            source_id=dashboard_with_dfsa.query_ids[0],
            source_timestamp=dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=2.0),
            source_lineage=[
                LineageAtom(object_type="DASHBOARD", object_id=dashboard_with_dfsa.id, other={"name": "my_dashboard"}),
                LineageAtom(
                    object_type="QUERY",
                    object_id=f"{dashboard_with_dfsa.id}/{dashboard_with_dfsa.query_ids[0]}",
                    other={"name": "my_query"},
                ),
            ],
            assessment_start_timestamp=dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=5.0),
            assessment_end_timestamp=dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=2.0),
        ),
    ]
    return records


@pytest.fixture
def used_hive_tables(
    ws: WorkspaceClient,
    make_workspace_file,
    job_with_failures: JobInfo,
    dashboard_with_hive_tables: Dashboard,
    statuses_pending_migration: list[TableMigrationStatus],
) -> list[UsedTable]:
    """The Hive tables are added to the `job_with_failures` and `dashboard_with_hive_tables`."""
    job, dashboard = job_with_failures, dashboard_with_hive_tables
    query = ws.queries.get(dashboard.query_ids[0])
    assert query.id is not None and query.display_name is not None and dashboard.name is not None
    records = []
    for status in statuses_pending_migration:
        table_full_name = ".".join(["hive_metastore", status.src_schema, status.src_table])
        assert table_full_name in (query.query_text or ""), f"Expecting table '{table_full_name} in query: {query.id}"
        workspace_file = make_workspace_file(content=f'df = spark.read.table("{table_full_name}")\ndisplay(df)')
        used_python_table = UsedTable(
            catalog_name="hive_metastore",
            schema_name=status.src_schema,
            table_name=status.src_table,
            is_read=False,
            # Technically, the mocked code is reading the table, but marking it as write allows us to set the owner to
            # the current user, which we can test below.
            is_write=True,
            source_id=str(workspace_file),
            source_timestamp=dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=2.0),
            source_lineage=[
                LineageAtom(object_type="WORKFLOW", object_id=job.job_id, other={"name": "my_workflow"}),
                LineageAtom(object_type="TASK", object_id=f"{job.job_id}/my_task_id"),
            ],
            assessment_start_timestamp=dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=5.0),
            assessment_end_timestamp=dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=2.0),
        )
        used_sql_table = UsedTable(
            catalog_name="hive_metastore",
            schema_name=status.src_schema,
            table_name=status.src_table,
            is_read=False,
            # Technically, the mocked code is reading the table, but marking it as write allows us to set the owner to
            # the current user, which we can test below.
            is_write=True,
            source_id=query.id,
            source_timestamp=dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=2.0),
            source_lineage=[
                LineageAtom(object_type="DASHBOARD", object_id=dashboard.id, other={"name": dashboard.name}),
                LineageAtom(
                    object_type="QUERY", object_id=f"{dashboard.id}/{query.id}", other={"name": query.display_name}
                ),
            ],
            assessment_start_timestamp=dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=5.0),
            assessment_end_timestamp=dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=2.0),
        )
        records.extend([used_python_table, used_sql_table])
    return records


@pytest.fixture
def used_uc_tables(
    ws,
    make_workspace_file,
    job_without_failures: JobInfo,
    dashboard_with_uc_tables: Dashboard,
    statuses_migrated: list[TableMigrationStatus],
) -> list[UsedTable]:
    """The UC tables are used by the job without failures."""
    job, dashboard = job_without_failures, dashboard_with_uc_tables
    query = ws.queries.get(dashboard.query_ids[0])
    assert query.id is not None and query.display_name is not None and dashboard.name is not None
    workspace_file = make_workspace_file()
    records = []
    for status in statuses_migrated:
        assert status.dst_catalog and status.dst_schema and status.dst_table, "Migrated tables are missing destination"
        used_python_table = UsedTable(
            catalog_name=status.dst_catalog,
            schema_name=status.dst_schema,
            table_name=status.dst_table,
            is_read=False,
            is_write=True,
            source_id=str(workspace_file),
            source_timestamp=dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=2.0),
            source_lineage=[
                LineageAtom(object_type="WORKFLOW", object_id=job.job_id, other={"name": "my_workflow"}),
                LineageAtom(object_type="TASK", object_id=f"{job}/my_task_id"),
            ],
            assessment_start_timestamp=dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=5.0),
            assessment_end_timestamp=dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=2.0),
        )
        used_sql_table = UsedTable(
            catalog_name="hive_metastore",
            schema_name=status.src_schema,
            table_name=status.src_table,
            is_read=False,
            # Technically, the mocked code is reading the table, but marking it as write allows us to set the owner to
            # the current user, which we can test below.
            is_write=True,
            source_id=query.id,
            source_timestamp=dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=2.0),
            source_lineage=[
                LineageAtom(object_type="DASHBOARD", object_id=dashboard.id, other={"name": dashboard.name}),
                LineageAtom(
                    object_type="QUERY", object_id=f"{dashboard.id}/{query.id}", other={"name": query.display_name}
                ),
            ],
            assessment_start_timestamp=dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=5.0),
            assessment_end_timestamp=dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=2.0),
        )
        records.extend([used_python_table, used_sql_table])
    return records


@pytest.fixture
def used_tables(used_hive_tables: list[UsedTable], used_uc_tables: list[UsedTable]) -> list[UsedTable]:
    return used_hive_tables + used_uc_tables


@pytest.fixture
def catalog_populated(  # pylint: disable=too-many-arguments
    runtime_ctx: MockRuntimeContext,
    workflow_runs: list[WorkflowRun],
    tables: list[Table],
    table_migration_statuses,
    udfs: list[Udf],
    grants: list[Grant],
    jobs: list[JobInfo],
    workflow_problems: list[JobProblem],
    clusters: list[ClusterInfo],
    pipelines: list[PipelineInfo],
    policies: list[PolicyInfo],
    used_tables: list[UsedTable],
    query_problems: list[QueryProblem],
    dashboards: list[Dashboard],
    dfsas: list[DirectFsAccess],
):
    """Populate the UCX catalog with multiworkspace tables.

    For optimization purposes, this fixture could be "module" (or "session") scoped. However, dependant fixtures are
    "function" scoped, thus one should first evaluate if those can be changed.
    """
    ProgressTrackingInstallation(runtime_ctx.sql_backend, runtime_ctx.ucx_catalog).run()
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
        table_migration_statuses,
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
    # Persist UsedTable to match with dashboard queries
    runtime_ctx.sql_backend.save_table(
        f'hive_metastore.{runtime_ctx.inventory_database}.used_tables_in_queries',
        used_tables,
        UsedTable,
        mode='overwrite',
    )
    # Persists DirectFsAccess to propagate them to Jobs
    runtime_ctx.sql_backend.save_table(
        f'hive_metastore.{runtime_ctx.inventory_database}.directfs_in_paths',
        [dfsa for dfsa in dfsas if dfsa.source_type != "QUERY"],
        DirectFsAccess,
        mode='overwrite',
    )
    # Persists QueryProblems to propagate them to Dashboards
    runtime_ctx.sql_backend.save_table(
        f'hive_metastore.{runtime_ctx.inventory_database}.query_problems',
        query_problems,
        QueryProblem,
        mode='overwrite',
    )
    for workflow_run in workflow_runs:  # No changes in progress between the two runs
        runtime_ctx.sql_backend.save_table(
            f"{runtime_ctx.ucx_catalog}.multiworkspace.workflow_runs",
            [workflow_run],
            WorkflowRun,
            mode="append",
        )
        runtime_ctx = runtime_ctx.replace(parent_run_id=workflow_run.workflow_run_id)
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
        runtime_ctx.dashboards_progress.append_inventory_snapshot(dashboards)
        del runtime_ctx.dashboards_progress
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
        ("01_00_percentage_migration_progress", [Row(percentage=round(100 * 23 / 38, 2))]),
        ("01_01_percentage_udf_migration_progress", [Row(percentage=round(100 * 1 / 2, 2))]),
        ("01_02_percentage_grant_migration_progress", [Row(percentage=round(100 * 12 / 13, 2))]),
        ("01_03_percentage_job_migration_progress", [Row(percentage=round(100 * 1 / 3, 2))]),
        ("01_04_percentage_cluster_migration_progress", [Row(percentage=round(100 * 1 / 2, 2))]),
        ("01_05_percentage_table_migration_progress", [Row(percentage=round(100 * 5 / 10, 2))]),
        ("01_06_percentage_pipeline_migration_progress", [Row(percentage=round(100 * 1 / 2, 2))]),
        ("01_07_percentage_policy_migration_progress", [Row(percentage=round(100 * 1 / 2, 2))]),
        ("01_08_percentage_dashboard_migration_progress", [Row(percentage=round(100 * 1 / 4, 2))]),
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
            "03_01_dashboards_pending_migration",
            [
                Row(count=3),
            ],
        ),
        (
            "03_03_dashboards_migrated",
            [
                Row(count=1),
            ],
        ),
        (
            "99_99_distinct_failures_per_object_type",
            [
                Row(
                    object_type="ClusterInfo",
                    count=1,
                    failure="Uses azure service principal credentials config in cluster",
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
                Row(object_type="Table", count=5, failure="Pending migration"),
                Row(object_type="Udf", count=1, failure="UDF not supported by UC"),
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


def exclude_fields_from_rows(rows: list[Row], *fields: str) -> list[Row]:
    """Exclude the owner field from the row."""
    rows_without_fields = []
    for row in rows:
        data = row.asDict()
        for field in fields:
            if field in data:
                data.pop(field)
        row = Row(**data)
        rows_without_fields.append(row)
    return rows_without_fields


def test_migration_progress_query_dashboard_pending_migration_by_owner_bar_graph(
    dashboard_metadata: DashboardMetadata,
    sql_backend: SqlBackend,
) -> None:
    """Separate test is required to set the owner of the used table at runtime"""
    query_name = "03_02_dashboards_pending_migration_by_owner_bar_graph"
    rows = [Row(count=3)]
    datasets = [d for d in dashboard_metadata.get_datasets() if d.name == query_name]
    assert len(datasets) == 1, f"Missing query: {query_name}"
    query_results = list(sql_backend.fetch(datasets[0].query))
    # See `test_redash_dashboard_ownership_is_me` for why we exclude the owner
    assert exclude_fields_from_rows(query_results, "owner") == rows


def test_migration_progress_query_dashboards_pending_migration_by_owner_overview(
    dashboard_metadata: DashboardMetadata,
    sql_backend: SqlBackend,
) -> None:
    """Separate test is required to set the owner of the used table at runtime"""
    query_name = "03_04_dashboards_pending_migration_by_owner_overview"
    rows = [
        Row(
            percentage=round(100 * 1 / 4, 2),
            total=4,
            total_migrated=1,
            total_not_migrated=3,
        ),
    ]
    datasets = [d for d in dashboard_metadata.get_datasets() if d.name == query_name]
    assert len(datasets) == 1, f"Missing query: {query_name}"
    # See `test_redash_dashboard_ownership_is_me` for why we exclude the owner
    query_results = list(sql_backend.fetch(datasets[0].query))
    assert exclude_fields_from_rows(query_results, "owner") == rows


def test_migration_progress_query_dashboards_pending_migration(
    ws: WorkspaceClient,
    dashboard_metadata: DashboardMetadata,
    sql_backend: SqlBackend,
    dashboard_with_hive_tables: Dashboard,
    statuses_pending_migration: list[TableMigrationStatus],
) -> None:
    """Test if the tables migration are mentioned"""
    query_name = "03_05_dashboards_pending_migration"
    workspace_id = ws.get_workspace_id()
    rows = []
    for status in sorted(statuses_pending_migration, key=lambda s: (s.src_schema, s.src_table)):
        table_full_name = ".".join(["hive_metastore", status.src_schema, status.src_table])
        row = Row(
            workspace_id=workspace_id,
            name=dashboard_with_hive_tables.name,
            dashboard_type="Redash",
            failure=f"Pending migration: {table_full_name}",
            dashboard_link=f"/sql/dashboards/{dashboard_with_hive_tables.id}",
        )
        rows.append(row)
    datasets = [d for d in dashboard_metadata.get_datasets() if d.name == query_name]
    assert len(datasets) == 1, f"Missing query: {query_name}"
    query_results = list(sql_backend.fetch(datasets[0].query))
    query_results_filtered = [r for r in query_results if r.name == dashboard_with_hive_tables.name]
    # See `test_redash_dashboard_ownership_is_me` for why we exclude the owner
    assert exclude_fields_from_rows(query_results_filtered, "owner") == rows

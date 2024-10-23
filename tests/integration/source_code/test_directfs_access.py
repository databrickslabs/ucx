import pytest

from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationIndex
from databricks.labs.ucx.source_code.directfs_access import DirectFsAccessOwnership
from databricks.labs.ucx.source_code.jobs import WorkflowLinter
from databricks.labs.ucx.source_code.queries import QueryLinter


@pytest.mark.xfail(reason="DirectFS access records don't currently include creator/owner information.")
def test_query_dfsa_ownership(runtime_ctx, make_query, make_dashboard, inventory_schema, sql_backend) -> None:
    """Verify the ownership of a direct-fs record for a query."""

    # A dashboard with a query that contains a direct filesystem reference.
    query = make_query(sql_query="SELECT * from csv.`dbfs://some_folder/some_file.csv`")
    dashboard = make_dashboard(query=query)

    # Produce a DFSA record for the query.
    linter = QueryLinter(
        runtime_ctx.workspace_client,
        TableMigrationIndex([]),
        runtime_ctx.directfs_access_crawler_for_queries,
        runtime_ctx.used_tables_crawler_for_queries,
        include_dashboard_ids=[dashboard.id],
    )
    linter.refresh_report(sql_backend, inventory_schema)

    # Find a record for the query.
    records = runtime_ctx.directfs_access_crawler_for_queries.snapshot()
    query_record = next(record for record in records if record.source_id == f"{dashboard.id}/{query.id}")

    # Verify ownership can be made.
    owner = runtime_ctx.directfs_access_ownership.owner_of(query_record)
    assert owner == runtime_ctx.workspace_client.current_user.me().user_name


@pytest.mark.xfail(reason="DirectFS access records don't currently include creator/owner information.")
def test_path_dfsa_ownership(
    runtime_ctx, make_notebook, make_job, make_directory, inventory_schema, sql_backend
) -> None:
    """Verify the ownership of a direct-fs record for a notebook/source path associated with a job."""

    # A job with a notebook task that contains direct filesystem access.
    notebook_source = b"display(spark.read.csv('/mnt/things/e/f/g'))"
    notebook = make_notebook(path=f"{make_directory()}/notebook.py", content=notebook_source)
    job = make_job(notebook_path=notebook)

    # Produce a DFSA record for the job.
    linter = WorkflowLinter(
        runtime_ctx.workspace_client,
        runtime_ctx.dependency_resolver,
        runtime_ctx.path_lookup,
        TableMigrationIndex([]),
        runtime_ctx.directfs_access_crawler_for_paths,
        runtime_ctx.used_tables_crawler_for_paths,
        include_job_ids=[job.job_id],
    )
    linter.refresh_report(sql_backend, inventory_schema)

    # Find a record for our job.
    records = runtime_ctx.directfs_access_crawler_for_paths.snapshot()
    path_record = next(record for record in records if record.source_id == str(notebook))

    # Verify ownership can be made.
    ownership = DirectFsAccessOwnership(runtime_ctx.administrator_locator)
    assert ownership.owner_of(path_record) == runtime_ctx.workspace_client.current_user.me().user_name

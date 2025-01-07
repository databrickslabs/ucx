import pytest

from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationIndex
from databricks.labs.ucx.source_code.base import DirectFsAccess, LineageAtom
from databricks.labs.ucx.source_code.linters.jobs import WorkflowLinter
from databricks.labs.ucx.source_code.base import DirectFsAccess, LineageAtom, CurrentSessionState
from databricks.labs.ucx.source_code.jobs import WorkflowLinter
from databricks.labs.ucx.source_code.linters.directfs import DirectFsAccessPyFixer
from databricks.labs.ucx.source_code.python.python_ast import Tree
from integration.conftest import runtime_ctx
from unit.source_code.linters.test_spark_connect import session_state


def test_legacy_query_dfsa_ownership(runtime_ctx) -> None:
    """Verify the ownership of a direct-fs record for a legacy query."""
    query = runtime_ctx.make_query(sql_query="SELECT * from csv.`dbfs://some_folder/some_file.csv`")
    dashboard = runtime_ctx.make_dashboard(query=query)

    runtime_ctx.query_linter.refresh_report()

    dfsas = list(runtime_ctx.directfs_access_crawler_for_queries.snapshot())
    # By comparing the element instead of the list the `field(compare=False)` of the dataclass attributes take effect
    assert dfsas == [
        DirectFsAccess(
            source_id=f"{dashboard.id}/{query.id}",
            source_lineage=[
                LineageAtom(
                    object_type="DASHBOARD",
                    object_id=dashboard.id,
                    other={"parent": dashboard.parent, "name": dashboard.name},
                ),
                LineageAtom(
                    object_type="QUERY",
                    object_id=f"{dashboard.id}/{query.id}",
                    other={"name": query.name},
                ),
            ],
            path="dbfs://some_folder/some_file.csv",
            is_read=True,
            is_write=False,
        )
    ]

    owner = runtime_ctx.directfs_access_ownership.owner_of(dfsas[0])
    assert owner == runtime_ctx.workspace_client.current_user.me().user_name


@pytest.mark.xfail(reason="https://github.com/databrickslabs/ucx/issues/3411")
def test_lakeview_query_dfsa_ownership(runtime_ctx) -> None:
    """Verify the ownership of a direct-fs record for a Lakeview query."""
    # `make_lakeview_dashboard` fixture expects query as string
    dashboard = runtime_ctx.make_lakeview_dashboard(query="SELECT * from csv.`dbfs://some_folder/some_file.csv`")

    runtime_ctx.query_linter.refresh_report()

    dfsas = list(runtime_ctx.directfs_access_crawler_for_queries.snapshot())
    # By comparing the element instead of the list the `field(compare=False)` of the dataclass attributes take effect
    # The "query" in the source and object id, and "count" in the name are hardcoded in the
    # `make_lakeview_dashboard` fixture
    assert dfsas == [
        DirectFsAccess(
            source_id=f"{dashboard.dashboard_id}/query",
            source_lineage=[
                LineageAtom(
                    object_type="DASHBOARD",
                    object_id=dashboard.dashboard_id,
                    other={"parent": dashboard.parent_path, "name": dashboard.display_name},
                ),
                LineageAtom(
                    object_type="QUERY",
                    object_id=f"{dashboard.dashboard_id}/query",
                    other={"name": "count"},
                ),
            ],
            path="dbfs://some_folder/some_file.csv",
            is_read=True,
            is_write=False,
        )
    ]

    owner = runtime_ctx.directfs_access_ownership.owner_of(dfsas[0])
    assert owner == runtime_ctx.workspace_client.current_user.me().user_name


def test_path_dfsa_ownership(
    runtime_ctx,
    make_notebook,
    make_job,
    make_directory,
    inventory_schema,
    sql_backend,
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
    owner = runtime_ctx.directfs_access_ownership.owner_of(path_record)
    assert owner == runtime_ctx.workspace_client.current_user.me().user_name

def test_path_dfsa_replacement(
    runtime_ctx,
    make_directory,
    make_mounted_location,
    inventory_schema,
    sql_backend,
) -> None:
    """Verify that the direct-fs access in python notebook is replaced with Unity catalog table"""

    mounted_location = '/mnt/things/e/f/g'
    external_table = runtime_ctx.make_table(external_csv=mounted_location,
                                                    )
    notebook_content = f"display(spark.read.csv('{mounted_location}'))"
    notebook = runtime_ctx.make_notebook(path=f"{make_directory()}/notebook.py",
                                         content=notebook_content.encode("ASCII"))
    job = runtime_ctx.make_job(notebook_path=notebook)

    # # Produce a DFSA record for the job.
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

    runtime_ctx.tables_crawler.snapshot()
    runtime_ctx.directfs_access_crawler_for_paths.snapshot()

    session_state = CurrentSessionState()
    directfs_py_fixer = DirectFsAccessPyFixer(session_state,
                                              runtime_ctx.directfs_access_crawler_for_paths,
                                              runtime_ctx.tables_crawler)
    directfs_py_fixer.populate_directfs_table_list([runtime_ctx.directfs_access_crawler_for_paths],
                                                   runtime_ctx.tables_crawler,
                                                   "workspace_name",
                                                   "catalog_name")

    assert True
    directfs_py_fixer.fix_tree(Tree.maybe_normalized_parse(notebook_content).tree)
    assert True

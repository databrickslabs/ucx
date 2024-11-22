from databricks.labs.ucx.assessment.jobs import JobInfo
from databricks.labs.ucx.framework.utils import escape_sql_identifier
from databricks.labs.ucx.source_code.base import DirectFsAccess, LineageAtom


def test_job_progress_encoder_failures(runtime_ctx, az_cli_ctx) -> None:
    az_cli_ctx.progress_tracking_installation.run()
    runtime_ctx = runtime_ctx.replace(
        parent_run_id=1,
        sql_backend=az_cli_ctx.sql_backend,
        ucx_catalog=az_cli_ctx.ucx_catalog,
    )

    job = runtime_ctx.make_job()
    assert job.job_id, "Expected job with id"
    assert job.settings and job.settings.tasks, "Expected job with tasks"

    dashboard = runtime_ctx.make_dashboard()

    direct_fs_access_for_path = DirectFsAccess(
        source_id="/path/to/write_dfsa.py",
        source_lineage=[
            LineageAtom(object_type="WORKFLOW", object_id=str(job.job_id), other={"name": job.settings.name}),
            LineageAtom(object_type="TASK", object_id=job.settings.tasks[0].task_key),
        ],
        path="dfsa:/path/to/data/",
        is_read=False,
        is_write=True,
    )
    runtime_ctx.directfs_access_crawler_for_paths.dump_all([direct_fs_access_for_path])

    direct_fs_access_for_query = DirectFsAccess(
        source_id="/path/to/write_dfsa.py",
        source_lineage=[
            LineageAtom(
                object_type="DASHBOARD",
                object_id=dashboard.id,
                other={"parent": dashboard.parent, "name": dashboard.name},
            ),
            LineageAtom(object_type="QUERY", object_id=f"{dashboard.id}/query", other={"name": "test"}),
        ],
        path="dfsa:/path/to/data/",
        is_read=False,
        is_write=True,
    )
    runtime_ctx.directfs_access_crawler_for_queries.dump_all([direct_fs_access_for_query])

    job_info = JobInfo(
        str(job.job_id),
        success=1,
        failures=0,
        job_name=job.settings.name,
        creator=job.creator_user_name,
    )
    runtime_ctx.jobs_progress.append_inventory_snapshot([job_info])

    history_table_name = escape_sql_identifier(runtime_ctx.tables_progress.full_name)
    records = list(runtime_ctx.sql_backend.fetch(f"SELECT * FROM {history_table_name}"))

    assert len(records) == 1, "Expected one historical entry"
    assert records[0].failures == [
        f"Direct file system access by '{job.settings.tasks[0].task_key}' in '/path/to/write_dfsa.py' to 'dfsa:/path/to/data/'",
    ]

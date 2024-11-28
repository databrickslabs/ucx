from datetime import datetime, timezone, timedelta

import pytest

from databricks.labs.ucx.source_code.base import LineageAtom, UsedTable
from databricks.labs.ucx.source_code.directfs_access import DirectFsAccess
from databricks.labs.ucx.source_code.jobs import JobProblem
from databricks.sdk.service.iam import PermissionLevel

from databricks.labs.ucx.source_code.queries import QueryProblem


def _populate_workflow_problems(installation_ctx):
    job_problems = [
        JobProblem(
            job_id=12345,
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
    installation_ctx.sql_backend.save_table(
        f'{installation_ctx.inventory_database}.workflow_problems',
        job_problems,
        JobProblem,
        mode='overwrite',
    )


def _populate_dashboard_problems(installation_ctx):
    query_problems = [
        QueryProblem(
            dashboard_id="12345",
            dashboard_parent="dashbards/parent",
            dashboard_name="my_dashboard",
            query_id="23456",
            query_parent="queries/parent",
            query_name="my_query",
            code="sql-parse-error",
            message="Could not parse SQL",
        )
    ]
    installation_ctx.sql_backend.save_table(
        f'{installation_ctx.inventory_database}.query_problems',
        query_problems,
        QueryProblem,
        mode='overwrite',
    )


def _populate_directfs_problems(installation_ctx):
    dfsas = [
        DirectFsAccess(
            path="some_path",
            is_read=False,
            is_write=True,
            source_id="xyz.py",
            source_timestamp=datetime.now(timezone.utc) - timedelta(hours=2.0),
            source_lineage=[
                LineageAtom(object_type="WORKFLOW", object_id="my_workflow_id", other={"name": "my_workflow"}),
                LineageAtom(object_type="TASK", object_id="my_workflow_id/my_task_id"),
                LineageAtom(object_type="NOTEBOOK", object_id="my_notebook_path"),
                LineageAtom(object_type="FILE", object_id="my file_path"),
            ],
            assessment_start_timestamp=datetime.now(timezone.utc) - timedelta(minutes=5.0),
            assessment_end_timestamp=datetime.now(timezone.utc) - timedelta(minutes=2.0),
        )
    ]
    installation_ctx.directfs_access_crawler_for_paths.dump_all(dfsas)
    dfsas = [
        DirectFsAccess(
            path="some_path",
            is_read=False,
            is_write=True,
            source_id="xyz.py",
            source_timestamp=datetime.now(timezone.utc) - timedelta(hours=2.0),
            source_lineage=[
                LineageAtom(object_type="DASHBOARD", object_id="my_dashboard_id", other={"name": "my_dashboard"}),
                LineageAtom(object_type="QUERY", object_id="my_dashboard_id/my_query_id", other={"name": "my_query"}),
            ],
            assessment_start_timestamp=datetime.now(timezone.utc) - timedelta(minutes=5.0),
            assessment_end_timestamp=datetime.now(timezone.utc) - timedelta(minutes=2.0),
        )
    ]
    installation_ctx.directfs_access_crawler_for_queries.dump_all(dfsas)


def _populate_used_tables(installation_ctx):
    tables = [
        UsedTable(
            catalog_name="hive_metastore",
            schema_name="staff_db",
            table_name="employees",
            is_read=False,
            is_write=True,
            source_id="xyz.py",
            source_timestamp=datetime.now(timezone.utc) - timedelta(hours=2.0),
            source_lineage=[
                LineageAtom(object_type="WORKFLOW", object_id="my_workflow_id", other={"name": "my_workflow"}),
                LineageAtom(object_type="TASK", object_id="my_workflow_id/my_task_id"),
                LineageAtom(object_type="NOTEBOOK", object_id="my_notebook_path"),
                LineageAtom(object_type="FILE", object_id="my file_path"),
            ],
            assessment_start_timestamp=datetime.now(timezone.utc) - timedelta(minutes=5.0),
            assessment_end_timestamp=datetime.now(timezone.utc) - timedelta(minutes=2.0),
        )
    ]
    installation_ctx.used_tables_crawler_for_paths.dump_all(tables)
    tables = [
        UsedTable(
            catalog_name="hive_metastore",
            schema_name="customers_db",
            table_name="customers",
            is_read=False,
            is_write=True,
            source_id="xyz.py",
            source_timestamp=datetime.now(timezone.utc) - timedelta(hours=2.0),
            source_lineage=[
                LineageAtom(object_type="DASHBOARD", object_id="my_dashboard_id", other={"name": "my_dashboard"}),
                LineageAtom(object_type="QUERY", object_id="my_dashboard_id/my_query_id", other={"name": "my_query"}),
            ],
            assessment_start_timestamp=datetime.now(timezone.utc) - timedelta(minutes=5.0),
            assessment_end_timestamp=datetime.now(timezone.utc) - timedelta(minutes=2.0),
        )
    ]
    installation_ctx.used_tables_crawler_for_queries.dump_all(tables)


@pytest.mark.skip("Development tool")
def test_dashboard_with_prepopulated_data(installation_ctx, make_cluster_policy, make_cluster_policy_permissions):
    """the purpose of this test is to prepopulate data used by the dashboard without running an actual -lengthy- assessment"""
    ucx_group, _ = installation_ctx.make_ucx_group()
    cluster_policy = make_cluster_policy()
    make_cluster_policy_permissions(
        object_id=cluster_policy.policy_id,
        permission_level=PermissionLevel.CAN_USE,
        group_name=ucx_group.display_name,
    )
    installation_ctx.__dict__['include_object_permissions'] = [f"cluster-policies:{cluster_policy.policy_id}"]
    installation_ctx.workspace_installation.run()
    print(f"\nInventory database is {installation_ctx.inventory_database}\n")
    # populate data
    _populate_workflow_problems(installation_ctx)
    _populate_dashboard_problems(installation_ctx)
    _populate_directfs_problems(installation_ctx)
    _populate_used_tables(installation_ctx)
    # put a breakpoint here
    print("Put a breakpoint here! Then go check the dashboard in your workspace ;-)\n")

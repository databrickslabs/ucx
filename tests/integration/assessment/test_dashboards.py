import pytest

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
            dashboard_id = "12345",
            dashboard_parent = "dashbards/parent",
            dashboard_name = "my_dashboard",
            query_id="23456",
            query_parent= "queries/parent",
            query_name="my_query",
            code="sql-parse-error",
            message="Could not parse SQL"
        )
    ]
    installation_ctx.sql_backend.save_table(
        f'{installation_ctx.inventory_database}.query_problems',
        query_problems,
        QueryProblem,
        mode='overwrite',
    )


@pytest.mark.skip
def test_dashboard_with_prepopulated_data(installation_ctx, make_cluster_policy, make_cluster_policy_permissions):
    """the purpose of this test is to prepopulate data used by the dashboard without running an assessment"""
    ucx_group, _ = installation_ctx.make_ucx_group()
    cluster_policy = make_cluster_policy()
    make_cluster_policy_permissions(
        object_id=cluster_policy.policy_id,
        permission_level=PermissionLevel.CAN_USE,
        group_name=ucx_group.display_name,
    )
    installation_ctx.__dict__['include_object_permissions'] = [f"cluster-policies:{cluster_policy.policy_id}"]
    installation_ctx.workspace_installation.run()
    # populate data
    _populate_workflow_problems(installation_ctx)
    _populate_dashboard_problems(installation_ctx)
    # put a breakpoint here
    print("Put a breakpoint here! Then go check the dashboard in your workspace ;-)")

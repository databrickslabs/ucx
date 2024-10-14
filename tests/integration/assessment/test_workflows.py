import dataclasses

from databricks.sdk.errors import NotFound, InvalidParameterValue
from databricks.sdk.retries import retried
from databricks.sdk.service.iam import PermissionLevel


@retried(on=[NotFound, InvalidParameterValue])
def test_running_real_assessment_job(
    installation_ctx,
    make_cluster_policy,
    make_cluster_policy_permissions,
    make_dashboard,
    sql_backend,
) -> None:
    ws_group, _ = installation_ctx.make_ucx_group()
    # TODO: Move `make_cluster_policy` and `make_cluster_policy_permissions` to context like other `make_` methods
    cluster_policy = make_cluster_policy()
    make_cluster_policy_permissions(
        object_id=cluster_policy.policy_id,
        permission_level=PermissionLevel.CAN_USE,
        group_name=ws_group.display_name,
    )
    installation_ctx = installation_ctx.replace(
        config_transform=lambda wc: dataclasses.replace(
            wc,
            include_object_permissions=[f"cluster-policies:{cluster_policy.policy_id}"],
        ),
    )

    source_schema = installation_ctx.make_schema(catalog_name="hive_metastore")
    managed_table = installation_ctx.make_table(schema_name=source_schema.name)
    external_table = installation_ctx.make_table(schema_name=source_schema.name, external=True)
    tmp_table = installation_ctx.make_table(schema_name=source_schema.name, ctas="SELECT 2+2 AS four")
    view = installation_ctx.make_table(schema_name=source_schema.name, ctas="SELECT 2+2 AS four", view=True)
    non_delta = installation_ctx.make_table(schema_name=source_schema.name, non_delta=True)
    installation_ctx.make_linting_resources()
    installation_ctx.workspace_installation.run()

    workflow = "assessment"
    installation_ctx.deployed_workflows.run_workflow(workflow)
    assert installation_ctx.deployed_workflows.validate_step(workflow), f"Workflow failed: {workflow}"

    after = installation_ctx.generic_permissions_support.load_as_dict("cluster-policies", cluster_policy.policy_id)
    assert after[ws_group.display_name] == PermissionLevel.CAN_USE

    expected_tables = {managed_table.name, external_table.name, tmp_table.name, view.name, non_delta.name}
    actual_tables = set(table.name for table in installation_ctx.tables_crawler.snapshot())
    assert actual_tables == expected_tables

    query = f"SELECT * FROM {installation_ctx.inventory_database}.workflow_problems"
    for row in sql_backend.fetch(query):
        assert row['path'] != 'UNKNOWN'

import dataclasses
from datetime import timedelta

from databricks.sdk.errors import NotFound, InvalidParameterValue
from databricks.sdk.retries import retried
from databricks.sdk.service.iam import PermissionLevel

from databricks.labs.ucx.hive_metastore import TablesCrawler


@retried(on=[NotFound, InvalidParameterValue])
def test_running_real_assessment_job(
    installation_ctx,
    make_cluster_policy,
    make_cluster_policy_permissions,
    make_dashboard,
    sql_backend,
    inventory_schema,
    populate_for_linting,
) -> None:
    ws_group, _ = installation_ctx.make_ucx_group()
    cluster_policy = make_cluster_policy()
    make_cluster_policy_permissions(
        object_id=cluster_policy.policy_id,
        permission_level=PermissionLevel.CAN_USE,
        group_name=ws_group.display_name,
    )

    source_schema = installation_ctx.make_schema(catalog_name="hive_metastore")
    managed_table = installation_ctx.make_table(schema_name=source_schema.name)
    external_table = installation_ctx.make_table(schema_name=source_schema.name, external=True)
    tmp_table = installation_ctx.make_table(schema_name=source_schema.name, ctas="SELECT 2+2 AS four")
    view = installation_ctx.make_table(schema_name=source_schema.name, ctas="SELECT 2+2 AS four", view=True)
    non_delta = installation_ctx.make_table(schema_name=source_schema.name, non_delta=True)
    installation_ctx = installation_ctx.replace(
        config_transform=lambda wc: dataclasses.replace(
            wc,
            include_object_permissions=[f"cluster-policies:{cluster_policy.policy_id}"],
            include_databases=[source_schema.name],
        ),
    )
    installation_ctx.workspace_installation.run()
    populate_for_linting(installation_ctx.installation)

    installation_ctx.deployed_workflows.run_workflow("assessment", max_wait=timedelta(minutes=25))
    assert installation_ctx.deployed_workflows.validate_step("assessment")

    after = installation_ctx.generic_permissions_support.load_as_dict("cluster-policies", cluster_policy.policy_id)

    assert after[ws_group.display_name] == PermissionLevel.CAN_USE

    tables = set[str]()
    local_crawler = TablesCrawler(sql_backend, inventory_schema, [source_schema.name])
    for _ in local_crawler.snapshot():
        tables.add(_.name)

    expected_tables = {managed_table.name, external_table.name, tmp_table.name, view.name, non_delta.name}
    assert len(tables) == len(expected_tables)
    assert set(tables) == expected_tables

from datetime import timedelta

from databricks.labs.pytester.fixtures.catalog import make_schema
from databricks.sdk.errors import NotFound, InvalidParameterValue
from databricks.sdk.retries import retried
from databricks.sdk.service.iam import PermissionLevel

from databricks.labs.ucx.hive_metastore import TablesCrawler


@retried(on=[NotFound, InvalidParameterValue])
def test_running_real_assessment_job(
    ws, installation_ctx, make_cluster_policy, make_cluster_policy_permissions, make_job, make_notebook, make_dashboard,
    make_schema, make_table, sql_backend, inventory_schema, populate_for_linting,
):
    ws_group, _ = installation_ctx.make_ucx_group()
    cluster_policy = make_cluster_policy()
    make_cluster_policy_permissions(
        object_id=cluster_policy.policy_id,
        permission_level=PermissionLevel.CAN_USE,
        group_name=ws_group.display_name,
    )
    source_schema = make_schema(catalog_name="hive_metastore")
    managed_table = make_table(schema_name=source_schema.name)
    external_table = make_table(schema_name=source_schema.name, external=True)
    tmp_table = make_table(schema_name=source_schema.name, ctas="SELECT 2+2 AS four")
    view = make_table(schema_name=source_schema.name, ctas="SELECT 2+2 AS four", view=True)
    non_delta = make_table(schema_name=source_schema.name, non_delta=True)

    installation_ctx.__dict__['include_object_permissions'] = [f"cluster-policies:{cluster_policy.policy_id}"]
    installation_ctx.workspace_installation.run()

    populate_for_linting(installation_ctx.installation)

    installation_ctx.deployed_workflows.run_workflow("assessment", max_wait=timedelta(minutes=25))
    assert installation_ctx.deployed_workflows.validate_step("assessment")

    after = installation_ctx.generic_permissions_support.load_as_dict("cluster-policies", cluster_policy.policy_id)

    assert after[ws_group.display_name] == PermissionLevel.CAN_USE


    tables_crawler = TablesCrawler(sql_backend, inventory_schema, [source_schema.name])
    tables_crawled = tables_crawler.snapshot()

    tables = []
    for _ in tables_crawled:
        tables.append(_.name)

    assert len(tables) == 5

    assert managed_table.name in tables
    assert external_table.name in tables
    assert tmp_table.name in tables
    assert view.name in tables
    assert non_delta.name in tables

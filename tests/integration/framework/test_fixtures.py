import logging
from datetime import timedelta, datetime, timezone
import pytest

# pylint: disable-next=import-private-name
from _pytest.outcomes import Failed, Skipped
from databricks.sdk.errors import InvalidParameterValue
from databricks.sdk.service import iam
from databricks.sdk.service.workspace import AclPermission

# pylint: disable-next=unused-wildcard-import,wildcard-import
from databricks.labs.ucx.mixins.fixtures import *  # noqa: F403

logger = logging.getLogger(__name__)
_SPARK_CONF = {
    "spark.databricks.cluster.profile": "singleNode",
}


@pytest.fixture  # type: ignore[no-redef]
def debug_env_name():  # pylint: disable=function-redefined
    return "ucws"


def test_user(make_user):
    logger.info(f"created {make_user()}")


def test_group(make_group, make_user):
    logger.info(f'created {make_group(display_name="abc", members=[make_user().id])}')


def test_secret_scope(make_secret_scope):
    logger.info(f"created {make_secret_scope()}")


def test_secret_scope_acl(make_secret_scope, make_secret_scope_acl, make_group):
    scope_name = make_secret_scope()
    make_secret_scope_acl(scope=scope_name, principal=make_group().display_name, permission=AclPermission.WRITE)


def test_notebook(make_notebook):
    logger.info(f"created {make_notebook()}")


def test_notebook_permissions(make_notebook, make_notebook_permissions, make_group):
    group = make_group()
    notebook = make_notebook()
    acl = make_notebook_permissions(
        object_id=notebook, permission_level=iam.PermissionLevel.CAN_RUN, group_name=group.display_name  # noqa: F405
    )
    logger.info(f"created {acl}")


def test_directory(make_notebook, make_directory):
    logger.info(f'created {make_notebook(path=f"{make_directory()}/foo.py")}')


def test_repo(make_repo):
    logger.info(f"created {make_repo()}")


def test_cluster_policy(make_cluster_policy):
    logger.info(f"created {make_cluster_policy()}")


def test_cluster(make_cluster, env_or_skip):
    logger.info(f"created {make_cluster(single_node=True, instance_pool_id=env_or_skip('TEST_INSTANCE_POOL_ID'))}")


def test_instance_pool(make_instance_pool):
    logger.info(f"created {make_instance_pool()}")


def test_job(make_job):
    logger.info(f"created {make_job()}")


def test_pipeline(make_pipeline):
    logger.info(f"created {make_pipeline()}")


def test_env_or_skip(env_or_skip):
    with pytest.raises((Skipped, Failed)):
        env_or_skip("NO_ENV_VAR_HERE")


def test_catalog_fixture(make_catalog):
    logger.info(f"Created new catalog: {make_catalog()}")
    logger.info(f"Created new catalog: {make_catalog()}")


def test_schema_fixture(make_schema):
    logger.info(f"Created new schema: {make_schema()}")
    logger.info(f"Created new schema: {make_schema()}")


def test_table_fixture(make_table):
    logger.info(f"Created new managed table in new schema: {make_table()}")
    logger.info(f'Created new managed table in default schema: {make_table(schema_name="default")}')
    logger.info(f"Created new external table in new schema: {make_table(external=True)}")
    logger.info(f"Created new external JSON table in new schema: {make_table(non_delta=True)}")
    logger.info(f'Created new tmp table in new schema: {make_table(ctas="SELECT 2+2 AS four")}')
    logger.info(f'Created new view in new schema: {make_table(view=True, ctas="SELECT 2+2 AS four")}')
    logger.info(f'Created table with properties: {make_table(tbl_properties={"test": "tableproperty"})}')


def test_dbfs_fixture(make_mounted_location):
    logger.info(f"Created new dbfs data copy:{make_mounted_location}")


def test_remove_after_tag_jobs(ws, env_or_skip, make_job):
    new_job = make_job(spark_conf=_SPARK_CONF)
    created_job = ws.jobs.get(new_job.job_id)
    job_tags = created_job.settings.tags
    assert "RemoveAfter" in job_tags

    purge_time = datetime.strptime(job_tags["RemoveAfter"], "%Y%m%d%H").replace(tzinfo=timezone.utc)
    assert (purge_time - datetime.now(timezone.utc)) < (TEST_RESOURCE_PURGE_TIMEOUT + timedelta(hours=1))  # noqa: F405


def test_remove_after_tag_clusters(ws, env_or_skip, make_cluster):
    new_cluster = make_cluster(single_node=True, instance_pool_id=env_or_skip('TEST_INSTANCE_POOL_ID'))
    created_cluster = ws.clusters.get(new_cluster.cluster_id)
    cluster_tags = created_cluster.custom_tags
    assert "RemoveAfter" in cluster_tags
    purge_time = datetime.strptime(cluster_tags["RemoveAfter"], "%Y%m%d%H").replace(tzinfo=timezone.utc)
    assert (purge_time - datetime.now(timezone.utc)) < (TEST_RESOURCE_PURGE_TIMEOUT + timedelta(hours=1))  # noqa: F405


def test_remove_after_tag_warehouse(ws, env_or_skip, make_warehouse):
    new_warehouse = make_warehouse()
    created_warehouse = ws.warehouses.get(new_warehouse.response.id)
    warehouse_tags = created_warehouse.tags.as_dict()
    assert warehouse_tags["custom_tags"][0]["key"] == "RemoveAfter"
    remove_after_tag = warehouse_tags["custom_tags"][0]["value"]
    purge_time = datetime.strptime(remove_after_tag, "%Y%m%d%H").replace(tzinfo=timezone.utc)
    assert (purge_time - datetime.now(timezone.utc)) < (TEST_RESOURCE_PURGE_TIMEOUT + timedelta(hours=1))  # noqa: F405


def test_remove_after_tag_instance_pool(ws, make_instance_pool):
    new_instance_pool = make_instance_pool()
    created_instance_pool = ws.instance_pools.get(new_instance_pool.instance_pool_id)
    pool_tags = created_instance_pool.custom_tags
    assert "RemoveAfter" in pool_tags
    purge_time = datetime.strptime(pool_tags["RemoveAfter"], "%Y%m%d%H").replace(tzinfo=timezone.utc)
    assert (purge_time - datetime.now(timezone.utc)) < (TEST_RESOURCE_PURGE_TIMEOUT + timedelta(hours=1))  # noqa: F405


def test_remove_after_property_table(ws, make_table, sql_backend):
    new_table = make_table()
    # TODO: tables.get is currently failing with
    #   databricks.sdk.errors.platform.NotFound: Catalog 'hive_metastore' does not exist.
    sql_response = list(sql_backend.fetch(f"DESCRIBE TABLE EXTENDED {new_table.full_name}"))
    for row in sql_response:
        if row.col_name == "Table Properties":
            assert "RemoveAfter" in row[1]


def test_remove_after_property_schema(ws, make_schema, sql_backend):
    new_schema = make_schema()
    # TODO: schemas.get is currently failing with
    #   databricks.sdk.errors.platform.NotFound: Catalog 'hive_metastore' does not exist.
    sql_response = list(sql_backend.fetch(f"DESCRIBE SCHEMA EXTENDED {new_schema.full_name}"))
    for row in sql_response:
        if row.database_description_item == "Properties":
            assert "RemoveAfter" in row[1]


def test_creating_lakeview_dashboard_permissions(
    make_lakeview_dashboard,
    # The `_permissions` fixtures are generated following a pattern resulting in an argument with too many characters
    make_lakeview_dashboard_permissions,  # pylint: disable=invalid-name
    make_group,
):
    # Only the last permission in the list is visible in the Databricks UI
    permissions = [
        iam.PermissionLevel.CAN_EDIT,
        iam.PermissionLevel.CAN_RUN,
        iam.PermissionLevel.CAN_MANAGE,
        iam.PermissionLevel.CAN_READ,
    ]
    dashboard = make_lakeview_dashboard()
    group = make_group()
    for permission in permissions:
        try:
            make_lakeview_dashboard_permissions(
                object_id=dashboard.dashboard_id,
                permission_level=permission,
                group_name=group.display_name,
            )
        except InvalidParameterValue as e:
            assert False, f"Could not create {permission} permission for lakeview dashboard: {e}"
    assert True, "Could create all fixtures"

import logging

import pytest

# pylint: disable-next=import-private-name
from _pytest.outcomes import Failed, Skipped
from databricks.sdk.service.workspace import AclPermission

# pylint: disable-next=unused-wildcard-import,wildcard-import
from databricks.labs.ucx.mixins.fixtures import *  # noqa: F403

logger = logging.getLogger(__name__)


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

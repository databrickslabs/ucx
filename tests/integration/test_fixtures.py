import logging
import os

from databricks.sdk.service.workspace import AclPermission

from databricks.labs.ucx.providers.mixins.fixtures import *  # noqa: F403

load_debug_env_if_runs_from_ide("ucws")  # noqa: F405

logger = logging.getLogger(__name__)


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


def test_cluster(make_cluster):
    logger.info(f"created {make_cluster(single_node=True, instance_pool_id=os.environ['TEST_INSTANCE_POOL_ID'])}")


def test_instance_pool(make_instance_pool):
    logger.info(f"created {make_instance_pool()}")


def test_job(make_job):
    logger.info(f"created {make_job()}")


def test_pipeline(make_pipeline):
    logger.info(f"created {make_pipeline()}")

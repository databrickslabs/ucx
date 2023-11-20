import logging
import random
from datetime import timedelta
import dataclasses

import pytest
from databricks.sdk.errors import InvalidParameterValue, NotFound, OperationFailed
from databricks.sdk.retries import retried
from databricks.sdk.service import iam
from databricks.sdk.service.catalog import SchemaInfo
from databricks.sdk.service.iam import PermissionLevel

from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.framework.parallel import Threads
from databricks.labs.ucx.hive_metastore.grants import GrantsCrawler
from databricks.labs.ucx.hive_metastore.tables import TablesCrawler
from databricks.labs.ucx.install import WorkspaceInstaller
from databricks.labs.ucx.workspace_access.generic import GenericPermissionsSupport

from .conftest import get_workspace_membership, make_ucx_group
from random import randrange

logger = logging.getLogger(__name__)


def test_performance(
    ws,
    sql_backend,
    make_cluster_policy,
    make_cluster_policy_permissions,
    make_ucx_group,
    make_job,
    make_job_permissions,
    make_random,
    make_schema,
    make_table,
    make_warehouse,
    env_or_skip,
    make_cluster,
    make_instance_pool,
    make_secret_scope,
    make_notebook
):
    #TODO Set and get permissions (unreliable API)
    #TODO Generate graph of permissions

    groups = []
    for i in range(10):
        # TODO Generate entitlements
        ws_group, acc_group = make_ucx_group()
        groups.append((ws_group, acc_group))

    for i in range(10):
        ws_object = make_secret_scope()
        #TODO: create issue -> instancepools in doc but instance-pool is right object_type
        set_permissions(groups, ws_object, "instance_pool_id", "instance-pools", ws, PermissionLevel.CAN_MANAGE)
        set_permissions(groups, ws_object, "instance_pool_id", "instance-pools", ws, PermissionLevel.CAN_ATTACH_TO)


    for i in range(10):
        warehouse = make_warehouse()
        set_permissions(groups, warehouse, "id", "warehouses", ws, PermissionLevel.CAN_MANAGE)
        #set_permissions(groups, warehouse, "id", "warehouses", ws, PermissionLevel.CAN_USE)

    for i in range(10):
        ws_object = make_cluster(single_node=True)
        set_permissions(groups, ws_object, "cluster_id", "clusters", ws, PermissionLevel.CAN_MANAGE)
        #set_permissions(groups, ws_object, "cluster_id", "clusters", ws, PermissionLevel.CAN_RESTART)
        #set_permissions(groups, ws_object, "cluster_id", "clusters", ws, PermissionLevel.CAN_ATTACH_TO)

    for i in range(10):
        ws_object = make_cluster_policy()
        set_permissions(groups, ws_object, "policy_id", "clusterpolicies", ws, PermissionLevel.CAN_USE)


def set_permissions(groups, ws_object, id_attribute, object_type, ws, permission):
    for j in range(5):
        rand = random.randint(1, 9)
        ws_group = groups[rand][0]
        request_object_id = getattr(ws_object, id_attribute)
        ws.permissions.set(request_object_type=object_type,
                           request_object_id=request_object_id,
                           access_control_list=[
                               iam.AccessControlRequest(group_name=ws_group.display_name,
                                                        permission_level=permission)
                           ])
        logger.info(f"Applied {permission.value} to {object_type} : {request_object_id} on {ws_group.display_name}")




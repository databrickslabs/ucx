import datetime
from dataclasses import dataclass
from datetime import timedelta
from functools import partial

from databricks.sdk.retries import retried
from databricks.sdk.service import iam, sql, pipelines, compute, jobs
from databricks.sdk.service.iam import Group, PermissionLevel
from databricks.sdk.service.workspace import (
    AclPermission,
    WorkspaceObjectAccessControlRequest,
    WorkspaceObjectPermissionLevel, ObjectInfo, WorkspaceObjectPermissions,
)

from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.framework.crawlers import StatementExecutionBackend
from databricks.labs.ucx.framework.parallel import Threads
from databricks.labs.ucx.install import WorkspaceInstaller
from databricks.labs.ucx.mixins.hardening import rate_limited
from databricks.labs.ucx.workspace_access.groups import MigratedGroup
import string
from typing import BinaryIO
import io
from databricks.sdk import WorkspaceClient, AccountClient
from databricks.sdk.errors import DatabricksError
from databricks.sdk.service import pipelines, jobs, compute, iam
from databricks.sdk.service.catalog import SchemaInfo, TableInfo, TableType, DataSourceFormat
from databricks.sdk.service.sql import CreateWarehouseRequestWarehouseType, AlertOptions, QueryInfo
import json
import logging
import random


@dataclass
class ObjectPermission:
    group: str
    object_type: str
    object_id: str
    permission: str

@dataclass
class User:
    id:str
    display_name:str

@dataclass
class PerfGroup:
    display_name:str


verificationErrors = []

logging.getLogger("tests").setLevel("DEBUG")

logging.getLogger("databricks.labs.ucx").setLevel("DEBUG")

formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh = logging.FileHandler('spam.log')
fh.setLevel(logging.DEBUG)
fh.setFormatter(formatter)

logging.getLogger("tests").addHandler(fh)
logging.getLogger("databricks.labs.ucx").addHandler(fh)
logger = logging.getLogger(__name__)
logger.addHandler(fh)


def test_performance(
    ws:WorkspaceClient,
    acc:AccountClient,
    sql_backend:StatementExecutionBackend
):
    NB_OF_TEST_WS_OBJECTS = 2
    NB_OF_FILES = 2
    MAX_NB_OF_FILES = 2
    NB_OF_TEST_GROUPS = 2
    NB_OF_SCHEMAS = 1
    MAX_NB_OF_TABLES = 1
    USER_POOL_LENGTH = 5

    test_database = create_or_fetch_test_db(sql_backend, ws)

    if not table_exists(sql_backend, test_database, "users"):
        create_users(USER_POOL_LENGTH, sql_backend, test_database, ws)
    users = []
    for row in sql_backend.fetch(f"SELECT * FROM hive_metastore.{test_database.name}.users"):
        users.append(User(*row))

    if not table_exists(sql_backend, test_database, "groups"):
        create_groups_parallel(NB_OF_TEST_GROUPS, ws, acc, sql_backend, test_database, list(chunks(users, 100))[0])
        #create_big_groups(3, ws, acc, sql_backend, test_database, users)

    groups = []
    for row in sql_backend.fetch(f"SELECT * FROM hive_metastore.{test_database.name}.groups"):
        groups.append(PerfGroup(display_name=row["display_name"]))

    schemas = create_schemas_parallel(NB_OF_SCHEMAS, groups, sql_backend, test_database, ws)
    create_tables_parallel(MAX_NB_OF_TABLES, schemas, groups, sql_backend, test_database, ws)
    create_views_parallel(MAX_NB_OF_TABLES, schemas, groups, sql_backend, test_database, ws)

    dirs = create_dirs_parallel(NB_OF_FILES, groups, sql_backend, test_database, ws)
    create_notebooks_parallel(MAX_NB_OF_FILES, dirs, groups, sql_backend, test_database, ws)

    create_scopes(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws)

    create_pipelines(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws)
    # create_ws_objects_parallel(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws, create_dlt,
    #                            [PermissionLevel.CAN_VIEW, PermissionLevel.CAN_MANAGE, PermissionLevel.CAN_RUN],
    #                            "pipeline_id", "pipelines")

    create_jobs(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws)
    # create_ws_objects_parallel(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws, create_job,
    #                            [PermissionLevel.CAN_VIEW, PermissionLevel.CAN_MANAGE, PermissionLevel.CAN_MANAGE_RUN],
    #                            "job_id", "jobs")


    create_experiments(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws)
    # create_ws_objects_parallel(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws, create_experiment,
    #                            [PermissionLevel.CAN_READ, PermissionLevel.CAN_MANAGE, PermissionLevel.CAN_EDIT],
    #                            "experiment_id", "experiments")

    create_models(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws)
    # create_ws_objects_parallel(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws, create_model,
    #                            [
    #                                PermissionLevel.CAN_READ,
    #                                PermissionLevel.CAN_MANAGE,
    #                                PermissionLevel.CAN_EDIT,
    #                                PermissionLevel.CAN_MANAGE_PRODUCTION_VERSIONS,
    #                                PermissionLevel.CAN_MANAGE_STAGING_VERSIONS,
    #                            ],
    #                            "id", "registered-models")

    create_pools(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws)
    # create_ws_objects_parallel(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws, create_pool,
    #                            [PermissionLevel.CAN_MANAGE, PermissionLevel.CAN_ATTACH_TO],
    #                            "instance_pool_id", "instance-pools")

    create_warehouses(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws)
    # create_ws_objects_parallel(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws, create_warehouse,
    #                            [PermissionLevel.CAN_MANAGE, PermissionLevel.CAN_USE],
    #                            "id", "warehouses")

    create_clusters(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws)
    # create_ws_objects_parallel(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws, create_cluster,
    #                            [PermissionLevel.CAN_MANAGE, PermissionLevel.CAN_RESTART, PermissionLevel.CAN_ATTACH_TO],
    #                            "cluster_id", "clusters")

    create_policies(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws)
    # create_ws_objects_parallel(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws, create_policy,
    #                            [PermissionLevel.CAN_USE],
    #                            "policy_id", "cluster-policies")

    queries = create_queries_parallel(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws)
    create_alerts_parallel(queries, groups, sql_backend, test_database, ws)

    create_dashboards(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws)

    create_repos(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws)

    backup_group_prefix = "db-temp-"
    inventory_database = f"ucx_{make_random(4)}"
    test_groups = [_.display_name for _ in groups]

    install = WorkspaceInstaller.run_for_config(
       ws,
       WorkspaceConfig(
           inventory_database=inventory_database,
           include_group_names=test_groups,
           renamed_group_prefix=backup_group_prefix,
           log_level="DEBUG",
       ),
       sql_backend=sql_backend,
       prefix=make_random(4),
    )

    required_workflows = ["assessment", "migrate-groups", "remove-workspace-local-backup-groups"]
    for step in required_workflows:
        install.run_workflow(step)

    try_validate_object(sql_backend, test_database, test_groups, ws, "pipelines")
    try_validate_object(sql_backend, test_database, test_groups, ws, "jobs")
    try_validate_object(sql_backend, test_database, test_groups, ws, "experiments")
    try_validate_object(sql_backend, test_database, test_groups, ws, "registered-models")
    try_validate_object(sql_backend, test_database, test_groups, ws, "instance-pools")
    try_validate_object(sql_backend, test_database, test_groups, ws, "warehouses")
    try_validate_object(sql_backend, test_database, test_groups, ws, "clusters")
    try_validate_object(sql_backend, test_database, test_groups, ws, "cluster-policies")
    try_validate_sql_objects(sql_backend, test_database, test_groups, ws, sql.ObjectTypePlural.ALERTS)
    try_validate_sql_objects(
        sql_backend, test_database, test_groups, ws, sql.ObjectTypePlural.DASHBOARDS
    )
    try_validate_sql_objects(sql_backend, test_database, test_groups, ws, sql.ObjectTypePlural.QUERIES)

    try_validate_files(sql_backend, test_database, test_groups, ws, "notebooks")
    try_validate_files(sql_backend, test_database, test_groups, ws, "directories")

    try_validate_tables( sql_backend, test_database, test_groups, "SCHEMA")
    try_validate_tables( sql_backend, test_database, test_groups, "TABLE")
    try_validate_tables( sql_backend, test_database, test_groups, "VIEW")

    try_validate_secrets(sql_backend, test_database, ws)
    validate_entitlements(sql_backend, test_database, ws)

    if len(verificationErrors) > 0:
        with open(f"perf-test-{datetime.datetime.now()}.txt", "w") as txt_file:
            for line in verificationErrors:
                txt_file.write(line + "\n")
    assert [] == verificationErrors


def table_exists(sql_backend, test_database, table_name):
    try:
        list(sql_backend.fetch(f"SELECT count(*) FROM {test_database.name}.{table_name}"))[0]
        logger.info(f"Table {table_name} exist, skipping this part")
        return True
    except:
        logger.info(f"Table {table_name} do not exist, populating objects")
        return False


def create_or_fetch_test_db(sql_backend, ws):
    dbs = []
    for db_name in sql_backend.fetch(f"SHOW DATABASES IN hive_metastore"):
        dbs.append(db_name["databaseName"])
    if "test_results" not in dbs:
        logger.debug("Creating test database test_results")
        test_database = create_schema(sql_backend, ws, name=f"test_results")
    else:
        logger.debug("Test database already exists")
        test_database = SchemaInfo(name="test_results")
    return test_database


def get_persisted_rows(sql_backend, test_database, object_type):
    persisted_rows = {}
    for object_id, group_name, permission in sql_backend.fetch(
            f"SELECT distinct object_id, group, permission FROM {test_database.name}.objects where object_type = '{object_type}'"
    ):
        if object_id in persisted_rows:
            previous_perms = persisted_rows[object_id]
            if group_name in previous_perms:
                previous_perms[group_name] = [permission] + previous_perms[group_name]
            else:
                previous_perms[group_name] = [permission]
            persisted_rows[object_id] = previous_perms
        else:
            persisted_rows[object_id] = {group_name: [permission]}

    return persisted_rows

def create_schemas_parallel(NB_OF_SCHEMAS, groups, sql_backend, test_database, ws):
    create_schema_task = []
    for i in range(NB_OF_SCHEMAS):
        create_schema_task.append(partial(create_schema, sql_backend, ws))

    schemas, errors = Threads.gather(f"schema creation", create_schema_task)
    logger.warning(f"Had {len(errors)} error while creating schemas")

    apply_permission_task = []
    for schema in schemas:
        apply_permission_task.append(partial(apply_schema_permissions, groups, schema, sql_backend))

    schema_permissions, errors = Threads.gather(f"schema_permissions", apply_permission_task)
    logger.warning(f"Had {len(errors)} error while applying permissions on schemas")

    permissions_flatten = [grant for grants in schema_permissions for grant in grants]
    sql_backend.save_table(f"{test_database.name}.objects", permissions_flatten, ObjectPermission)
    return schemas


def apply_schema_permissions(groups, schema, sql_backend) -> [ObjectPermission]:
    schema_permissions = create_hive_metastore_permissions(
        groups, ["SELECT", "MODIFY", "READ_METADATA", "CREATE", "USAGE"]
    )
    grant_permissions(sql_backend, "SCHEMA", schema.name, schema_permissions)
    owner = groups[random.randint(0, len(groups) - 1)].display_name
    transfer_ownership(sql_backend, "SCHEMA", schema.name, owner)

    to_persist = []
    for group, permissions in schema_permissions.items():
        for permission in permissions:
            to_persist.append(ObjectPermission(group, "SCHEMA", schema.name, permission))
    return to_persist


def create_tables_parallel(MAX_NB_OF_TABLES, schemas, groups, sql_backend, test_database, ws):
    create_table_task = []

    for schema in schemas:
        nb_of_tables = random.randint(1, MAX_NB_OF_TABLES)
        for j in range(nb_of_tables):
            create_table_task.append(partial(create_table, sql_backend, ws, schema_name=schema.name))

    tables, errors = Threads.gather(f"table creation", create_table_task)
    logger.warning(f"Had {len(errors)} error while creating tables")

    apply_permission_task = []
    for table in tables:
        apply_permission_task.append(partial(apply_table_permission, groups, sql_backend, table, test_database))

    table_permissions, errors = Threads.gather(f"table permissions", apply_permission_task)
    logger.warning(f"Had {len(errors)} error while creating tables")


def apply_table_permission(groups, sql_backend, table, test_database):
    full_name = table.schema_name + "." + table.name
    table_permission = create_hive_metastore_permissions(groups, ["SELECT", "MODIFY", "READ_METADATA"])
    grant_permissions(sql_backend, "TABLE", full_name, table_permission)
    owner = groups[random.randint(0, len(groups) - 1)].display_name
    transfer_ownership(sql_backend, "TABLE", full_name, owner)

    to_persist = []
    for group, permissions in table_permission.items():
        for permission in permissions:
            to_persist.append(ObjectPermission(group, "TABLE", full_name, permission))

    sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)
    return to_persist


def create_views_parallel(MAX_NB_OF_TABLES, schemas, groups, sql_backend, test_database, ws):
    create_view_task = []

    for schema in schemas:
        nb_of_tables = random.randint(1, MAX_NB_OF_TABLES)
        for j in range(nb_of_tables):
            create_view_task.append(partial(create_table, sql_backend, ws, schema_name=schema.name, view=True, ctas="SELECT 2+2 AS four"))

    views, errors = Threads.gather(f"view creation", create_view_task)
    logger.warning(f"Had {len(errors)} error while creating views")

    apply_permission_task = []
    for view in views:
        apply_permission_task.append(partial(apply_view_permissions, groups, sql_backend, view, test_database))

    view_permissions, errors = Threads.gather(f"view permissions", apply_permission_task)
    logger.warning(f"Had {len(errors)} error while creating views")


def apply_view_permissions(groups, sql_backend, view, test_database):
    full_name = view.schema_name + "." + view.name
    view_permission = create_hive_metastore_permissions(groups, ["SELECT"])
    grant_permissions(sql_backend, "VIEW", full_name, view_permission)
    owner = groups[random.randint(0, len(groups) - 1)].display_name
    transfer_ownership(sql_backend, "VIEW", full_name, owner)

    to_persist = []
    for group, permissions in view_permission.items():
        for permission in permissions:
            to_persist.append(ObjectPermission(group, "VIEW", full_name, permission))
    to_persist.append(ObjectPermission(owner, "VIEW", full_name, "OWN"))
    sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)
    return to_persist

def create_dirs_parallel(
    NB_OF_FILES, groups, sql_backend, test_database, ws
):
    dir_tasks = []
    for i in range(NB_OF_FILES):
        dir_tasks.append(partial(create_directory, ws))

    directories, errors = Threads.gather(f"create directories", dir_tasks)
    logger.warning(f"Had {len(errors)} error while creating directories")

    dir_permissions_tasks = []
    for directory in directories:
        dir_permissions_tasks.append(partial(assign_dir_permissions, groups, directory, test_database, sql_backend, ws))

    directory_permissions, errors = Threads.gather(f"assign directories permissions", dir_permissions_tasks)
    logger.warning(f"Had {len(errors)} error while assigning directories permissions")

    return directories


def create_directory(ws:WorkspaceClient):
    test_dir = create_dir(ws)
    logger.info(f"Created dir {test_dir}")
    return ws.workspace.get_status(test_dir)


def assign_dir_permissions(groups, stat, test_database, sql_backend, ws:WorkspaceClient) -> [ObjectPermission]:
    dir_perms = create_permissions(
        groups,
        [
            WorkspaceObjectPermissionLevel.CAN_MANAGE,
            WorkspaceObjectPermissionLevel.CAN_RUN,
            WorkspaceObjectPermissionLevel.CAN_EDIT,
            WorkspaceObjectPermissionLevel.CAN_READ,
        ],
    )
    assign_ws_local_permissions("directories", dir_perms, stat.object_id, ws)
    to_persist = []
    for group, permission in dir_perms.items():
        to_persist.append(ObjectPermission(group, "directories", stat.object_id, permission.value))

    sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)
    return to_persist

def create_notebooks_parallel(MAX_NB_OF_FILES, directories, groups, sql_backend, test_database, ws):
    notebook_tasks = []
    for dir_path in directories:
        nb_of_notebooks = random.randint(1, MAX_NB_OF_FILES)
        for j in range(nb_of_notebooks):
            notebook_tasks.append(partial(do_notebook, ws, dir_path))

    notebooks, errors = Threads.gather(f"create notebooks", notebook_tasks)
    logger.warning(f"Had {len(errors)} error while creating notebooks")

    nb_permissions_tasks = []
    for notebook in notebooks:
        nb_permissions_tasks.append(partial(notebook_permissions, notebook, groups, ws, sql_backend, test_database))

    notebook_perms, errors = Threads.gather(f"assign notebook permissions", nb_permissions_tasks)
    logger.warning(f"Had {len(errors)} error while assigning notebook permissions")

def do_notebook(ws, test_dir:ObjectInfo):
    nb = create_notebook(ws, path=test_dir.path + "/" + make_random() + ".py")
    logger.info(f"Created notebook {nb}")
    return ws.workspace.get_status(nb)

def notebook_permissions(nb_stat, groups, ws, sql_backend, test_database):
    nb_perms = create_permissions(
        groups,
        [
            WorkspaceObjectPermissionLevel.CAN_MANAGE,
            WorkspaceObjectPermissionLevel.CAN_RUN,
            WorkspaceObjectPermissionLevel.CAN_EDIT,
            WorkspaceObjectPermissionLevel.CAN_READ,
        ],
    )
    assign_ws_local_permissions("notebooks", nb_perms, nb_stat.object_id, ws)
    logger.debug(f"Assigned permission on notebook {nb_stat.path}")

    to_persist=[]
    for group, permission in nb_perms.items():
        to_persist.append(ObjectPermission(group, "notebooks", nb_stat.object_id, permission.value))

    sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)
    return to_persist

def create_dirs_n_notebookes(
    NB_OF_FILES, MAX_NB_OF_FILES, groups, sql_backend, test_database, ws
):
    to_persist = []
    for i in range(NB_OF_FILES):
        test_dir = create_dir(ws)
        stat = ws.workspace.get_status(test_dir)
        dir_perms = create_permissions(
            groups,
            [
                WorkspaceObjectPermissionLevel.CAN_MANAGE,
                WorkspaceObjectPermissionLevel.CAN_RUN,
                WorkspaceObjectPermissionLevel.CAN_EDIT,
                WorkspaceObjectPermissionLevel.CAN_READ,
            ],
        )
        assign_ws_local_permissions("directories", dir_perms, stat.object_id, ws)
        for group, permission in dir_perms.items():
            to_persist.append(ObjectPermission(group, "directories", stat.object_id, permission.value))
        logger.info(f"Created directory {test_dir}")
        nb_of_notebooks = random.randint(1, MAX_NB_OF_FILES)
        logger.info(f"Creating {nb_of_notebooks} notebooks on directory {test_dir}")
        for j in range(nb_of_notebooks):
            nb = create_notebook(ws, path=test_dir + "/" + make_random() + ".py")
            nb_stat = ws.workspace.get_status(nb)
            nb_perms = create_permissions(
                groups,
                [
                    WorkspaceObjectPermissionLevel.CAN_MANAGE,
                    WorkspaceObjectPermissionLevel.CAN_RUN,
                    WorkspaceObjectPermissionLevel.CAN_EDIT,
                    WorkspaceObjectPermissionLevel.CAN_READ,
                ],
            )
            assign_ws_local_permissions("notebooks", nb_perms, nb_stat.object_id, ws)
            for group, permission in nb_perms.items():
                to_persist.append(ObjectPermission(group, "notebooks", nb_stat.object_id, permission.value))
            logger.info(f"Created notebook {nb}")
    sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)


def create_repos(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws):
    for j in range(NB_OF_TEST_WS_OBJECTS):
        to_persist = []
        repo = create_repo(ws)
        repo_perms = create_permissions(
            groups,
            [
                WorkspaceObjectPermissionLevel.CAN_MANAGE,
                WorkspaceObjectPermissionLevel.CAN_RUN,
                WorkspaceObjectPermissionLevel.CAN_EDIT,
                WorkspaceObjectPermissionLevel.CAN_READ,
            ],
        )
        assign_ws_local_permissions("repos", repo_perms, repo.id, ws)
        for group, permission in repo_perms.items():
            to_persist.append(ObjectPermission(group, "repos", repo.id, permission.value))
        logger.info(f"Created repo {repo.id}")
        sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)


def create_dashboards(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws):
    for j in range(NB_OF_TEST_WS_OBJECTS):
        to_persist = []
        dashboard = create_dashboard(ws)
        dashboard_permissions = create_permissions(
            groups, [sql.PermissionLevel.CAN_MANAGE, sql.PermissionLevel.CAN_RUN, sql.PermissionLevel.CAN_VIEW]
        )
        set_dbsql_permissions(dashboard, "id", sql.ObjectTypePlural.DASHBOARDS, ws, dashboard_permissions)
        for group, permission in dashboard_permissions.items():
            to_persist.append(
                ObjectPermission(group, sql.ObjectTypePlural.DASHBOARDS.value, dashboard.id, permission.value)
            )
        logger.info(f"Created dashboard {dashboard.id}")
        sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)

def create_queries_parallel(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws):
    tasks = []
    for i in range(NB_OF_TEST_WS_OBJECTS):
        tasks.append(partial(create_query, ws))

    queries, failure = Threads.gather("Query creation", tasks)
    logger.warning(f"Had {len(failure)} when creating queries")

    apply_tasks = []
    for query in queries:
        apply_tasks.append(partial(apply_query_permissions, query, groups, sql_backend, test_database, ws))

    permissions, failure = Threads.gather("Apply query permissions", apply_tasks)
    logger.warning(f"Had {len(failure)} when applying queries")
    return queries


def apply_query_permissions(query, groups, sql_backend, test_database, ws):
    to_persist = []
    query_permissions = create_permissions(
        groups, [sql.PermissionLevel.CAN_MANAGE, sql.PermissionLevel.CAN_RUN, sql.PermissionLevel.CAN_VIEW]
    )
    set_dbsql_permissions(query, "id", sql.ObjectTypePlural.QUERIES, ws, query_permissions)
    for group, permission in query_permissions.items():
        to_persist.append(ObjectPermission(group, sql.ObjectTypePlural.QUERIES.value, query.id, permission.value))
    logger.info(f"Created query {query.id}")
    sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)


def create_alerts_parallel(queries, ws, groups, sql_backend, test_database):
    alert_task = []
    for query in queries:
        nb_of_alerts = random.randint(1, 5)
        for j in range(nb_of_alerts):
            alert_task.append(partial(create_alert, query_id=query.id, ws=ws))

    alerts, failure = Threads.gather("create alerts", alert_task)
    logger.warning(f"Had {len(failure)} errors when creating alerts")

    alert_permissions = []
    for alert in alerts:
        alert_permissions.append(partial(create_alert_task, groups, alert, sql_backend, test_database, ws))

    success, failure = Threads.gather("Apply alerts permission", alert_permissions)
    logger.warning(f"Had {len(failure)} errors when creating alerts")


def create_alert_task(groups, alert, sql_backend, test_database, ws):
    to_persist = []
    alert_permissions = create_permissions(
        groups, [sql.PermissionLevel.CAN_MANAGE, sql.PermissionLevel.CAN_RUN, sql.PermissionLevel.CAN_VIEW]
    )
    set_dbsql_permissions(alert, "id", sql.ObjectTypePlural.ALERTS, ws, alert_permissions)
    for group, permission in alert_permissions.items():
        to_persist.append(
            ObjectPermission(group, sql.ObjectTypePlural.ALERTS.value, alert.id, permission.value)
        )
    sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)
    logger.info(f"Created alert {alert.id}")



def create_policies(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws):
    for i in range(NB_OF_TEST_WS_OBJECTS):
        to_persist = []
        ws_object = create_policy(ws)
        ws_permissions = create_permissions(groups, [PermissionLevel.CAN_USE])
        set_permissions(ws_object, "policy_id", "cluster-policies", ws, ws_permissions)
        for group, permission in ws_permissions.items():
            to_persist.append(ObjectPermission(group, "cluster-policies", ws_object.policy_id, permission.value))
        logger.info(f"Created policy {ws_object.policy_id}")
        sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)


def create_clusters(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws):
    for i in range(NB_OF_TEST_WS_OBJECTS):
        to_persist = []
        ws_object = create_cluster(ws)
        ws_permissions = create_permissions(
            groups, [PermissionLevel.CAN_MANAGE, PermissionLevel.CAN_RESTART, PermissionLevel.CAN_ATTACH_TO]
        )
        set_permissions(ws_object, "cluster_id", "clusters", ws, ws_permissions)
        for group, permission in ws_permissions.items():
            to_persist.append(ObjectPermission(group, "clusters", ws_object.cluster_id, permission.value))
        logger.info(f"Created cluster {ws_object.cluster_id}")
        sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)


def create_warehouses(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws):
    for i in range(NB_OF_TEST_WS_OBJECTS):
        to_persist = []
        warehouse = create_warehouse(ws)
        ws_permissions = create_permissions(groups, [PermissionLevel.CAN_MANAGE, PermissionLevel.CAN_USE])
        set_permissions(warehouse, "id", "warehouses", ws, ws_permissions)
        for group, permission in ws_permissions.items():
            to_persist.append(ObjectPermission(group, "warehouses", warehouse.id, permission.value))
        logger.info(f"Created warehouse {warehouse.id}")
        sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)


def create_pools(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws):
    for i in range(NB_OF_TEST_WS_OBJECTS):
        to_persist = []
        ws_object = create_pool(ws)
        ws_permissions = create_permissions(groups, [PermissionLevel.CAN_MANAGE, PermissionLevel.CAN_ATTACH_TO])
        set_permissions(ws_object, "instance_pool_id", "instance-pools", ws, ws_permissions)
        for group, permission in ws_permissions.items():
            to_persist.append(ObjectPermission(group, "instance-pools", ws_object.instance_pool_id, permission.value))
        logger.info(f"Created pool {ws_object.instance_pool_id}")
        sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)


def create_models(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws):
    for i in range(NB_OF_TEST_WS_OBJECTS):
        to_persist = []
        ws_object = create_model(ws)
        ws_permissions = create_permissions(
            groups,
            [
                PermissionLevel.CAN_READ,
                PermissionLevel.CAN_MANAGE,
                PermissionLevel.CAN_EDIT,
                PermissionLevel.CAN_MANAGE_PRODUCTION_VERSIONS,
                PermissionLevel.CAN_MANAGE_STAGING_VERSIONS,
            ],
        )
        set_permissions(ws_object, "id", "registered-models", ws, ws_permissions)
        for group, permission in ws_permissions.items():
            to_persist.append(ObjectPermission(group, "registered-models", ws_object.id, permission.value))
        logger.info(f"Created model {ws_object.id}")
        sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)


def create_experiments(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws):
    for i in range(NB_OF_TEST_WS_OBJECTS):
        to_persist = []
        ws_object = create_experiment(ws)
        ws_permissions = create_permissions(
            groups, [PermissionLevel.CAN_READ, PermissionLevel.CAN_MANAGE, PermissionLevel.CAN_EDIT]
        )
        set_permissions(ws_object, "experiment_id", "experiments", ws, ws_permissions)
        for group, permission in ws_permissions.items():
            to_persist.append(ObjectPermission(group, "experiments", ws_object.experiment_id, permission.value))
        logger.info(f"Created experiment {ws_object.experiment_id}")
        sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)

def create_ws_objects_parallel(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws, fixture, all_permissions, id_attribute, object_type):
    fixture_tasks = []
    for i in range(NB_OF_TEST_WS_OBJECTS):
        fixture_tasks.append(partial(fixture, ws))

    success, failure = Threads.gather(f"Creation of {object_type} ", fixture_tasks)
    logger.warning(f"Had {len(failure)} when creating objects")

    ws_object_permissions_tasks = []
    for ws_object in success:
        ws_object_permissions_tasks.append(partial(ws_object_permissions, groups, all_permissions, ws_object, id_attribute, object_type, ws, sql_backend, test_database))

    success, failure = Threads.gather(f"{object_type} permissions", ws_object_permissions_tasks)
    logger.warning(f"Had {len(failure)} when applying object permissions")


def ws_object_permissions(groups, all_permissions, ws_object, id_attribute, object_type, ws, sql_backend, test_database):
    ws_permissions = create_permissions(
        groups, all_permissions
    )
    to_persist = []
    set_permissions(ws_object, id_attribute, object_type, ws, ws_permissions)
    object_id = getattr(ws, id_attribute)
    for group, permission in ws_permissions.items():
        to_persist.append(ObjectPermission(group, object_type, object_id, permission.value))
    logger.info(f"Created {object_type} {object_id}")
    sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)

def create_jobs(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws):
    for i in range(NB_OF_TEST_WS_OBJECTS):
        to_persist = []
        ws_object = create_job(ws)
        ws_permissions = create_permissions(
            groups, [PermissionLevel.CAN_VIEW, PermissionLevel.CAN_MANAGE, PermissionLevel.CAN_MANAGE_RUN]
        )
        set_permissions(ws_object, "job_id", "jobs", ws, ws_permissions)
        for group, permission in ws_permissions.items():
            to_persist.append(ObjectPermission(group, "jobs", ws_object.job_id, permission.value))
        logger.info(f"Created jobs {ws_object.job_id}")
        sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)


def create_pipelines(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws):
    for i in range(NB_OF_TEST_WS_OBJECTS):
        to_persist = []
        ws_object = create_dlt(ws)
        ws_permissions = create_permissions(
            groups, [PermissionLevel.CAN_VIEW, PermissionLevel.CAN_MANAGE, PermissionLevel.CAN_RUN]
        )
        set_permissions(ws_object, "pipeline_id", "pipelines", ws, ws_permissions)
        for group, permission in ws_permissions.items():
            to_persist.append(ObjectPermission(group, "pipelines", ws_object.pipeline_id, permission.value))
        logger.info(f"Created pipeline {ws_object.pipeline_id}")
        sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)


def create_scopes(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws: WorkspaceClient):
    for i in range(NB_OF_TEST_WS_OBJECTS):
        to_persist = []
        scope = create_scope(ws)
        ws_permissions = create_permissions(groups, [AclPermission.MANAGE, AclPermission.READ, AclPermission.WRITE])
        set_secrets_permissions(scope, ws, ws_permissions)
        for group, permission in ws_permissions.items():
            to_persist.append(ObjectPermission(group, "secrets", scope, permission.value))
        logger.info(f"Created scope {scope}")
        sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)


def create_big_groups(NB_OF_TEST_GROUPS, ws:WorkspaceClient, acc:AccountClient, sql_backend, test_database, user_pool):
    to_persist = []
    for i in range(NB_OF_TEST_GROUPS):
        entitlements_list = [
            "workspace-access",
            "databricks-sql-access",
            "allow-cluster-create",
            "allow-instance-pool-create",
        ]
        entitlements = [iam.ComplexValue(value=_) for _ in random.choices(entitlements_list, k=random.randint(1, 3))]

        members = []
        for user in user_pool:
            members.append(iam.ComplexValue(value=user.id))

        group_name = f"ucx_{make_random(6)}"
        logger.info(f"Creating big group {group_name} with {len(members)} members")
        grp = do_ws_group(ws, group_name, [], entitlements)
        acc_grp = do_acc_group(acc, group_name, [])

        for chunk in chunks(members, 100):
            logger.debug(f"Adding 100 users to ws group {group_name}")
            ws.groups.patch(
                grp.id,
                operations=[iam.Patch(op=iam.PatchOp.ADD,path="members",value=[x.as_dict() for x in chunk])],
                schemas=[iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
            )
            logger.debug(f"Adding 100 users to acc group {group_name}")
            acc.groups.patch(
                acc_grp.id,
                operations=[iam.Patch(op=iam.PatchOp.ADD,path="members",value=[x.as_dict() for x in chunk])],
                schemas=[iam.PatchSchema.URN_IETF_PARAMS_SCIM_API_MESSAGES_2_0_PATCH_OP],
            )
        to_persist.append(PerfGroup(grp.display_name))

    sql_backend.save_table(f"{test_database.name}.groups", to_persist, PerfGroup)

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def create_groups_parallel(NB_OF_TEST_GROUPS, ws, acc, sql_backend, test_database, user_pool):
    tasks = []
    for i in range(NB_OF_TEST_GROUPS):
        entitlements_list = [
            "workspace-access",
            "databricks-sql-access",
            "allow-cluster-create",
            "allow-instance-pool-create",
        ]
        entitlements = [iam.ComplexValue(value=_) for _ in random.choices(entitlements_list, k=random.randint(1, 3))]

        nb_of_members = random.randint(1, len(user_pool) - 1)
        members = []
        for j in range(nb_of_members):
            user_index_in_list = random.randint(0, len(user_pool) - 1)
            members.append(iam.ComplexValue(value=user_pool[user_index_in_list].id))

        group_name = f"ucx_{make_random(6)}"
        logger.info(f"Creating group {group_name} with {len(members)} members")
        tasks.append(partial(do_ws_group, ws, group_name, members, entitlements))
        tasks.append(partial(do_acc_group, acc, group_name, members))

    groups, failures = Threads.gather("Group creation", tasks)

    grp_names = set([group.display_name for group in groups])
    to_persist = [PerfGroup(grp_name) for grp_name in grp_names]

    sql_backend.save_table(f"{test_database.name}.groups", to_persist, PerfGroup)


@rate_limited(max_requests=35, burst_period_seconds=60)
def do_ws_group(ws:WorkspaceClient, name, members, entitlements):
    return ws.groups.create(display_name=name, entitlements=entitlements, members=members)


@rate_limited(max_requests=5, burst_period_seconds=1)
def do_acc_group(acc:AccountClient, name, members):
    return acc.groups.create(display_name=name, members=members)


def create_users(MAX_GRP_USERS, sql_backend, test_database, ws:WorkspaceClient):
    tasks = []
    for i in range(MAX_GRP_USERS):
        tasks.append(partial(create_user, ws))
    users, errors = Threads.gather(f"User creation", tasks)

    if len(errors) > 0:
        logger.error(f"Detected {len(errors)} while scanning tables")

    to_persist = []
    for user in users:
        to_persist.append(User(user.id, user.display_name))
    sql_backend.save_table(f"{test_database.name}.users", users, User)


def try_validate_object(persisted_rows, sql_backend, test_database, test_groups, ws):
    try:
        validate_objects(persisted_rows, sql_backend, test_database, test_groups, ws)
    except Exception as e:
        logger.warning(f"Something wrong happened when asserting objects -> {e}")


def validate_objects(sql_backend, test_database, test_groups, ws: WorkspaceClient, object_type):
    persisted_rows = get_persisted_rows(sql_backend, test_database, object_type)

    for pipe_id in sql_backend.fetch(
        f"SELECT distinct object_id FROM {test_database.name}.objects where object_type = '{object_type}'"
    ):
        obj_id = pipe_id["object_id"]
        acls = ws.permissions.get(object_type, obj_id).access_control_list
        for acl in acls:
            if acl.group_name in ["users", "admins", "account users"]:
                continue
            if acl.group_name not in test_groups:
                continue

            validate_that(
                len(acl.all_permissions) == 1,
                f"More than 1 permission found in {object_type} {obj_id} -> {json.dumps(acl.as_dict())}",
            )
            validate_that(
                acl.group_name in persisted_rows[obj_id],
                f"{acl.group_name} not found in persisted rows for {object_type} {obj_id}",
            )
            validate_that(
                acl.all_permissions[0].permission_level.value in persisted_rows[obj_id][acl.group_name],
                f"{acl.all_permissions[0].permission_level.value} not found in persisted rows for {object_type} {obj_id} and group {acl.group_name}",
            )


def validate_entitlements(sql_backend, test_database, ws: WorkspaceClient):
    for row in sql_backend.fetch(f"SELECT * FROM {test_database.name}.groups"):
        mggrp = MigratedGroup(*row)
        try:
            migrated_group = ws.groups.get(mggrp.external_id)
        except Exception as e:
            logger.warning(f"There was a problem when fetching migrated group {mggrp.external_id}")
            logger.warning(e)
            continue

        target_entitlements = (
            json.dumps([gg.as_dict() for gg in migrated_group.entitlements]) if migrated_group.entitlements else None
        )
        validate_that(
            target_entitlements == mggrp.entitlements,
            f"Migrated group {mggrp.name_in_workspace} does not have the same entitlements as the one in the account \n"
            f"previous group = {mggrp.entitlements} \n"
            f"new group = {target_entitlements}",
        )


def try_validate_secrets( sql_backend, test_database, ws):
    try:
        validate_secrets( sql_backend, test_database, ws)
    except Exception as e:
        logger.warning(f"Something wrong happened when asserting objects -> {e}")


def validate_secrets(sql_backend, test_database, ws: WorkspaceClient):
    persisted_rows = get_persisted_rows(sql_backend, test_database, "secrets")
    for pipe_id in sql_backend.fetch(
        f"SELECT distinct group, object_id FROM {test_database.name}.objects where object_type = 'secrets'"
    ):
        obj_id = pipe_id["object_id"]
        group = pipe_id["group"]
        logger.debug(f"Validating secret {obj_id}")
        try:
            acl = ws.secrets.get_acl(obj_id, group)
        except Exception as e:
            logger.warning(f"Could not get secret {obj_id} for group {group}")
            logger.warning(e)
            continue

        validate_that(
            acl.permission.value in persisted_rows[obj_id][acl.principal],
            f"permission {acl.permission.value} not found for scope {obj_id} and group {group}",
        )


def try_validate_tables(sql_backend, test_database, test_groups, object_type):
    logger.info(f"Validating {object_type}")
    try:
        validate_tables(sql_backend, test_database, test_groups, object_type)
    except RuntimeError as e:
        logger.warning(f"Something wrong happened when asserting tables -> {e}")


def validate_tables(sql_backend, test_database, test_groups, object_type):
    persisted_rows = get_persisted_rows(sql_backend, test_database, object_type)
    validation_tasks = []
    for pipe_id in sql_backend.fetch(
        f"SELECT distinct object_id FROM {test_database.name}.objects where object_type = '{object_type}'"
    ):
        obj_id = pipe_id["object_id"]
        logger.debug(f"Validating {object_type} {obj_id}")
        validation_tasks.append(partial(validate_table, obj_id, object_type, persisted_rows, sql_backend, test_groups))

    succ, errors = Threads.gather("Table validation", validation_tasks)
    logger.warning(f"There was {len(errors)} while validating tables ")


def validate_table(obj_id, object_type, persisted_rows, sql_backend, test_groups):
    for row in sql_backend.fetch(f"SHOW GRANTS ON {object_type} {obj_id}"):
        (principal, action_type, remote_object_type, _) = row
        if principal in ["users", "admins", "account users"]:
            continue
        if principal not in test_groups:
            continue
        if remote_object_type != object_type:
            continue
        validate_that(principal in persisted_rows[obj_id], f"{principal} not found in {object_type} {obj_id}")
        validate_that(
            action_type in persisted_rows[obj_id][principal],
            f"{principal} does not have {action_type} permission on {object_type} {obj_id}",
        )


def try_validate_sql_objects(sql_backend, test_database, test_groups, ws, object_type):
    try:
        validate_sql_objects(sql_backend, test_database, test_groups, ws, object_type)
    except Exception as e:
        logger.warning(f"Something wrong happened when asserting SQL objects -> {e}")


def validate_sql_objects(sql_backend, test_database, test_groups, ws, object_type):
    persisted_rows = get_persisted_rows(sql_backend, test_database, object_type)
    for pipe_id in sql_backend.fetch(
        f"SELECT distinct object_id FROM {test_database.name}.objects where object_type = '{object_type.value}'"
    ):
        obj_id = pipe_id["object_id"]
        acls = ws.dbsql_permissions.get(object_type, obj_id).access_control_list
        for acl in acls:
            if acl.group_name in ["users", "admins", "account users"]:
                continue
            if acl.group_name not in test_groups:
                continue

            validate_that(
                acl.group_name in persisted_rows[obj_id],
                f"{acl.group_name} not found in persisted rows for {object_type} {obj_id}",
            )
            validate_that(
                acl.permission_level.value in persisted_rows[obj_id][acl.group_name],
                f"{acl.permission_level.value} not found in persisted rows for {object_type} {obj_id} and group {acl.group_name}",
            )


def try_validate_files(sql_backend, test_database, test_groups, ws, object_type):
    try:
        validate_files(sql_backend, test_database, test_groups, ws, object_type)
    except Exception as e:
        logger.warning(f"Something wrong happened when asserting files -> {e}")


def validate_files(sql_backend, test_database, test_groups, ws: WorkspaceClient, object_type):
    validation_tasks = []
    persisted_rows = get_persisted_rows(sql_backend, test_database, object_type)
    for pipe_id in sql_backend.fetch(
        f"SELECT distinct object_id FROM {test_database.name}.objects where object_type = '{object_type}'"
    ):
        validation_tasks.append(partial(validate_table, pipe_id, ws, object_type, test_groups, persisted_rows))

    success , failure = Threads.gather(f"Validate {object_type}", validation_tasks)
    logger.warning(f"Had {len(failure)} when verifying {object_type}")


def validate_file(pipe_id, ws, object_type, test_groups, persisted_rows):
    obj_id = pipe_id["object_id"]
    try:
        acls = ws.workspace.get_permissions(object_type, obj_id).access_control_list
    except Exception as e:
        logger.warning(f"Could not fetch permissions for {object_type} {obj_id} ")
        logger.warning(e)
        return

    logger.debug(f"Validating {object_type} {obj_id}")
    for acl in acls:
        non_inherited_permissions = [perm for perm in acl.all_permissions if not perm.inherited]
        if non_inherited_permissions:
            if acl.group_name in ["users", "admins", "account users"]:
                continue
            if acl.group_name not in test_groups:
                continue

            validate_that(
                acl.group_name in persisted_rows[obj_id],
                f"{acl.group_name} not found in persisted rows for {object_type} {obj_id}",
            )

            for perm in non_inherited_permissions:
                validate_that(
                    perm.permission_level.value in persisted_rows[obj_id][acl.group_name],
                    f"{perm.permission_level.value} not found in persisted rows for {object_type} {obj_id} and group {acl.group_name}",
                )


def assign_ws_local_permissions(object_type, dir_perms, object_id, ws:WorkspaceClient) -> [WorkspaceObjectAccessControlRequest]:
    acls = []
    for group, permission in dir_perms.items():
        acls.append(WorkspaceObjectAccessControlRequest(group_name=group, permission_level=permission))
    ws.workspace.set_permissions(
        workspace_object_type=object_type, workspace_object_id=object_id, access_control_list=acls
    )
    return acls


def transfer_ownership(sql_backend, securable, name, owner):
    sql_backend.execute(f"ALTER {securable} {name} OWNER TO `{owner}`")


def grant_permissions(sql_backend, securable, name, permissions):
    for group, permission in permissions.items():
        permissions = ",".join(permission)
        sql_backend.execute(f"GRANT {permissions} ON {securable} {name} TO `{group}`")


def create_hive_metastore_permissions(groups, all_permissions):
    schema_permissions = {}
    for permission in all_permissions:
        for j in range(10):
            rand = random.randint(0, len(groups) - 1)
            ws_group = groups[rand].display_name
            if ws_group in schema_permissions:
                previous_perms = schema_permissions[ws_group]
                previous_perms.add(permission)
                schema_permissions[ws_group] = previous_perms
            else:
                schema_permissions[ws_group] = {permission}
    return schema_permissions


def create_permissions(groups, all_permissions):
    schema_permissions = {}
    for permission in all_permissions:
        for j in range(10):
            rand = random.randint(0, len(groups)-1)
            ws_group = groups[rand].display_name
            schema_permissions[ws_group] = permission
    return schema_permissions


def set_permissions(ws_object, id_attribute, object_type, ws, permissions):
    request_object_id = getattr(ws_object, id_attribute)
    acls = []
    for group, permission in permissions.items():
        acls.append(iam.AccessControlRequest(group_name=group, permission_level=permission))
    ws.permissions.update(
        request_object_type=object_type, request_object_id=request_object_id, access_control_list=acls
    )


def set_secrets_permissions(scope_name, ws, permissions):
    for group, permission in permissions.items():
        ws.secrets.put_acl(scope=scope_name, permission=permission, principal=group)


def set_dbsql_permissions(ws_object, id_attribute, object_type, ws, group_permissions):
    acls = []
    for group, permission in group_permissions.items():
        acls.append(sql.AccessControl(group_name=group, permission_level=permission))
    request_object_id = getattr(ws_object, id_attribute)
    set_perm_or_retry(object_type=object_type, object_id=request_object_id, acl=acls, ws=ws)


@rate_limited(max_requests=30)
def set_perm_or_retry(object_type: sql.ObjectTypePlural, object_id: str, acl: list[sql.AccessControl], ws):
    set_retry_on_value_error = retried(on=[ValueError], timeout=timedelta(minutes=1))
    set_retried_check = set_retry_on_value_error(safe_set_permissions)
    return set_retried_check(object_type, object_id, acl, ws)


def safe_set_permissions(
    object_type: sql.ObjectTypePlural, object_id: str, acl: list[sql.AccessControl] | None, ws
) -> sql.SetResponse | None:
    try:
        return ws.dbsql_permissions.set(object_type=object_type, object_id=object_id, access_control_list=acl)
    except DatabricksError as e:
        if e.error_code in [
            "BAD_REQUEST",
            "UNAUTHORIZED",
            "PERMISSION_DENIED",
            "NOT_FOUND",
        ]:
            logger.warning(f"Could not update permissions for {object_type} {object_id} due to {e.error_code}")
            return None
        else:
            msg = f"{e.error_code} can be retried for {object_type} {object_id}, doing another attempt..."
            raise ValueError(msg) from e


def persist_groups(groups: [(Group, Group)], sql_backend, test_database):
    to_persist = []
    for ws_group, acc_group in groups:
        to_persist.append(
            MigratedGroup(
                id_in_workspace=ws_group.id,
                name_in_workspace=ws_group.display_name,
                name_in_account=acc_group.display_name,
                temporary_name="",
                external_id=acc_group.id,
                members=json.dumps([gg.as_dict() for gg in ws_group.members]) if ws_group.members else None,
                roles=json.dumps([gg.as_dict() for gg in ws_group.roles]) if ws_group.roles else None,
                entitlements=json.dumps([gg.as_dict() for gg in ws_group.entitlements])
                if ws_group.entitlements
                else None,
            )
        )

    sql_backend.save_table(f"{test_database.name}.groups", to_persist, MigratedGroup)


def validate_that(func, message):
    try:
        if not func:
            verificationErrors.append(message)
    except Exception as e:
        verificationErrors.append(message)
        logger.warning("Something wrong happened during the assertion: %s", e)

#TODO: FIXTURES ARE TOO DANGEROUS BECAUSE iF THERE'S ANY FAILURE WE HAVE TO WAIL A FULL DAY TO RECREATE OBJECTS

@rate_limited(max_requests=5, burst_period_seconds=1)
def create_user(ws:WorkspaceClient) -> User:
    name = f"sdk-{make_random(6)}@example.com".lower()
    user = ws.users.create(user_name=name)
    logger.info(f"Created user {name}")
    return user


def _scim_values(ids: list[str]) -> list[iam.ComplexValue]:
    return [iam.ComplexValue(value=x) for x in ids]


def create_group(ws:WorkspaceClient, acc:AccountClient, members, entitlements):
    workspace_group_name = f"ucx_{make_random(6)}"
    account_group_name = workspace_group_name

    ws_group = create_ws_group(ws, display_name=workspace_group_name, members=members, entitlements=entitlements)
    acc_group = create_acc_group(acc, display_name=account_group_name, members=members)
    return ws_group, acc_group


def create_ws_group(ws, display_name, members, entitlements):
    return _make_group("workspace group", ws.config, ws.groups, members=members, entitlements=entitlements, display_name=display_name)


def create_acc_group(acc, display_name, members):
    return _make_group("account group", acc.config, acc.groups, members=members, display_name=display_name)


def _make_group(name, cfg, interface, members: list[str] | None = None,
    roles: list[str] | None = None,
    entitlements: list[str] | None = None,
    display_name: str | None = None, **kwargs):

    kwargs["display_name"] = f"sdk-{make_random(6)}" if display_name is None else display_name
    if members is not None:
        kwargs["members"] = _scim_values(members)
    if roles is not None:
        kwargs["roles"] = _scim_values(roles)
    if entitlements is not None:
        kwargs["entitlements"] = _scim_values(entitlements)
    # TODO: REQUEST_LIMIT_EXCEEDED: GetUserPermissionsRequest RPC token bucket limit has been exceeded.
    group = interface.create(**kwargs)
    if cfg.is_account_client:
        logger.info(f"Account group {group.display_name}: {cfg.host}/users/groups/{group.id}/members")
    else:
        logger.info(f"Workspace group {group.display_name}: {cfg.host}#setting/accounts/groups/{group.id}")
    return group


def make_random(k=16):
    charset = string.ascii_uppercase + string.ascii_lowercase + string.digits
    return "".join(random.choices(charset, k=int(k)))


def create_scope(ws, **kwargs):
    name = f"sdk-{make_random(4)}"
    ws.secrets.create_scope(name, **kwargs)
    return name


def create_dlt(ws, **kwargs) -> pipelines.CreatePipelineResponse:
    if "name" not in kwargs:
        kwargs["name"] = f"sdk-{make_random(4)}"
    if "libraries" not in kwargs:
        kwargs["libraries"] = [pipelines.PipelineLibrary(notebook=pipelines.NotebookLibrary(path=create_notebook(ws)))]
    if "clusters" not in kwargs:
        kwargs["clusters"] = [
            pipelines.PipelineCluster(
                node_type_id=ws.clusters.select_node_type(local_disk=True),
                label="default",
                num_workers=1,
                custom_tags={
                    "cluster_type": "default",
                },
            )
        ]
    return ws.pipelines.create(continuous=False, **kwargs)


def create_job(ws, **kwargs):
    task_spark_conf = None
    if "name" not in kwargs:
        kwargs["name"] = f"sdk-{make_random(4)}"
    if "spark_conf" in kwargs:
        task_spark_conf = kwargs["spark_conf"]
        kwargs.pop("spark_conf")
    if "tasks" not in kwargs:
        if task_spark_conf:
            kwargs["tasks"] = [
                jobs.Task(
                    task_key=make_random(4),
                    description=make_random(4),
                    new_cluster=compute.ClusterSpec(
                        num_workers=1,
                        node_type_id=ws.clusters.select_node_type(local_disk=True),
                        spark_version=ws.clusters.select_spark_version(latest=True),
                        spark_conf=task_spark_conf,
                    ),
                    notebook_task=jobs.NotebookTask(notebook_path=create_notebook(ws)),
                    timeout_seconds=0,
                )
            ]
        else:
            kwargs["tasks"] = [
                jobs.Task(
                    task_key=make_random(4),
                    description=make_random(4),
                    new_cluster=compute.ClusterSpec(
                        num_workers=1,
                        node_type_id=ws.clusters.select_node_type(local_disk=True),
                        spark_version=ws.clusters.select_spark_version(latest=True),
                    ),
                    notebook_task=jobs.NotebookTask(notebook_path=create_notebook(ws)),
                    timeout_seconds=0,
                )
            ]
    job = ws.jobs.create(**kwargs)
    logger.info(f"Job: {ws.config.host}#job/{job.job_id}")
    return job


def create_experiment(
    ws: WorkspaceClient,
    path: str | None = None,
    experiment_name: str | None = None,
    **kwargs,
):
    if path is None:
        path = f"/Users/{ws.current_user.me().user_name}/{make_random(4)}"
    if experiment_name is None:
        experiment_name = f"sdk-{make_random(4)}"

    try:
        ws.workspace.mkdirs(path)
    except DatabricksError:
        pass

    return ws.experiments.create_experiment(name=f"{path}/{experiment_name}", **kwargs)


def create_model(
    ws:WorkspaceClient,
    model_name: str | None = None,
    **kwargs,
):
    if model_name is None:
        model_name = f"sdk-{make_random(4)}"

    created_model = ws.model_registry.create_model(model_name, **kwargs)
    model = ws.model_registry.get_model(created_model.registered_model.name)
    return model.registered_model_databricks


def create_pool(ws:WorkspaceClient, instance_pool_name=None, node_type_id=None, **kwargs):
    if instance_pool_name is None:
        instance_pool_name = f"sdk-{make_random(4)}"
    if node_type_id is None:
        node_type_id = ws.clusters.select_node_type(local_disk=True)
    return ws.instance_pools.create(instance_pool_name, node_type_id, **kwargs)


def create_warehouse(
    ws:WorkspaceClient,
    warehouse_name: str | None = None,
    warehouse_type: CreateWarehouseRequestWarehouseType | None = None,
    cluster_size: str | None = None,
    max_num_clusters: int = 1,
    enable_serverless_compute: bool = False,
    **kwargs,
):
    if warehouse_name is None:
        warehouse_name = f"sdk-{make_random(4)}"
    if warehouse_type is None:
        warehouse_type = CreateWarehouseRequestWarehouseType.PRO
    if cluster_size is None:
        cluster_size = "2X-Small"

    return ws.warehouses.create(
        name=warehouse_name,
        cluster_size=cluster_size,
        warehouse_type=warehouse_type,
        max_num_clusters=max_num_clusters,
        enable_serverless_compute=enable_serverless_compute,
        **kwargs,
    )


def create_cluster(
    ws:WorkspaceClient,
    single_node: bool = True,
    cluster_name: str | None = None,
    spark_version: str | None = None,
    autotermination_minutes=10,
    **kwargs,
):
    if cluster_name is None:
        cluster_name = f"sdk-{make_random(4)}"
    if spark_version is None:
        spark_version = ws.clusters.select_spark_version(latest=True)
    if single_node:
        kwargs["num_workers"] = 0
        if "spark_conf" in kwargs:
            kwargs["spark_conf"] = kwargs["spark_conf"] | {
                "spark.databricks.cluster.profile": "singleNode",
                "spark.master": "local[*]",
            }
        else:
            kwargs["spark_conf"] = {"spark.databricks.cluster.profile": "singleNode", "spark.master": "local[*]"}
        kwargs["custom_tags"] = {"ResourceClass": "SingleNode"}
    if "instance_pool_id" not in kwargs:
        kwargs["node_type_id"] = ws.clusters.select_node_type(local_disk=True)

    return ws.clusters.create(
        cluster_name=cluster_name,
        spark_version=spark_version,
        autotermination_minutes=autotermination_minutes,
        **kwargs,
    )

def create_policy(ws:WorkspaceClient, name: str | None = None, **kwargs):
    if name is None:
        name = f"sdk-{make_random(4)}"
    if "definition" not in kwargs:
        kwargs["definition"] = json.dumps(
            {"spark_conf.spark.databricks.delta.preview.enabled": {"type": "fixed", "value": "true"}}
        )
    cluster_policy = ws.cluster_policies.create(name, **kwargs)
    logger.info(
        f"Cluster policy: {ws.config.host}#setting/clusters/cluster-policies/view/{cluster_policy.policy_id}"
    )
    return cluster_policy


def create_query(ws:WorkspaceClient, query_name: str | None = None, query: str | None = None) -> QueryInfo:
    if query_name is None:
        query_name = f"ucx_query_Q{make_random(4)}"

    dbsql_query = ws.queries.create(
        name=f"{query_name}",
        description="TEST QUERY FOR UCX",
        query="SELECT 1+1;",
    )
    logger.info(f"Query Created {query_name}: {ws.config.host}/sql/editor/{dbsql_query.id}")
    return dbsql_query


def create_alert(query_id, ws:WorkspaceClient, name: str | None = None):
    if name is None:
        name = f"ucx_T{make_random(4)}"
    return ws.alerts.create(options=AlertOptions(column="1", op="==", value="1"), name=name, query_id=query_id)


def create_dashboard(ws:WorkspaceClient, name: str | None = None):
    if name is None:
        name = f"ucx_T{make_random(4)}"
    return ws.dashboards.create(name=name)


def create_repo(ws:WorkspaceClient, url=None, provider=None, path=None, **kwargs):
    if path is None:
        path = f"/Repos/{ws.current_user.me().user_name}/sdk-{make_random(4)}"
    if url is None:
        url = "https://github.com/shreyas-goenka/empty-repo.git"
    if provider is None:
        provider = "github"
    return ws.repos.create(url, provider, path=path, **kwargs)


def create_dir(ws:WorkspaceClient, path: str | None = None):
    if path is None:
        path = f"/Users/{ws.current_user.me().user_name}/sdk-{make_random(4)}"
    ws.workspace.mkdirs(path)
    return path


def create_notebook(ws:WorkspaceClient, path: str | None = None, content: BinaryIO | None = None, **kwargs):
    if path is None:
        path = f"/Users/{ws.current_user.me().user_name}/sdk-{make_random(4)}.py"
    if content is None:
        content = io.BytesIO(b"print(1)")
    ws.workspace.upload(path, content, **kwargs)
    return path


def create_schema(sql_backend, ws:WorkspaceClient, catalog_name: str = "hive_metastore", name: str | None = None) -> SchemaInfo:
    if name is None:
        name = f"ucx_S{make_random(4)}".lower()
    full_name = f"{catalog_name}.{name}".lower()
    sql_backend.execute(f"CREATE SCHEMA {full_name}")
    schema_info = SchemaInfo(catalog_name=catalog_name, name=name, full_name=full_name)
    logger.info(
        f"Schema {schema_info.full_name}: "
        f"{ws.config.host}/explore/data/{schema_info.catalog_name}/{schema_info.name}"
    )
    return schema_info

def create_table(
    sql_backend,
    ws:WorkspaceClient,
    catalog_name="hive_metastore",
    schema_name: str | None = None,
    name: str | None = None,
    ctas: str | None = None,
    non_delta: bool = False,
    external: bool = False,
    external_csv: str | None = None,
    view: bool = False,
    tbl_properties: dict[str, str] | None = None,
    function: bool = False,
) -> TableInfo:
    if schema_name is None:
        schema = create_schema(sql_backend, ws, catalog_name=catalog_name)
        catalog_name = schema.catalog_name
        schema_name = schema.name
    if name is None:
        name = f"ucx_T{make_random(4)}".lower()
    table_type = None
    data_source_format = None
    storage_location = None
    full_name = f"{catalog_name}.{schema_name}.{name}".lower()

    ddl = f'CREATE {"VIEW" if view else "TABLE"} {full_name}'
    if view:
        table_type = TableType.VIEW
    if ctas is not None:
        # temporary (if not view)
        ddl = f"{ddl} AS {ctas}"
    elif function:
        ddl = f"{ddl}() RETURNS INT NOT DETERMINISTIC CONTAINS SQL RETURN (rand() * 6)::INT + 1;"
    elif non_delta:
        table_type = TableType.MANAGED
        data_source_format = DataSourceFormat.JSON
        storage_location = "dbfs:/databricks-datasets/iot-stream/data-device"
        ddl = f"{ddl} USING json LOCATION '{storage_location}'"
    elif external_csv is not None:
        table_type = TableType.EXTERNAL
        data_source_format = DataSourceFormat.CSV
        storage_location = external_csv
        ddl = f"{ddl} USING CSV OPTIONS (header=true) LOCATION '{storage_location}'"
    elif external:
        # external table
        table_type = TableType.EXTERNAL
        data_source_format = DataSourceFormat.DELTASHARING
        url = "s3a://databricks-datasets-oregon/delta-sharing/share/open-datasets.share"
        storage_location = f"{url}#delta_sharing.default.lending_club"
        ddl = f"{ddl} USING deltaSharing LOCATION '{storage_location}'"
    else:
        # managed table
        table_type = TableType.MANAGED
        data_source_format = DataSourceFormat.DELTA
        storage_location = f"dbfs:/user/hive/warehouse/{schema_name}/{name}"
        ddl = f"{ddl} (id INT, value STRING)"
    if tbl_properties:
        tbl_properties = ",".join([f" '{k}' = '{v}' " for k, v in tbl_properties.items()])
        ddl = f"{ddl} TBLPROPERTIES ({tbl_properties})"

    sql_backend.execute(ddl)
    table_info = TableInfo(
        catalog_name=catalog_name,
        schema_name=schema_name,
        name=name,
        full_name=full_name,
        properties=tbl_properties,
        storage_location=storage_location,
        table_type=table_type,
        data_source_format=data_source_format,
    )
    logger.info(
        f"Table {table_info.full_name}: "
        f"{ws.config.host}/explore/data/{table_info.catalog_name}/{table_info.schema_name}/{table_info.name}"
    )
    return table_info

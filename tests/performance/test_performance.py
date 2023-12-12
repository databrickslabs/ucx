import datetime
from dataclasses import dataclass
from datetime import timedelta

from databricks.sdk.retries import retried
from databricks.sdk.service import iam, sql, pipelines, compute, jobs
from databricks.sdk.service.iam import Group, PermissionLevel
from databricks.sdk.service.workspace import (
    AclPermission,
    WorkspaceObjectAccessControlRequest,
    WorkspaceObjectPermissionLevel,
)

from databricks.labs.ucx.config import WorkspaceConfig
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
    display_name:str

@dataclass
class PersistedGroup:
    group: str
    object_type: str
    object_id: str
    permission: str


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
    ws,
    acc,
    sql_backend
):
    NB_OF_TEST_WS_OBJECTS = 100
    NB_OF_FILES = 100
    MAX_NB_OF_FILES = 100
    NB_OF_TEST_GROUPS = 1000
    NB_OF_SCHEMAS = 100
    MAX_NB_OF_TABLES = 20
    USER_POOL_LENGTH = 2000

    fresh_account(acc)
    test_database = create_schema(sql_backend, ws, name=f"test_results_{datetime.datetime.utcnow().strftime('%Y_%m_%d_%H_%M_%S')}")
    users = create_users(USER_POOL_LENGTH, sql_backend, test_database, ws)

    groups = create_groups(NB_OF_TEST_GROUPS, ws, acc, sql_backend, test_database, users)

    create_scopes(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws)

    create_pipelines(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws)

    create_jobs(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws)

    create_experiments(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws)

    create_models(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws)

    create_pools(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws)

    create_warehouses(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws)

    create_clusters(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws)

    create_policies(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws)

    create_queries_and_alerts(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws)

    create_dashboards(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws)

    create_repos(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws)

    create_dirs_n_notebookes(
        NB_OF_FILES, MAX_NB_OF_FILES, groups, sql_backend, test_database, ws
    )

    create_schemas_n_tables(
        NB_OF_SCHEMAS, MAX_NB_OF_TABLES, groups, sql_backend, test_database, users, ws
    )

    backup_group_prefix = "db-temp-"
    inventory_database = f"ucx_{make_random(4)}"
    test_groups = [_[0].display_name for _ in groups]

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

    persisted_rows = get_persisted_rows(sql_backend, test_database)

    try_validate_object(persisted_rows, sql_backend, test_database, test_groups, ws, "pipelines")
    try_validate_object(persisted_rows, sql_backend, test_database, test_groups, ws, "jobs")
    try_validate_object(persisted_rows, sql_backend, test_database, test_groups, ws, "experiments")
    try_validate_object(persisted_rows, sql_backend, test_database, test_groups, ws, "registered-models")
    try_validate_object(persisted_rows, sql_backend, test_database, test_groups, ws, "instance-pools")
    try_validate_object(persisted_rows, sql_backend, test_database, test_groups, ws, "warehouses")
    try_validate_object(persisted_rows, sql_backend, test_database, test_groups, ws, "clusters")
    try_validate_object(persisted_rows, sql_backend, test_database, test_groups, ws, "cluster-policies")
    try_validate_sql_objects(persisted_rows, sql_backend, test_database, test_groups, ws, sql.ObjectTypePlural.ALERTS)
    try_validate_sql_objects(
        persisted_rows, sql_backend, test_database, test_groups, ws, sql.ObjectTypePlural.DASHBOARDS
    )
    try_validate_sql_objects(persisted_rows, sql_backend, test_database, test_groups, ws, sql.ObjectTypePlural.QUERIES)

    try_validate_files(persisted_rows, sql_backend, test_database, test_groups, ws, "notebooks")
    try_validate_files(persisted_rows, sql_backend, test_database, test_groups, ws, "directories")

    try_validate_tables(persisted_rows, sql_backend, test_database, test_groups, "SCHEMA")
    try_validate_tables(persisted_rows, sql_backend, test_database, test_groups, "TABLE")
    try_validate_tables(persisted_rows, sql_backend, test_database, test_groups, "VIEW")

    try_validate_secrets(persisted_rows, sql_backend, test_database, test_groups, ws)
    validate_entitlements(sql_backend, test_database, ws)

    if len(verificationErrors) > 0:
        with open(f"perf-test-{datetime.datetime.now()}.txt", "w") as txt_file:
            for line in verificationErrors:
                txt_file.write(line + "\n")
    assert [] == verificationErrors

def fresh_account(acc):
    for user in list(acc.users.list()):
        if "william" not in user.display_name:
            logger.info(f"Deleting user {user.display_name}")
            acc.users.delete(user.id)

    for grp in list(acc.groups.list()):
        logger.info(f"Deleting group {grp.display_name}")
        acc.groups.delete(grp.id)


def recover(ws, acc, sql_backend):
    inventory_database = "test_inv_database" #Needs to be modified depending on recover
    results_database = "test_inv_database"

    mggrps = []
    for row in sql_backend.fetch(f"SELECT * FROM hive_metastore.{results_database}.groups"):
        mggrps.append(MigratedGroup(*row))

    test_groups = [_.name_in_workspace for _ in mggrps]

    install_and_run(sql_backend, ws, inventory_database , test_groups)
    validate(sql_backend, SchemaInfo(name=results_database), test_groups, ws)

def install_and_run(sql_backend, ws:WorkspaceClient, inventory_database:str, test_groups):
    backup_group_prefix = "db-temp-"

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

def validate(sql_backend, test_database, test_groups, ws):
    persisted_rows = get_persisted_rows(sql_backend, test_database)

    try_validate_object(persisted_rows, sql_backend, test_database, test_groups, ws, "pipelines")
    try_validate_object(persisted_rows, sql_backend, test_database, test_groups, ws, "jobs")
    try_validate_object(persisted_rows, sql_backend, test_database, test_groups, ws, "experiments")
    try_validate_object(persisted_rows, sql_backend, test_database, test_groups, ws, "registered-models")
    try_validate_object(persisted_rows, sql_backend, test_database, test_groups, ws, "instance-pools")
    try_validate_object(persisted_rows, sql_backend, test_database, test_groups, ws, "warehouses")
    try_validate_object(persisted_rows, sql_backend, test_database, test_groups, ws, "clusters")
    try_validate_object(persisted_rows, sql_backend, test_database, test_groups, ws, "cluster-policies")
    try_validate_sql_objects(persisted_rows, sql_backend, test_database, test_groups, ws, sql.ObjectTypePlural.ALERTS)
    try_validate_sql_objects(
        persisted_rows, sql_backend, test_database, test_groups, ws, sql.ObjectTypePlural.DASHBOARDS
    )
    try_validate_sql_objects(persisted_rows, sql_backend, test_database, test_groups, ws, sql.ObjectTypePlural.QUERIES)

    try_validate_files(persisted_rows, sql_backend, test_database, test_groups, ws, "notebooks")
    try_validate_files(persisted_rows, sql_backend, test_database, test_groups, ws, "directories")

    try_validate_tables(persisted_rows, sql_backend, test_database, test_groups, "SCHEMA")
    try_validate_tables(persisted_rows, sql_backend, test_database, test_groups, "TABLE")
    try_validate_tables(persisted_rows, sql_backend, test_database, test_groups, "VIEW")

    try_validate_secrets(persisted_rows, sql_backend, test_database, test_groups, ws)
    #validate_entitlements(sql_backend, test_database, ws)

    if len(verificationErrors) > 0:
        with open(f"perf-test-{datetime.datetime.now()}.txt", "w") as txt_file:
            for line in verificationErrors:
                txt_file.write(line + "\n")
    assert [] == verificationErrors

def get_persisted_rows(sql_backend, test_database):
    persisted_rows = {}
    for row in sql_backend.fetch(f"SELECT * FROM {test_database.name}.objects"):
        mgted_grp = ObjectPermission(*row)
        if mgted_grp.object_id in persisted_rows:
            previous_perms = persisted_rows[mgted_grp.object_id]
            if mgted_grp.group in previous_perms:
                previous_perms[mgted_grp.group] = [mgted_grp.permission] + previous_perms[mgted_grp.group]
            else:
                previous_perms[mgted_grp.group] = [mgted_grp.permission]
            persisted_rows[mgted_grp.object_id] = previous_perms
        else:
            persisted_rows[mgted_grp.object_id] = {mgted_grp.group: [mgted_grp.permission]}
    logger.debug(json.dumps(persisted_rows))
    return persisted_rows


def create_schemas_n_tables(
    NB_OF_SCHEMAS, MAX_NB_OF_TABLES, groups, sql_backend, test_database, users, ws
):
    to_persist = []
    for i in range(NB_OF_SCHEMAS):
        schema = create_schema(sql_backend, ws)
        schema_permissions = create_hive_metastore_permissions(
            groups, ["SELECT", "MODIFY", "READ_METADATA", "CREATE", "USAGE"]
        )
        grant_permissions(sql_backend, "SCHEMA", schema.name, schema_permissions)
        owner = groups[random.randint(0, len(groups) - 1)][0].display_name
        transfer_ownership(sql_backend, "SCHEMA", schema.name, owner)

        for group, permissions in schema_permissions.items():
            for permission in permissions:
                to_persist.append(ObjectPermission(group, "SCHEMA", schema.name, permission))
        to_persist.append(ObjectPermission(owner, "DATABASE", schema.name, "OWN"))

        nb_of_tables = random.randint(1, MAX_NB_OF_TABLES)
        logger.info(f"Created schema {schema.name}")
        logger.info(f"Creating {nb_of_tables} tables and views on schema {schema.name}")

        for j in range(nb_of_tables):
            table = create_table(sql_backend, ws, schema_name=schema.name)
            full_name = schema.name + "." + table.name
            table_permission = create_hive_metastore_permissions(groups, ["SELECT", "MODIFY", "READ_METADATA"])
            grant_permissions(sql_backend, "TABLE", full_name, table_permission)

            owner = groups[random.randint(0, len(groups) - 1)][0].display_name
            transfer_ownership(sql_backend, "TABLE", full_name, owner)
            for group, permissions in table_permission.items():
                for permission in permissions:
                    to_persist.append(ObjectPermission(group, "TABLE", full_name, permission))
            to_persist.append(ObjectPermission(owner, "TABLE", full_name, "OWN"))
            logger.info(f"Created table {full_name}")

        for j in range(nb_of_tables):
            view = create_table(sql_backend, ws, schema_name=schema.name, view=True, ctas="SELECT 2+2 AS four")
            full_name = schema.name + "." + view.name
            view_permission = create_hive_metastore_permissions(groups, ["SELECT"])
            grant_permissions(sql_backend, "VIEW", full_name, view_permission)

            owner = groups[random.randint(0, len(groups) - 1)][0].display_name
            transfer_ownership(sql_backend, "VIEW", full_name, owner)
            for group, permissions in view_permission.items():
                for permission in permissions:
                    to_persist.append(ObjectPermission(group, "VIEW", full_name, permission))
            to_persist.append(ObjectPermission(owner, "VIEW", full_name, "OWN"))
            logger.info(f"Created view {full_name}")
        for j in range(nb_of_tables):
            table = create_table(sql_backend, ws, schema_name=schema.name)
            full_name = schema.name + "." + table.name
            table_permission = create_hive_metastore_permissions(groups, ["SELECT", "MODIFY", "READ_METADATA"])

            grant_permissions(sql_backend, "TABLE", full_name, table_permission)

            owner = users[random.randint(0, len(users) - 1)].display_name
            transfer_ownership(sql_backend, "TABLE", full_name, owner)
            for group, permissions in table_permission.items():
                for permission in permissions:
                    to_persist.append(ObjectPermission(group, "TABLE", full_name, permission))

            to_persist.append(ObjectPermission(owner, "TABLE", full_name, "OWN"))
            logger.info(f"Created table {full_name}")
        for j in range(nb_of_tables):
            view = create_table(sql_backend, ws, schema_name=schema.name, view=True, ctas="SELECT 2+2 AS four")
            full_name = schema.name + "." + view.name
            view_permission = create_hive_metastore_permissions(groups, ["SELECT"])

            grant_permissions(sql_backend, "VIEW", full_name, view_permission)

            owner = users[random.randint(0, len(users) - 1)].display_name
            transfer_ownership(sql_backend, "VIEW", full_name, owner)
            for group, permissions in view_permission.items():
                for permission in permissions:
                    to_persist.append(ObjectPermission(group, "VIEW", full_name, permission))
            to_persist.append(ObjectPermission(owner, "VIEW", full_name, "OWN"))
            logger.info(f"Created view {full_name}")
    sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)


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
    to_persist = []
    for j in range(NB_OF_TEST_WS_OBJECTS):
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
    to_persist = []
    for j in range(NB_OF_TEST_WS_OBJECTS):
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


def create_queries_and_alerts(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws):
    to_persist = []
    for i in range(NB_OF_TEST_WS_OBJECTS):
        query = create_query(ws)
        query_permissions = create_permissions(
            groups, [sql.PermissionLevel.CAN_MANAGE, sql.PermissionLevel.CAN_RUN, sql.PermissionLevel.CAN_VIEW]
        )
        set_dbsql_permissions(query, "id", sql.ObjectTypePlural.QUERIES, ws, query_permissions)
        for group, permission in query_permissions.items():
            to_persist.append(ObjectPermission(group, sql.ObjectTypePlural.QUERIES.value, query.id, permission.value))
        logger.info(f"Created query {query.id}")

        nb_of_alerts = random.randint(1, 10)
        logger.info(f"Creating {nb_of_alerts} alerts on top of query {query.name}")
        for j in range(nb_of_alerts):
            alert = create_alert(query_id=query.id, ws=ws)
            alert_permissions = create_permissions(
                groups, [sql.PermissionLevel.CAN_MANAGE, sql.PermissionLevel.CAN_RUN, sql.PermissionLevel.CAN_VIEW]
            )
            set_dbsql_permissions(alert, "id", sql.ObjectTypePlural.ALERTS, ws, alert_permissions)
            for group, permission in alert_permissions.items():
                to_persist.append(
                    ObjectPermission(group, sql.ObjectTypePlural.ALERTS.value, alert.id, permission.value)
                )
            logger.info(f"Created alert {alert.id}")
    sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)


def create_policies(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws):
    to_persist = []
    for i in range(NB_OF_TEST_WS_OBJECTS):
        ws_object = create_policy(ws)
        ws_permissions = create_permissions(groups, [PermissionLevel.CAN_USE])
        set_permissions(ws_object, "policy_id", "cluster-policies", ws, ws_permissions)
        for group, permission in ws_permissions.items():
            to_persist.append(ObjectPermission(group, "cluster-policies", ws_object.policy_id, permission.value))
        logger.info(f"Created policy {ws_object.policy_id}")
    sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)


def create_clusters(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws):
    to_persist = []
    for i in range(NB_OF_TEST_WS_OBJECTS):
        ws_object = create_cluster(ws, single_node=True)
        ws_permissions = create_permissions(
            groups, [PermissionLevel.CAN_MANAGE, PermissionLevel.CAN_RESTART, PermissionLevel.CAN_ATTACH_TO]
        )
        set_permissions(ws_object, "cluster_id", "clusters", ws, ws_permissions)
        for group, permission in ws_permissions.items():
            to_persist.append(ObjectPermission(group, "clusters", ws_object.cluster_id, permission.value))
        logger.info(f"Created cluster {ws_object.cluster_id}")
    sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)


def create_warehouses(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws):
    to_persist = []
    for i in range(NB_OF_TEST_WS_OBJECTS):
        warehouse = create_warehouse(ws)
        ws_permissions = create_permissions(groups, [PermissionLevel.CAN_MANAGE, PermissionLevel.CAN_USE])
        set_permissions(warehouse, "id", "warehouses", ws, ws_permissions)
        for group, permission in ws_permissions.items():
            to_persist.append(ObjectPermission(group, "warehouses", warehouse.id, permission.value))
        logger.info(f"Created warehouse {warehouse.id}")
    sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)


def create_pools(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws):
    to_persist = []
    for i in range(NB_OF_TEST_WS_OBJECTS):
        ws_object = create_pool(ws)
        ws_permissions = create_permissions(groups, [PermissionLevel.CAN_MANAGE, PermissionLevel.CAN_ATTACH_TO])
        set_permissions(ws_object, "instance_pool_id", "instance-pools", ws, ws_permissions)
        for group, permission in ws_permissions.items():
            to_persist.append(ObjectPermission(group, "instance-pools", ws_object.instance_pool_id, permission.value))
        logger.info(f"Created pool {ws_object.instance_pool_id}")
    sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)


def create_models(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws):
    to_persist = []
    for i in range(NB_OF_TEST_WS_OBJECTS):
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
    to_persist = []
    for i in range(NB_OF_TEST_WS_OBJECTS):
        ws_object = create_experiment(ws)
        ws_permissions = create_permissions(
            groups, [PermissionLevel.CAN_READ, PermissionLevel.CAN_MANAGE, PermissionLevel.CAN_EDIT]
        )
        set_permissions(ws_object, "experiment_id", "experiments", ws, ws_permissions)
        for group, permission in ws_permissions.items():
            to_persist.append(ObjectPermission(group, "experiments", ws_object.experiment_id, permission.value))
        logger.info(f"Created experiment {ws_object.experiment_id}")
    sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)


def create_jobs(NB_OF_TEST_WS_OBJECTS, groups, sql_backend, test_database, ws):
    to_persist = []
    for i in range(NB_OF_TEST_WS_OBJECTS):
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
    to_persist = []
    for i in range(NB_OF_TEST_WS_OBJECTS):
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
    to_persist = []
    for i in range(NB_OF_TEST_WS_OBJECTS):
        scope = create_scope(ws)
        ws_permissions = create_permissions(groups, [AclPermission.MANAGE, AclPermission.READ, AclPermission.WRITE])
        set_secrets_permissions(scope, ws, ws_permissions)
        for group, permission in ws_permissions.items():
            to_persist.append(ObjectPermission(group, "secrets", scope, permission.value))
        logger.info(f"Created scope {scope}")
    sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)


def create_groups(NB_OF_TEST_GROUPS, ws, acc, sql_backend, test_database, user_pool):
    groups = []
    for i in range(NB_OF_TEST_GROUPS):
        entitlements_list = [
            "workspace-access",
            "databricks-sql-access",
            "allow-cluster-create",
            "allow-instance-pool-create",
        ]
        entitlements = [_ for _ in random.choices(entitlements_list, k=random.randint(1, 3))]

        nb_of_members = random.randint(1, len(user_pool) - 1)
        members = []
        for j in range(nb_of_members):
            user_index_in_list = random.randint(0, len(user_pool) - 1)
            members.append(user_pool[user_index_in_list].id)

        ws_group, acc_group = create_group(ws, acc, members, entitlements)
        groups.append((ws_group, acc_group))
        logger.info(f"Created group {ws_group.display_name} {i + 1} with {len(ws_group.members)} members")
    persist_groups(groups, sql_backend, test_database)
    return groups


def create_users(MAX_GRP_USERS, sql_backend, test_database, ws:WorkspaceClient):
    users = []
    to_persist = []
    for i in range(MAX_GRP_USERS):
        user = create_user(ws)
        users.append(user)
        to_persist.append(User(user.display_name))
        logger.info(f"Created user {user.display_name}")
    sql_backend.save_table(f"{test_database.name}.users", users, User)
    return users


def try_validate_object(persisted_rows, sql_backend, test_database, test_groups, ws, object_type):
    try:
        validate_objects(persisted_rows, sql_backend, test_database, test_groups, ws, object_type)
    except Exception as e:
        logger.warning(f"Something wrong happened when asserting objects -> {e}")


def validate_objects(persisted_rows, sql_backend, test_database, test_groups, ws: WorkspaceClient, object_type):
    for pipe_id in sql_backend.fetch(
        f"SELECT distinct object_id FROM {test_database.name}.objects where object_type = '{object_type}'"
    ):
        obj_id = pipe_id["object_id"]
        acls = ws.permissions.get(
            object_type, obj_id
        ).access_control_list  # TODO: can fail, must capture the exception and move on
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


def try_validate_secrets(persisted_rows, sql_backend, test_database, test_groups, ws):
    try:
        validate_secrets(persisted_rows, sql_backend, test_database, test_groups, ws)
    except Exception as e:
        logger.warning(f"Something wrong happened when asserting objects -> {e}")


def validate_secrets(persisted_rows, sql_backend, test_database, test_groups, ws: WorkspaceClient):
    for pipe_id in sql_backend.fetch(
        f"SELECT distinct group, object_id FROM {test_database.name}.objects where object_type = 'secrets'"
    ):
        obj_id = pipe_id["object_id"]
        group = pipe_id["group"]
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


def try_validate_tables(persisted_rows, sql_backend, test_database, test_groups, object_type):
    logger.info(f"Validating {object_type}")
    try:
        validate_tables(persisted_rows, sql_backend, test_database, test_groups, object_type)
    except RuntimeError as e:
        logger.warning(f"Something wrong happened when asserting tables -> {e}")


def validate_tables(persisted_rows, sql_backend, test_database, test_groups, object_type):
    for pipe_id in sql_backend.fetch(
        f"SELECT distinct object_id FROM {test_database.name}.objects where object_type = '{object_type}'"
    ):
        obj_id = pipe_id["object_id"]
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


def try_validate_sql_objects(persisted_rows, sql_backend, test_database, test_groups, ws, object_type):
    try:
        validate_sql_objects(persisted_rows, sql_backend, test_database, test_groups, ws, object_type)
    except Exception as e:
        logger.warning(f"Something wrong happened when asserting SQL objects -> {e}")


def validate_sql_objects(persisted_rows, sql_backend, test_database, test_groups, ws, object_type):
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


def try_validate_files(persisted_rows, sql_backend, test_database, test_groups, ws, object_type):
    try:
        validate_files(persisted_rows, sql_backend, test_database, test_groups, ws, object_type)
    except Exception as e:
        logger.warning(f"Something wrong happened when asserting files -> {e}")


def validate_files(persisted_rows, sql_backend, test_database, test_groups, ws: WorkspaceClient, object_type):
    for pipe_id in sql_backend.fetch(
        f"SELECT distinct object_id FROM {test_database.name}.objects where object_type = '{object_type}'"
    ):
        obj_id = pipe_id["object_id"]
        try:
            acls = ws.workspace.get_permissions(object_type, obj_id).access_control_list
        except Exception as e:
            logger.warning(f"Could not fetch permissions for {object_type} {obj_id} ")
            logger.warning(e)
            continue
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


def assign_ws_local_permissions(object_type, dir_perms, object_id, ws):
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
            ws_group = groups[rand][0].display_name
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
            ws_group = groups[rand][0].display_name
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

def create_user(ws:WorkspaceClient):
    return ws.users.create(user_name=f"sdk-{make_random(4)}@example.com".lower())

def _scim_values(ids: list[str]) -> list[iam.ComplexValue]:
    return [iam.ComplexValue(value=x) for x in ids]


def create_group(ws:WorkspaceClient, acc:AccountClient, members, entitlements):
    workspace_group_name = f"ucx_{make_random(4)}"
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

    kwargs["display_name"] = f"sdk-{make_random(4)}" if display_name is None else display_name
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
    single_node: bool = False,
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

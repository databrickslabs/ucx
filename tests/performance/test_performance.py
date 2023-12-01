import json
import logging
import random
from dataclasses import dataclass
from datetime import timedelta

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError
from databricks.sdk.retries import retried
from databricks.sdk.service import iam, sql
from databricks.sdk.service.iam import Group, PermissionLevel
from databricks.sdk.service.settings import TokenAccessControlRequest, TokenPermissionLevel
from databricks.sdk.service.workspace import (
    AclPermission,
    WorkspaceObjectAccessControlRequest,
    WorkspaceObjectPermissionLevel,
)

from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.install import WorkspaceInstaller
from databricks.labs.ucx.mixins.hardening import rate_limited
from databricks.labs.ucx.workspace_access.groups import MigratedGroup

logger = logging.getLogger(__name__)


@dataclass
class ObjectPermission:
    group: str
    object_type: str
    object_id: str
    permission: str


@dataclass
class PersistedGroup:
    group: str
    object_type: str
    object_id: str
    permission: str


verificationErrors = []

def test_performance(
    ws,
    make_cluster_policy,
    make_ucx_group,
    make_warehouse,
    make_cluster,
    make_instance_pool,
    make_secret_scope,
    make_model,
    make_experiment,
    make_job,
    make_pipeline,
    make_schema,
    sql_backend,
    make_table,
    make_user,
    make_query,
    make_alert,
    make_dashboard,
    make_directory,
    make_repo,
    make_notebook,
    make_random,
    env_or_skip,
):
    NB_OF_TEST_WS_OBJECTS = 100
    NB_OF_FILES = 10000
    NB_OF_TEST_GROUPS = 1000
    NB_OF_SCHEMAS = 1

    test_database = make_schema()
    groups = create_groups(NB_OF_TEST_GROUPS, make_ucx_group, sql_backend, test_database)

    users = create_users(make_user)

    create_scopes(NB_OF_TEST_WS_OBJECTS, groups, make_secret_scope, sql_backend, test_database, ws)

    create_pipelines(NB_OF_TEST_WS_OBJECTS, groups, make_pipeline, sql_backend, test_database, ws)

    create_jobs(NB_OF_TEST_WS_OBJECTS, groups, make_job, sql_backend, test_database, ws)

    create_experiments(NB_OF_TEST_WS_OBJECTS, groups, make_experiment, sql_backend, test_database, ws)

    create_models(NB_OF_TEST_WS_OBJECTS, groups, make_model, sql_backend, test_database, ws)

    create_pools(NB_OF_TEST_WS_OBJECTS, groups, make_instance_pool, sql_backend, test_database, ws)

    create_warehouses(NB_OF_TEST_WS_OBJECTS, groups, make_warehouse, sql_backend, test_database, ws)

    create_clusters(NB_OF_TEST_WS_OBJECTS, groups, make_cluster, sql_backend, test_database, ws)

    create_policies(NB_OF_TEST_WS_OBJECTS, groups, make_cluster_policy, sql_backend, test_database, ws)

    create_queries_and_alerts(NB_OF_TEST_WS_OBJECTS, groups, make_alert, make_query, sql_backend, test_database, ws)

    create_dashboards(NB_OF_TEST_WS_OBJECTS, groups, make_dashboard, sql_backend, test_database, ws)

    create_repos(NB_OF_TEST_WS_OBJECTS, groups, make_repo, sql_backend, test_database, ws)

    create_dirs_n_notebookes(NB_OF_FILES, groups, make_directory, make_notebook, make_random, sql_backend,
                             test_database, ws)

    create_schemas_n_tables(NB_OF_SCHEMAS, groups, make_schema, make_table, sql_backend,
                            test_database, users)

    backup_group_prefix = "db-temp-"
    inventory_database = f"ucx_{make_random(4)}"
    test_groups = [_[0].display_name for _ in groups]

    install = WorkspaceInstaller.run_for_config(
        ws,
        WorkspaceConfig(
            inventory_database=inventory_database,
            instance_pool_id=env_or_skip("TEST_INSTANCE_POOL_ID"),
            include_group_names=test_groups,
            renamed_group_prefix=backup_group_prefix,
            log_level="DEBUG",
        ),
        sql_backend=sql_backend,
        prefix=make_random(4),
        override_clusters={
            "main": env_or_skip("TEST_DEFAULT_CLUSTER_ID"),
            "tacl": env_or_skip("TEST_LEGACY_TABLE_ACL_CLUSTER_ID"),
        },
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

    test_users = [_.display_name for _ in users]
    try_validate_tables(persisted_rows, sql_backend, test_database, test_groups, test_users, ws, "SCHEMA")
    try_validate_tables(persisted_rows, sql_backend, test_database, test_groups, test_users, ws, "TABLE")
    try_validate_tables(persisted_rows, sql_backend, test_database, test_groups, test_users, ws, "VIEW")

    try_validate_secrets(persisted_rows, sql_backend, test_database, test_groups, ws)
    validate_entitlements(sql_backend, test_database, ws)

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


def create_schemas_n_tables(NB_OF_SCHEMAS, groups, make_schema, make_table, sql_backend,
                            test_database, users):
    to_persist = []
    for i in range(NB_OF_SCHEMAS):
        schema = make_schema()
        schema_permissions = create_hive_metastore_permissions(
            groups, ["SELECT", "MODIFY", "READ_METADATA", "CREATE", "USAGE"]
        )
        grant_permissions(sql_backend, "SCHEMA", schema.name, schema_permissions)
        owner = groups[random.randint(1, len(groups) - 1)][0].display_name
        transfer_ownership(sql_backend, "SCHEMA", schema.name, owner)

        for group, permissions in schema_permissions.items():
            for permission in permissions:
                to_persist.append(ObjectPermission(group, "SCHEMA", schema.name, permission))
        to_persist.append(ObjectPermission(owner, "DATABASE", schema.name, "OWN"))

        nb_of_tables = random.randint(1, 10)
        logger.info(f"Creating {nb_of_tables} tables and views on schema {schema.name}")

        for j in range(nb_of_tables):
            table = make_table(schema_name=schema.name)
            full_name = schema.name + "." + table.name
            table_permission = create_hive_metastore_permissions(groups, ["SELECT", "MODIFY", "READ_METADATA"])
            grant_permissions(sql_backend, "TABLE", full_name, table_permission)

            owner = groups[random.randint(1, len(groups) - 1)][0].display_name
            transfer_ownership(sql_backend, "TABLE", full_name, owner)
            for group, permissions in table_permission.items():
                for permission in permissions:
                    to_persist.append(ObjectPermission(group, "TABLE", full_name, permission))
            to_persist.append(ObjectPermission(owner, "TABLE", full_name, "OWN"))

        for j in range(nb_of_tables):
            view = make_table(schema_name=schema.name, view=True, ctas="SELECT 2+2 AS four")
            full_name = schema.name + "." + view.name
            view_permission = create_hive_metastore_permissions(groups, ["SELECT"])
            grant_permissions(sql_backend, "VIEW", full_name, view_permission)

            owner = groups[random.randint(1, len(groups) - 1)][0].display_name
            transfer_ownership(sql_backend, "VIEW", full_name, owner)
            for group, permissions in view_permission.items():
                for permission in permissions:
                    to_persist.append(ObjectPermission(group, "VIEW", full_name, permission))
            to_persist.append(ObjectPermission(owner, "VIEW", full_name, "OWN"))
        for j in range(nb_of_tables):
            table = make_table(schema_name=schema.name)
            full_name = schema.name + "." + table.name
            table_permission = create_hive_metastore_permissions(groups, ["SELECT", "MODIFY", "READ_METADATA"])

            grant_permissions(sql_backend, "TABLE", full_name, table_permission)

            owner = users[random.randint(1, len(users) - 1)].display_name
            transfer_ownership(sql_backend, "TABLE", full_name, owner)
            for group, permissions in table_permission.items():
                for permission in permissions:
                    to_persist.append(ObjectPermission(group, "TABLE", full_name, permission))

            to_persist.append(ObjectPermission(owner, "TABLE", full_name, "OWN"))
        for j in range(nb_of_tables):
            view = make_table(schema_name=schema.name, view=True, ctas="SELECT 2+2 AS four")
            full_name = schema.name + "." + view.name
            view_permission = create_hive_metastore_permissions(groups, ["SELECT"])

            grant_permissions(sql_backend, "VIEW", full_name, view_permission)

            owner = users[random.randint(1, len(users) - 1)].display_name
            transfer_ownership(sql_backend, "VIEW", full_name, owner)
            for group, permissions in view_permission.items():
                for permission in permissions:
                    to_persist.append(ObjectPermission(group, "VIEW", full_name, permission))
            to_persist.append(ObjectPermission(owner, "VIEW", full_name, "OWN"))
    sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)


def create_dirs_n_notebookes(NB_OF_FILES, groups, make_directory, make_notebook, make_random, sql_backend,
                             test_database, ws):
    to_persist = []
    for i in range(NB_OF_FILES):
        test_dir = make_directory()
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

        nb_of_notebooks = random.randint(1, 100)
        logger.info(f"Creating {nb_of_notebooks} notebooks on directory {test_dir}")
        for j in range(nb_of_notebooks):
            nb = make_notebook(path=test_dir + "/" + make_random() + ".py")
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
    sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)


def create_repos(NB_OF_TEST_WS_OBJECTS, groups, make_repo, sql_backend, test_database, ws):
    to_persist = []
    for j in range(NB_OF_TEST_WS_OBJECTS):
        repo = make_repo()
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
    sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)


def create_dashboards(NB_OF_TEST_WS_OBJECTS, groups, make_dashboard, sql_backend, test_database, ws):
    to_persist = []
    for j in range(NB_OF_TEST_WS_OBJECTS):
        dashboard = make_dashboard()
        dashboard_permissions = create_permissions(
            groups, [sql.PermissionLevel.CAN_MANAGE, sql.PermissionLevel.CAN_RUN, sql.PermissionLevel.CAN_VIEW]
        )
        set_dbsql_permissions(dashboard, "id", sql.ObjectTypePlural.DASHBOARDS, ws, dashboard_permissions)
        for group, permission in dashboard_permissions.items():
            to_persist.append(
                ObjectPermission(group, sql.ObjectTypePlural.DASHBOARDS.value, dashboard.id, permission.value)
            )
    sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)


def create_queries_and_alerts(NB_OF_TEST_WS_OBJECTS, groups, make_alert, make_query, sql_backend, test_database, ws):
    to_persist = []
    for i in range(NB_OF_TEST_WS_OBJECTS):
        query = make_query()
        query_permissions = create_permissions(
            groups, [sql.PermissionLevel.CAN_MANAGE, sql.PermissionLevel.CAN_RUN, sql.PermissionLevel.CAN_VIEW]
        )
        set_dbsql_permissions(query, "id", sql.ObjectTypePlural.QUERIES, ws, query_permissions)
        for group, permission in query_permissions.items():
            to_persist.append(ObjectPermission(group, sql.ObjectTypePlural.QUERIES.value, query.id, permission.value))

        nb_of_alerts = random.randint(1, 10)
        logger.info(f"Creating {nb_of_alerts} alerts on top of query {query.name}")
        for j in range(nb_of_alerts):
            alert = make_alert(query_id=query.id)
            alert_permissions = create_permissions(
                groups, [sql.PermissionLevel.CAN_MANAGE, sql.PermissionLevel.CAN_RUN, sql.PermissionLevel.CAN_VIEW]
            )
            set_dbsql_permissions(alert, "id", sql.ObjectTypePlural.ALERTS, ws, alert_permissions)
            for group, permission in alert_permissions.items():
                to_persist.append(
                    ObjectPermission(group, sql.ObjectTypePlural.ALERTS.value, alert.id, permission.value)
                )
    sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)


def create_policies(NB_OF_TEST_WS_OBJECTS, groups, make_cluster_policy, sql_backend, test_database, ws):
    to_persist = []
    for i in range(NB_OF_TEST_WS_OBJECTS):
        ws_object = make_cluster_policy()
        ws_permissions = create_permissions(groups, [PermissionLevel.CAN_USE])
        set_permissions(ws_object, "policy_id", "cluster-policies", ws, ws_permissions)
        for group, permission in ws_permissions.items():
            to_persist.append(ObjectPermission(group, "cluster-policies", ws_object.policy_id, permission.value))
    sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)


def create_clusters(NB_OF_TEST_WS_OBJECTS, groups, make_cluster, sql_backend, test_database, ws):
    to_persist = []
    for i in range(NB_OF_TEST_WS_OBJECTS):
        ws_object = make_cluster(single_node=True)
        ws_permissions = create_permissions(
            groups, [PermissionLevel.CAN_MANAGE, PermissionLevel.CAN_RESTART, PermissionLevel.CAN_ATTACH_TO]
        )
        set_permissions(ws_object, "cluster_id", "clusters", ws, ws_permissions)
        for group, permission in ws_permissions.items():
            to_persist.append(ObjectPermission(group, "clusters", ws_object.cluster_id, permission.value))
    sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)


def create_warehouses(NB_OF_TEST_WS_OBJECTS, groups, make_warehouse, sql_backend, test_database, ws):
    to_persist = []
    for i in range(NB_OF_TEST_WS_OBJECTS):
        warehouse = make_warehouse()
        ws_permissions = create_permissions(groups, [PermissionLevel.CAN_MANAGE, PermissionLevel.CAN_USE])
        set_permissions(warehouse, "id", "warehouses", ws, ws_permissions)
        for group, permission in ws_permissions.items():
            to_persist.append(ObjectPermission(group, "warehouses", warehouse.id, permission.value))
    sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)


def create_pools(NB_OF_TEST_WS_OBJECTS, groups, make_instance_pool, sql_backend, test_database, ws):
    to_persist = []
    for i in range(NB_OF_TEST_WS_OBJECTS):
        ws_object = make_instance_pool()
        ws_permissions = create_permissions(groups, [PermissionLevel.CAN_MANAGE, PermissionLevel.CAN_ATTACH_TO])
        set_permissions(ws_object, "instance_pool_id", "instance-pools", ws, ws_permissions)
        for group, permission in ws_permissions.items():
            to_persist.append(ObjectPermission(group, "instance-pools", ws_object.instance_pool_id, permission.value))
    sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)


def create_models(NB_OF_TEST_WS_OBJECTS, groups, make_model, sql_backend, test_database, ws):
    to_persist = []
    for i in range(NB_OF_TEST_WS_OBJECTS):
        ws_object = make_model()
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
    sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)


def create_experiments(NB_OF_TEST_WS_OBJECTS, groups, make_experiment, sql_backend, test_database, ws):
    to_persist = []
    for i in range(NB_OF_TEST_WS_OBJECTS):
        ws_object = make_experiment()
        ws_permissions = create_permissions(
            groups, [PermissionLevel.CAN_READ, PermissionLevel.CAN_MANAGE, PermissionLevel.CAN_EDIT]
        )
        set_permissions(ws_object, "experiment_id", "experiments", ws, ws_permissions)
        for group, permission in ws_permissions.items():
            to_persist.append(ObjectPermission(group, "experiments", ws_object.experiment_id, permission.value))
    sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)


def create_jobs(NB_OF_TEST_WS_OBJECTS, groups, make_job, sql_backend, test_database, ws):
    to_persist = []
    for i in range(NB_OF_TEST_WS_OBJECTS):
        ws_object = make_job()
        ws_permissions = create_permissions(
            groups, [PermissionLevel.CAN_VIEW, PermissionLevel.CAN_MANAGE, PermissionLevel.CAN_MANAGE_RUN]
        )
        set_permissions(ws_object, "job_id", "jobs", ws, ws_permissions)
        for group, permission in ws_permissions.items():
            to_persist.append(ObjectPermission(group, "jobs", ws_object.job_id, permission.value))
    sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)


def create_pipelines(NB_OF_TEST_WS_OBJECTS, groups, make_pipeline, sql_backend, test_database, ws):
    to_persist = []
    for i in range(NB_OF_TEST_WS_OBJECTS):
        ws_object = make_pipeline()
        ws_permissions = create_permissions(
            groups, [PermissionLevel.CAN_VIEW, PermissionLevel.CAN_MANAGE, PermissionLevel.CAN_RUN]
        )
        set_permissions(ws_object, "pipeline_id", "pipelines", ws, ws_permissions)
        for group, permission in ws_permissions.items():
            to_persist.append(ObjectPermission(group, "pipelines", ws_object.pipeline_id, permission.value))
    sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)


def create_scopes(NB_OF_TEST_WS_OBJECTS, groups, make_secret_scope, sql_backend, test_database, ws:WorkspaceClient):
    to_persist = []
    for i in range(NB_OF_TEST_WS_OBJECTS):
        scope = make_secret_scope()
        ws_permissions = create_permissions(groups, [AclPermission.MANAGE, AclPermission.READ, AclPermission.WRITE])
        set_secrets_permissions(scope, ws, ws_permissions)
        for group, permission in ws_permissions.items():
            to_persist.append(ObjectPermission(group, "secrets", scope, permission.value))
    sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)


def create_groups(NB_OF_TEST_GROUPS, make_ucx_group, sql_backend, test_database):
    groups = []
    for i in range(NB_OF_TEST_GROUPS):
        entitlements_list = [
            "workspace-access",
            "databricks-sql-access",
            "allow-cluster-create",
            "allow-instance-pool-create",
        ]
        entitlements = [_ for _ in random.choices(entitlements_list, k=random.randint(1, 3))]
        ws_group, acc_group = make_ucx_group(entitlements=entitlements)
        groups.append((ws_group, acc_group))
        logger.info(f"Created group number {i + 1}")
    persist_groups(groups, sql_backend, test_database)
    return groups


def create_users(make_user):
    users = []
    for i in range(10):
        user = make_user()
        users.append(user)
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

            validate_that(len(acl.all_permissions) == 1,
                          f"More than 1 permission found in {object_type} {obj_id} -> {json.dumps(acl.as_dict())}")
            validate_that(acl.group_name in persisted_rows[obj_id],
                          f"{acl.group_name} not found in persisted rows for {object_type} {obj_id}")
            validate_that(
                acl.all_permissions[0].permission_level.value in persisted_rows[obj_id][acl.group_name],
            f"{acl.all_permissions[0].permission_level.value} not found in persisted rows for {object_type} {obj_id} and group {acl.group_name}")


def validate_entitlements(sql_backend, test_database, ws:WorkspaceClient):
    for row in sql_backend.fetch(f"SELECT * FROM {test_database.name}.groups"):
        mggrp = MigratedGroup(*row)
        try:
            migrated_group = ws.groups.get(mggrp.external_id)
        except Exception as e:
            logger.warning(f"There was a problem when fetching migrated group {mggrp.external_id}")
            logger.warning(e)
            continue

        target_entitlements = json.dumps([gg.as_dict() for gg in migrated_group.entitlements]) if migrated_group.entitlements else None
        validate_that(target_entitlements == mggrp.entitlements,
                      f"Migrated group {mggrp.name_in_workspace} does not have the same entitlements as the one in the account \n"
                      f"previous group = {mggrp.entitlements} \n"
                      f"new group = {target_entitlements}")



def try_validate_secrets(persisted_rows, sql_backend, test_database, test_groups, ws):
    try:
        validate_secrets(persisted_rows, sql_backend, test_database, test_groups, ws)
    except Exception as e:
        logger.warning(f"Something wrong happened when asserting objects -> {e}")


def validate_secrets(persisted_rows, sql_backend, test_database, test_groups, ws:WorkspaceClient):
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

        validate_that(acl.permission.value in persisted_rows[obj_id][acl.principal], f"permission {acl.permission.value} not found for scope {obj_id} and group {group}")


def try_validate_tables(persisted_rows, sql_backend, test_database, test_groups, users, ws, object_type):
    logger.info(f"Validating {object_type}")
    try:
        validate_tables(persisted_rows, sql_backend, test_database, test_groups, users, ws, object_type)
    except RuntimeError as e:
        logger.warning(f"Something wrong happened when asserting tables -> {e}")


def validate_tables(persisted_rows, sql_backend, test_database, test_groups, users, ws, object_type):
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

            validate_that(
                principal in persisted_rows[obj_id],
      f"{principal} not found in {object_type} {obj_id}")
            validate_that(
                action_type in persisted_rows[obj_id][principal],
       f"{principal} does not have {action_type} permission on {object_type} {obj_id}")


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

            validate_that(acl.group_name in persisted_rows[obj_id],
                          f"{acl.group_name} not found in persisted rows for {object_type} {obj_id}")
            validate_that(
                acl.permission_level.value in persisted_rows[obj_id][acl.group_name],
                f"{acl.permission_level.value} not found in persisted rows for {object_type} {obj_id} and group {acl.group_name}")


def try_validate_files(persisted_rows, sql_backend, test_database, test_groups, ws, object_type):
    try:
        validate_files(persisted_rows, sql_backend, test_database, test_groups, ws, object_type)
    except Exception as e:
        logger.warning(f"Something wrong happened when asserting files -> {e}")


def validate_files(persisted_rows, sql_backend, test_database, test_groups, ws:WorkspaceClient, object_type):
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

                validate_that(acl.group_name in persisted_rows[obj_id],
                          f"{acl.group_name} not found in persisted rows for {object_type} {obj_id}")

                for perm in non_inherited_permissions:
                    validate_that(
                        perm.permission_level.value in persisted_rows[obj_id][acl.group_name],
                        f"{perm.permission_level.value} not found in persisted rows for {object_type} {obj_id} and group {acl.group_name}")



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
            rand = random.randint(1, len(groups) - 1)
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
            rand = random.randint(1, len(groups) - 1)
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
                entitlements=json.dumps([gg.as_dict() for gg in ws_group.entitlements]) if ws_group.entitlements else None,
            )
        )

    sql_backend.save_table(f"{test_database.name}.groups", to_persist, MigratedGroup)

def validate_that(func, message):
    try:
        if not func:
            verificationErrors.append(message)
    except Exception as e:
        verificationErrors.append(message)
        logger.warning('Something wrong happened during the assertion: %s', e)

import logging
import random
import time
import unittest
from dataclasses import dataclass
from datetime import timedelta

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError
from databricks.sdk.retries import retried
from databricks.sdk.service import iam
from databricks.sdk.service import sql

from databricks.sdk.service.iam import PermissionLevel, Group
from databricks.sdk.service.serving import ServingEndpointAccessControlRequest, ServingEndpointPermissionLevel, \
    EndpointCoreConfigInput, ServedModelInput
from databricks.sdk.service.workspace import AclPermission, WorkspaceObjectAccessControlRequest, \
    WorkspaceObjectPermissionLevel

from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.install import WorkspaceInstaller
from databricks.labs.ucx.mixins.hardening import rate_limited
from databricks.labs.ucx.workspace_access.generic import RetryableError
from databricks.labs.ucx.workspace_access.groups import MigratedGroup
from integration.conftest import get_workspace_membership, make_ucx_group

logger = logging.getLogger(__name__)
from databricks.labs.ucx.mixins.fixtures import ws, sql_backend, make_warehouse, env_or_skip
import json


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


class PerformanceTest(unittest.TestCase):
    def setUp(self):
        self.verificationErrors = []

    def tearDown(self):
        self.assertEqual([], self.verificationErrors)

    @pytest.fixture(autouse=True)
    def test_performance(
        self,
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
        NB_OF_TEST_WS_OBJECTS = 10
        NB_OF_TEST_GROUPS = 30
        NB_OF_SCHEMAS = 1

        test_database = make_schema()
        groups = []
        for i in range(NB_OF_TEST_GROUPS):
            entitlements_list = ["workspace-access", "databricks-sql-access", "allow-cluster-create", "allow-instance-pool-create"]
            entitlements = [_ for _ in random.choices(entitlements_list, k=random.randint(1, 3))]
            ws_group, acc_group = make_ucx_group(entitlements=entitlements)
            groups.append((ws_group, acc_group))
            logger.info(f"Created group number {i+1}")
        self.persist_groups(groups, sql_backend, test_database)

        users = []
        for i in range(10):
            user = make_user()
            users.append(user)

        to_persist = []
        for i in range(NB_OF_TEST_WS_OBJECTS):
            scope = make_secret_scope()
            ws_permissions = self.create_permissions(groups, [AclPermission.MANAGE, AclPermission.READ, AclPermission.WRITE])
            self.set_secrets_permissions(scope, ws, ws_permissions)
            for group, permission in ws_permissions.items():
                to_persist.append(ObjectPermission(group, "secrets", scope, permission.value))
        sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)

        to_persist = []
        for i in range(NB_OF_TEST_WS_OBJECTS):
            ws_object = make_pipeline()
            ws_permissions = self.create_permissions(groups, [PermissionLevel.CAN_VIEW, PermissionLevel.CAN_MANAGE, PermissionLevel.CAN_RUN])
            self.set_permissions(ws_object, "pipeline_id", "pipelines", ws, ws_permissions)
            for group, permission in ws_permissions.items():
                to_persist.append(ObjectPermission(group, "pipelines", ws_object.pipeline_id, permission.value))
        sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)

        to_persist = []
        for i in range(NB_OF_TEST_WS_OBJECTS):
            ws_object = make_job()
            ws_permissions = self.create_permissions(groups, [PermissionLevel.CAN_VIEW, PermissionLevel.CAN_MANAGE, PermissionLevel.CAN_MANAGE_RUN])
            self.set_permissions(ws_object, "job_id", "jobs", ws, ws_permissions)
            for group, permission in ws_permissions.items():
                to_persist.append(ObjectPermission(group, "jobs", ws_object.job_id, permission.value))
        sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)

        to_persist = []
        for i in range(NB_OF_TEST_WS_OBJECTS):
            ws_object = make_experiment()
            ws_permissions = self.create_permissions(groups, [PermissionLevel.CAN_READ, PermissionLevel.CAN_MANAGE, PermissionLevel.CAN_EDIT])
            self.set_permissions(ws_object, "experiment_id", "experiments", ws, ws_permissions)
            for group, permission in ws_permissions.items():
                to_persist.append(ObjectPermission(group, "experiments", ws_object.experiment_id, permission.value))
        sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)

        to_persist = []
        for i in range(NB_OF_TEST_WS_OBJECTS):
            ws_object = make_model()
            ws_permissions = self.create_permissions(groups, [PermissionLevel.CAN_READ, PermissionLevel.CAN_MANAGE, PermissionLevel.CAN_EDIT, PermissionLevel.CAN_MANAGE_PRODUCTION_VERSIONS, PermissionLevel.CAN_MANAGE_STAGING_VERSIONS])
            self.set_permissions(ws_object, "id", "registered-models", ws, ws_permissions)
            for group, permission in ws_permissions.items():
                to_persist.append(ObjectPermission(group, "registered-models", ws_object.id, permission.value))
        sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)

        to_persist = []
        for i in range(NB_OF_TEST_WS_OBJECTS):
            ws_object = make_instance_pool()
            ws_permissions = self.create_permissions(groups, [PermissionLevel.CAN_MANAGE, PermissionLevel.CAN_ATTACH_TO])
            self.set_permissions(ws_object, "instance_pool_id", "instance-pools", ws, ws_permissions)
            for group, permission in ws_permissions.items():
                to_persist.append(ObjectPermission(group, "instance-pools", ws_object.instance_pool_id, permission.value))
        sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)

        to_persist = []
        for i in range(NB_OF_TEST_WS_OBJECTS):
            warehouse = make_warehouse()
            ws_permissions = self.create_permissions(groups, [PermissionLevel.CAN_MANAGE, PermissionLevel.CAN_USE])
            self.set_permissions(warehouse, "id", "warehouses", ws, ws_permissions)
            for group, permission in ws_permissions.items():
                to_persist.append(ObjectPermission(group, "warehouses", warehouse.id, permission.value))
        sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)

        to_persist = []
        for i in range(NB_OF_TEST_WS_OBJECTS):
            ws_object = make_cluster(single_node=True)
            ws_permissions = self.create_permissions(groups, [PermissionLevel.CAN_MANAGE, PermissionLevel.CAN_RESTART, PermissionLevel.CAN_ATTACH_TO])
            self.set_permissions(ws_object, "cluster_id", "clusters", ws, ws_permissions)
            for group, permission in ws_permissions.items():
                to_persist.append(ObjectPermission(group, "clusters", ws_object.cluster_id, permission.value))
        sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)

        to_persist = []
        for i in range(NB_OF_TEST_WS_OBJECTS):
            ws_object = make_cluster_policy()
            ws_permissions = self.create_permissions(groups, [PermissionLevel.CAN_USE])
            self.set_permissions(ws_object, "policy_id", "cluster-policies", ws, ws_permissions)
            for group, permission in ws_permissions.items():
                to_persist.append(ObjectPermission(group, "cluster-policies", ws_object.policy_id, permission.value))
        sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)

        to_persist = []
        for i in range(NB_OF_TEST_WS_OBJECTS):
            query = make_query()
            query_permissions = self.create_permissions(groups, [sql.PermissionLevel.CAN_MANAGE, sql.PermissionLevel.CAN_RUN, sql.PermissionLevel.CAN_VIEW])
            self.set_dbsql_permissions(query, "id", sql.ObjectTypePlural.QUERIES, ws, query_permissions)
            for group, permission in query_permissions.items():
                to_persist.append(ObjectPermission(group, sql.ObjectTypePlural.QUERIES.value, query.id, permission.value))
            for j in range(NB_OF_TEST_WS_OBJECTS):
                alert = make_alert(query_id=query.id)
                alert_permissions = self.create_permissions(groups, [sql.PermissionLevel.CAN_MANAGE, sql.PermissionLevel.CAN_RUN, sql.PermissionLevel.CAN_VIEW])
                self.set_dbsql_permissions(alert, "id", sql.ObjectTypePlural.ALERTS, ws, alert_permissions)
                for group, permission in alert_permissions.items():
                    to_persist.append(ObjectPermission(group, sql.ObjectTypePlural.ALERTS.value, alert.id, permission.value))
        sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)

        to_persist = []
        for j in range(NB_OF_TEST_WS_OBJECTS):
            dashboard = make_dashboard()
            dashboard_permissions = self.create_permissions(groups, [sql.PermissionLevel.CAN_MANAGE, sql.PermissionLevel.CAN_RUN, sql.PermissionLevel.CAN_VIEW])
            self.set_dbsql_permissions(dashboard, "id", sql.ObjectTypePlural.DASHBOARDS, ws, dashboard_permissions)
            for group, permission in dashboard_permissions.items():
                to_persist.append(ObjectPermission(group, sql.ObjectTypePlural.DASHBOARDS.value, dashboard.id, permission.value))
        sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)

        to_persist = []
        for j in range(NB_OF_TEST_WS_OBJECTS):
            repo = make_repo()
            repo_perms = self.create_permissions(groups, [WorkspaceObjectPermissionLevel.CAN_MANAGE, WorkspaceObjectPermissionLevel.CAN_RUN, WorkspaceObjectPermissionLevel.CAN_EDIT, WorkspaceObjectPermissionLevel.CAN_READ])
            self.assign_ws_local_permissions("repos", repo_perms, repo.id, ws)
            for group, permission in repo_perms.items():
                to_persist.append(ObjectPermission(group, "repos", repo.id, permission.value))
        sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)

        to_persist = []
        for i in range(NB_OF_TEST_WS_OBJECTS):
            dir = make_directory()
            stat = ws.workspace.get_status(dir)
            dir_perms = self.create_permissions(groups, [WorkspaceObjectPermissionLevel.CAN_MANAGE, WorkspaceObjectPermissionLevel.CAN_RUN, WorkspaceObjectPermissionLevel.CAN_EDIT, WorkspaceObjectPermissionLevel.CAN_READ])
            self.assign_ws_local_permissions("directories", dir_perms, stat.object_id, ws)
            for group, permission in dir_perms.items():
                to_persist.append(ObjectPermission(group, "directories", stat.object_id, permission.value))
            for j in range(NB_OF_TEST_WS_OBJECTS):
                nb = make_notebook(path = dir + "/" + make_random() + ".py")
                stat = ws.workspace.get_status(nb)
                nb_perms = self.create_permissions(groups, [WorkspaceObjectPermissionLevel.CAN_MANAGE, WorkspaceObjectPermissionLevel.CAN_RUN, WorkspaceObjectPermissionLevel.CAN_EDIT, WorkspaceObjectPermissionLevel.CAN_READ])
                self.assign_ws_local_permissions("notebooks", nb_perms, stat.object_id, ws)
                for group, permission in dir_perms.items():
                    to_persist.append(ObjectPermission(group, "notebooks", stat.object_id, permission.value))
        sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)


        to_persist = []
        for i in range(NB_OF_SCHEMAS):
            schema = make_schema()
            schema_permissions = self.create_hive_metastore_permissions(groups, ["SELECT", "MODIFY", "READ_METADATA", "CREATE", "USAGE"])
            self.grant_permissions(sql_backend, "SCHEMA", schema.name, schema_permissions)
            owner = groups[random.randint(1, len(groups) - 1)][0].display_name
            self.transfer_ownership(sql_backend, "SCHEMA", schema.name, owner)

            for group, permission in schema_permissions.items():
                to_persist.append(ObjectPermission(group, "SCHEMA", schema.name, permission))
            to_persist.append(ObjectPermission(owner, "SCHEMA", schema.name, "OWNER"))

            if random.randint(1, NB_OF_TEST_WS_OBJECTS) < 8:
                for j in range(2):
                    table = make_table(schema_name=schema.name)
                    full_name = schema.name + "." + table.name
                    table_permission = self.create_hive_metastore_permissions(groups, ["SELECT", "MODIFY", "READ_METADATA"])
                    self.grant_permissions(sql_backend, "TABLE", full_name, table_permission)

                    owner = groups[random.randint(1, len(groups) - 1)][0].display_name
                    self.transfer_ownership(sql_backend, "TABLE", full_name, owner)
                    for group, permission in table_permission.items():
                        to_persist.append(ObjectPermission(group, "TABLE", full_name, permission))
                    to_persist.append(ObjectPermission(owner, "TABLE", full_name, "OWNER"))

                for j in range(2):
                    view = make_table(schema_name=schema.name, view=True, ctas="SELECT 2+2 AS four")
                    full_name = schema.name + "." + view.name
                    view_permission = self.create_hive_metastore_permissions(groups, ["SELECT"])
                    self.grant_permissions(sql_backend, "VIEW", full_name, view_permission)

                    owner = groups[random.randint(1, len(groups) - 1)][0].display_name
                    self.transfer_ownership(sql_backend, "VIEW", full_name, owner)
                    for group, permission in view_permission.items():
                        to_persist.append(ObjectPermission(group, "VIEW", full_name, permission))
                    to_persist.append(ObjectPermission(owner, "VIEW", full_name, "OWNER"))
                for j in range(2):
                    table = make_table(schema_name=schema.name)
                    full_name = schema.name + "." + table.name
                    table_permission = self.create_hive_metastore_permissions(groups, ["SELECT", "MODIFY", "READ_METADATA"])

                    self.grant_permissions(sql_backend, "TABLE", full_name, table_permission)

                    owner = users[random.randint(1, len(users) - 1)].display_name
                    self.transfer_ownership(sql_backend, "TABLE", full_name, owner)
                    for group, permission in table_permission.items():
                        to_persist.append(ObjectPermission(group, "TABLE", full_name, permission))
                    to_persist.append(ObjectPermission(owner, "TABLE", full_name, "OWNER"))
                for j in range(2):
                    view = make_table(schema_name=schema.name, view=True, ctas="SELECT 2+2 AS four")
                    full_name = schema.name + "." + view.name
                    view_permission = self.create_hive_metastore_permissions(groups, ["SELECT"])

                    self.grant_permissions(sql_backend, "VIEW", full_name, view_permission)

                    owner = users[random.randint(1, len(users) - 1)].display_name
                    self.transfer_ownership(sql_backend, "VIEW", full_name, owner)
                    for group, permission in view_permission.items():
                        to_persist.append(ObjectPermission(group, "VIEW", full_name, permission))
                    to_persist.append(ObjectPermission(owner, "VIEW", full_name, "OWNER"))
        sql_backend.save_table(f"{test_database.name}.objects", to_persist, ObjectPermission)

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

        persisted_rows = {}
        for row in sql_backend.fetch(f"SELECT * FROM {test_database.name}.objects"):
            mgted_grp = ObjectPermission(*row)
            if mgted_grp.object_id in persisted_rows:
                previous_perms = persisted_rows[mgted_grp.object_id]
                previous_perms[mgted_grp.group] = mgted_grp.permission
                persisted_rows[mgted_grp.object_id] = previous_perms
            else:
                persisted_rows[mgted_grp.object_id] = {mgted_grp.group: mgted_grp.permission}

        self.try_validate_object(persisted_rows, sql_backend, test_database, test_groups, ws, "pipelines")
        self.try_validate_object(persisted_rows, sql_backend, test_database, test_groups, ws, "jobs")
        self.try_validate_object(persisted_rows, sql_backend, test_database, test_groups, ws, "experiments")
        self.try_validate_object(persisted_rows, sql_backend, test_database, test_groups, ws, "registered-models")
        self.try_validate_object(persisted_rows, sql_backend, test_database, test_groups, ws, "instance-pools")
        self.try_validate_object(persisted_rows, sql_backend, test_database, test_groups, ws, "warehouses")
        self.try_validate_object(persisted_rows, sql_backend, test_database, test_groups, ws, "clusters")
        self.try_validate_object(persisted_rows, sql_backend, test_database, test_groups, ws, "cluster-policies")
        self.try_validate_sql_objects(persisted_rows, sql_backend, test_database, test_groups, ws, sql.ObjectTypePlural.ALERTS)
        self.try_validate_sql_objects(persisted_rows, sql_backend, test_database, test_groups, ws, sql.ObjectTypePlural.DASHBOARDS)
        self.try_validate_sql_objects(persisted_rows, sql_backend, test_database, test_groups, ws, sql.ObjectTypePlural.QUERIES)
        self.try_validate_files(persisted_rows, sql_backend, test_database, test_groups, ws, "notebooks")
        self.try_validate_files(persisted_rows, sql_backend, test_database, test_groups, ws, "directories")

        test_users = [_.display_name for _ in users]
        self.try_validate_tables(persisted_rows, sql_backend, test_database, test_groups, test_users, ws,"SCHEMA")
        self.try_validate_tables(persisted_rows, sql_backend, test_database, test_groups, test_users, ws, "TABLE")
        self.try_validate_tables(persisted_rows, sql_backend, test_database, test_groups, test_users, ws, "VIEW")

        self.try_validate_secrets(persisted_rows, sql_backend, test_database, test_groups, ws)

    def try_validate_object(self, persisted_rows, sql_backend, test_database, test_groups, ws, object_type):
        try:
            self.validate_objects(persisted_rows, sql_backend, test_database, test_groups, ws, object_type)
        except Exception as e:
            logger.warning(f"Something wrong happened when asserting objects -> {e}")

    def validate_objects(self, persisted_rows, sql_backend, test_database, test_groups, ws, object_type):
        for pipe_id in sql_backend.fetch(f"SELECT distinct object_id FROM {test_database.name}.objects where object_type = '{object_type}'"):
            obj_id = pipe_id["object_id"]
            acls = ws.permissions.get(object_type, obj_id).access_control_list #TODO: can fail, must capture the exception and move on
            for acl in acls:
                if acl.group_name in ["users", "admins", "account users"]:
                    continue
                if acl.group_name not in test_groups:
                    continue
                try: self.assertEqual(len(acl.all_permissions), 1)
                except AssertionError as e: self.verificationErrors.append(str(e))
                try: self.assertTrue(acl.group_name in persisted_rows[obj_id])
                except AssertionError as e: self.verificationErrors.append(str(e))
                try: self.assertEqual(acl.all_permissions[0].permission_level.value, persisted_rows[obj_id][acl.group_name])
                except AssertionError as e: self.verificationErrors.append(str(e))

    def try_validate_secrets(self, persisted_rows, sql_backend, test_database, test_groups, ws):
        try:
            self.validate_secrets(persisted_rows, sql_backend, test_database, test_groups, ws)
        except Exception as e:
            logger.warning(f"Something wrong happened when asserting objects -> {e}" )

    def validate_secrets(self, persisted_rows, sql_backend, test_database, test_groups, ws):
        for pipe_id in sql_backend.fetch(f"SELECT distinct group, object_id FROM {test_database.name}.objects where object_type = 'secrets'"):
            obj_id = pipe_id["object_id"]
            group = pipe_id["group"]
            acl = ws.secrets.get_acl(obj_id, group)
            if acl.principal not in test_groups:
                continue
            try:
                self.assertEqual(acl.permission.value, persisted_rows[obj_id][acl.principal])
            except AssertionError as e:
                self.verificationErrors.append(str(e))

    def try_validate_tables(self, persisted_rows, sql_backend, test_database, test_groups, users, ws, object_type):
        try:
            self.validate_tables(persisted_rows, sql_backend, test_database, test_groups, users, ws, object_type)
        except RuntimeError as e:
            logger.warning(f"Something wrong happened when asserting tables -> {e}" )

    def validate_tables(self, persisted_rows, sql_backend, test_database, test_groups, users, ws, object_type):
        for pipe_id in sql_backend.fetch(f"SELECT distinct object_id FROM {test_database.name}.objects where object_type = '{object_type}'"):
            obj_id = pipe_id["object_id"]
            for row in sql_backend.fetch(f"SHOW GRANTS ON {object_type} {obj_id}"):
                (principal, action_type, object_type, _) = row
                if principal in ["users", "admins", "account users"]:
                    continue
                if principal not in test_groups or principal not in users:
                    continue
                assert principal in persisted_rows[obj_id]
                assert action_type == persisted_rows[obj_id][principal]

                try: self.assertTrue(principal in persisted_rows[obj_id])
                except AssertionError as e: self.verificationErrors.append(str(e))
                try: self.assertEqual(action_type, persisted_rows[obj_id][principal])
                except AssertionError as e: self.verificationErrors.append(str(e))

    def try_validate_sql_objects(self, persisted_rows, sql_backend, test_database, test_groups, ws, object_type):
        try:
            self.validate_sql_objects(persisted_rows, sql_backend, test_database, test_groups, ws, object_type)
        except Exception as e:
            logger.warning(f"Something wrong happened when asserting SQL objects -> {e}" )

    def validate_sql_objects(self, persisted_rows, sql_backend, test_database, test_groups, ws, object_type):
        for pipe_id in sql_backend.fetch(f"SELECT distinct object_id FROM {test_database.name}.objects where object_type = '{object_type.value}'"):
            obj_id = pipe_id["object_id"]
            acls = ws.dbsql_permissions.get(object_type, obj_id).access_control_list
            for acl in acls:
                if acl.group_name in ["users", "admins", "account users"]:
                    continue
                if acl.group_name not in test_groups:
                    continue
                try: self.assertTrue(acl.group_name in persisted_rows[obj_id])
                except AssertionError as e: self.verificationErrors.append(str(e))
                try: self.assertEqual(acl.permission_level.value, persisted_rows[obj_id][acl.group_name])
                except AssertionError as e: self.verificationErrors.append(str(e))

    def try_validate_files(self, persisted_rows, sql_backend, test_database, test_groups, ws, object_type):
        try:
            self.validate_files(persisted_rows, sql_backend, test_database, test_groups, ws, object_type)
        except Exception as e:
            logger.warning(f"Something wrong happened when asserting files -> {e}")

    def validate_files(self, persisted_rows, sql_backend, test_database, test_groups, ws, object_type):
        for pipe_id in sql_backend.fetch(f"SELECT distinct object_id FROM {test_database.name}.objects where object_type = '{object_type}'"):
            obj_id = pipe_id["object_id"]
            acls = ws.workspace.get_permissions(object_type, obj_id).access_control_list
            for acl in acls:
                if acl.group_name in ["users", "admins", "account users"]:
                    continue
                if acl.group_name not in test_groups:
                    continue
                try: self.assertEqual(len(acl.all_permissions), 1)
                except AssertionError as e: self.verificationErrors.append(str(e))
                try: self.assertTrue(acl.group_name in persisted_rows[obj_id])
                except AssertionError as e: self.verificationErrors.append(str(e))
                try: self.assertEqual(acl.all_permissions[0].permission_level.value, persisted_rows[obj_id][acl.group_name])
                except AssertionError as e: self.verificationErrors.append(str(e))


    def assign_ws_local_permissions(self, object_type, dir_perms, object_id, ws):
        acls = []
        for group, permission in dir_perms.items():
            acls.append(WorkspaceObjectAccessControlRequest(group_name=group, permission_level=permission))
        ws.workspace.set_permissions(workspace_object_type=object_type, workspace_object_id=object_id, access_control_list=acls)
        return acls

    def transfer_ownership(self, sql_backend, securable, name, owner):
        sql_backend.execute(f"ALTER {securable} {name} OWNER TO `{owner}`")


    def grant_permissions(self, sql_backend, securable, name, permissions):
        for group, permission in permissions.items():
            permissions = ",".join(permission)
            sql_backend.execute(f"GRANT {permissions} ON {securable} {name} TO `{group}`")


    def create_hive_metastore_permissions(self, groups, all_permissions):
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

    def create_permissions(self, groups, all_permissions):
        schema_permissions = {}
        for permission in all_permissions:
            for j in range(10):
                rand = random.randint(1, len(groups) - 1)
                ws_group = groups[rand][0].display_name
                schema_permissions[ws_group] = permission
        return schema_permissions


    def set_permissions(self, ws_object, id_attribute, object_type, ws, permissions):
        request_object_id = getattr(ws_object, id_attribute)
        acls = []
        for group, permission in permissions.items():
            acls.append(iam.AccessControlRequest(group_name=group, permission_level=permission))
        ws.permissions.update(request_object_type=object_type, request_object_id=request_object_id, access_control_list=acls)


    def set_secrets_permissions(self, scope_name, ws, permissions):
        for group, permission in permissions.items():
            ws.secrets.put_acl(scope=scope_name, permission=permission, principal=group)


    def set_dbsql_permissions(self, ws_object, id_attribute, object_type, ws, group_permissions):
        acls = []
        for group, permission in group_permissions.items():
            acls.append(sql.AccessControl(group_name=group, permission_level=permission))
        request_object_id = getattr(ws_object, id_attribute)
        self.set_perm_or_retry(object_type=object_type, object_id=request_object_id, acl=acls, ws=ws)


    @rate_limited(max_requests=30)
    def set_perm_or_retry(self, object_type: sql.ObjectTypePlural, object_id: str, acl: list[sql.AccessControl], ws):
        set_retry_on_value_error = retried(on=[RetryableError], timeout=timedelta(minutes=1))
        set_retried_check = set_retry_on_value_error(self.safe_set_permissions)
        return set_retried_check(object_type, object_id, acl, ws)


    def safe_set_permissions(self,
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
                raise RetryableError(message=msg) from e


    def persist_groups(self, groups: [(Group, Group)], sql_backend, test_database):
        to_persist = []
        for (ws_group, acc_group) in groups:
            to_persist.append(MigratedGroup(
                id_in_workspace=ws_group.id,
                name_in_workspace=ws_group.display_name,
                name_in_account=acc_group.display_name,
                temporary_name="",
                external_id=acc_group.id,
                members=json.dumps([gg.as_dict() for gg in ws_group.members]) if ws_group.members else None,
                roles=json.dumps([gg.as_dict() for gg in ws_group.roles]) if ws_group.roles else None,
                entitlements=json.dumps(
                    [gg.as_dict() for gg in ws_group.entitlements]) if ws_group.entitlements else None,
            ))

        sql_backend.save_table(f"{test_database.name}.groups", to_persist, MigratedGroup)




import functools
import json
import logging
import os.path
import sys
from collections.abc import Callable
from dataclasses import replace
from datetime import timedelta
from functools import cached_property

import pytest  # pylint: disable=wrong-import-order
from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.installer import InstallState, RawState
from databricks.labs.blueprint.parallel import Threads, ManyError
from databricks.labs.blueprint.tui import MockPrompts
from databricks.labs.blueprint.wheels import ProductInfo
from databricks.sdk.errors import (
    AlreadyExists,
    InvalidParameterValue,
    NotFound,
)
from databricks.sdk.retries import retried
from databricks.sdk.service import compute, sql
from databricks.sdk.service.catalog import TableInfo
from databricks.sdk.service.iam import PermissionLevel

import databricks
from databricks.labs.ucx.assessment.aws import AWSRoleAction
from databricks.labs.ucx.azure.access import StoragePermissionMapping
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.hive_metastore.grants import Grant
from databricks.labs.ucx.hive_metastore.locations import Mount
from databricks.labs.ucx.hive_metastore.mapping import Rule
from databricks.labs.ucx.hive_metastore.tables import Table
from databricks.labs.ucx.install import WorkspaceInstallation, WorkspaceInstaller
from databricks.labs.ucx.installer.workflows import (
    DeployedWorkflows,
    WorkflowsDeployment,
)
from databricks.labs.ucx.runtime import Workflows
from databricks.labs.ucx.workspace_access import redash
from databricks.labs.ucx.workspace_access.generic import (
    GenericPermissionsSupport,
    Listing,
)
from databricks.labs.ucx.workspace_access.groups import GroupManager, MigratedGroup
from databricks.labs.ucx.workspace_access.manager import PermissionManager
from databricks.labs.ucx.workspace_access.redash import RedashPermissionsSupport
from tests.integration.conftest import (
    TestRuntimeContext,
)

logger = logging.getLogger(__name__)


class TestInstallationContext(TestRuntimeContext):
    def __init__(
            self,
            make_table_fixture,
            make_schema_fixture,
            make_udf_fixture,
            make_group_fixture,
            env_or_skip_fixture,
            make_random_fixture,
            make_acc_group_fixture,
            make_user_fixture,
    ):
        super().__init__(
            make_table_fixture, make_schema_fixture, make_udf_fixture, make_group_fixture, env_or_skip_fixture
        )
        self._make_random = make_random_fixture
        self._make_acc_group = make_acc_group_fixture
        self._make_user = make_user_fixture

    def make_ucx_group(self, workspace_group_name=None, account_group_name=None):
        if not workspace_group_name:
            workspace_group_name = f"ucx_{self._make_random(4)}"
        if not account_group_name:
            account_group_name = workspace_group_name
        user = self._make_user()
        members = [user.id]
        ws_group = self.make_group(
            display_name=workspace_group_name,
            members=members,
            entitlements=["allow-cluster-create"],
        )
        acc_group = self._make_acc_group(display_name=account_group_name, members=members)
        return ws_group, acc_group

    @cached_property
    def running_clusters(self):
        logger.debug("Waiting for clusters to start...")
        default_cluster_id = self._env_or_skip("TEST_DEFAULT_CLUSTER_ID")
        tacl_cluster_id = self._env_or_skip("TEST_LEGACY_TABLE_ACL_CLUSTER_ID")
        table_migration_cluster_id = self._env_or_skip("TEST_USER_ISOLATION_CLUSTER_ID")
        ensure_cluster_is_running = self.workspace_client.clusters.ensure_cluster_is_running
        Threads.strict(
            "ensure clusters running",
            [
                functools.partial(ensure_cluster_is_running, default_cluster_id),
                functools.partial(ensure_cluster_is_running, tacl_cluster_id),
                functools.partial(ensure_cluster_is_running, table_migration_cluster_id),
            ],
        )
        logger.debug("Waiting for clusters to start...")
        return default_cluster_id, tacl_cluster_id, table_migration_cluster_id

    @cached_property
    def installation(self):
        return Installation(self.workspace_client, self.product_info.product_name())

    @cached_property
    def environ(self) -> dict[str, str]:
        return {**os.environ}

    @cached_property
    def workspace_installer(self):
        return WorkspaceInstaller(
            self.prompts,
            self.installation,
            self.workspace_client,
            self.product_info,
            self.environ,
        )

    @cached_property
    def include_object_permissions(self):
        return None

    @cached_property
    def config(self) -> WorkspaceConfig:
        workspace_config = self.workspace_installer.configure()
        default_cluster_id, tacl_cluster_id, table_migration_cluster_id = self.running_clusters
        workspace_config = replace(
            workspace_config,
            override_clusters={
                "main": default_cluster_id,
                "tacl": tacl_cluster_id,
                "table_migration": table_migration_cluster_id,
            },
            workspace_start_path=self.installation.install_folder(),
            renamed_group_prefix=self.renamed_group_prefix,
            include_group_names=self.created_groups,
            include_databases=self.created_databases,
            include_object_permissions=self.include_object_permissions,
        )
        self.installation.save(workspace_config)
        return workspace_config

    @cached_property
    def product_info(self):
        return ProductInfo.for_testing(WorkspaceConfig)

    @cached_property
    def tasks(self):
        return Workflows.all().tasks()

    @cached_property
    def skip_dashboards(self):
        return True

    @cached_property
    def workflows_deployment(self):
        return WorkflowsDeployment(
            self.config,
            self.installation,
            self.install_state,
            self.workspace_client,
            self.product_info.wheels(self.workspace_client),
            self.product_info,
            timedelta(minutes=3),
            self.tasks,
            skip_dashboards=self.skip_dashboards,
        )

    @cached_property
    def workspace_installation(self):
        return WorkspaceInstallation(
            self.config,
            self.installation,
            self.install_state,
            self.sql_backend,
            self.workspace_client,
            self.workflows_deployment,
            self.prompts,
            self.product_info,
            skip_dashboards=self.skip_dashboards,
        )

    @cached_property
    def extend_prompts(self):
        return {}

    @cached_property
    def renamed_group_prefix(self):
        return f"rename-{self.product_info.product_name()}-"

    @cached_property
    def prompts(self):
        return MockPrompts(
            {
                r'Open job overview in your browser.*': 'no',
                r'Do you want to uninstall ucx.*': 'yes',
                r'Do you want to delete the inventory database.*': 'yes',
                r".*PRO or SERVERLESS SQL warehouse.*": "1",
                r"Choose how to map the workspace groups.*": "1",
                r".*Inventory Database.*": self.inventory_database,
                r".*Backup prefix*": self.renamed_group_prefix,
                r".*": "",
            }
            | (self.extend_prompts or {})
        )


@pytest.fixture
def installation_ctx(  # pylint: disable=too-many-arguments
        ws,
        sql_backend,
        make_table,
        make_schema,
        make_udf,
        make_group,
        env_or_skip,
        make_random,
        make_acc_group,
        make_user,
):
    ctx = TestInstallationContext(
        make_table,
        make_schema,
        make_udf,
        make_group,
        env_or_skip,
        make_random,
        make_acc_group,
        make_user,
    )
    yield ctx.replace(workspace_client=ws, sql_backend=sql_backend)
    ctx.workspace_installation.uninstall()


@pytest.fixture
def new_installation(ws, sql_backend, env_or_skip, make_random):
    cleanup = []

    def factory(
            config_transform: Callable[[WorkspaceConfig], WorkspaceConfig] | None = None,
            installation: Installation | None = None,
            product_info: ProductInfo | None = None,
            environ: dict[str, str] | None = None,
            extend_prompts: dict[str, str] | None = None,
            inventory_schema_name: str | None = None,
            skip_dashboards: bool = True,
    ):
        logger.debug("Creating new installation...")
        if not product_info:
            product_info = ProductInfo.for_testing(WorkspaceConfig)
        if not environ:
            environ = {}
        if not inventory_schema_name:
            inventory_schema_name = f"ucx_S{make_random(4).lower()}"
        renamed_group_prefix = f"rename-{product_info.product_name()}-"
        prompts = MockPrompts(
            {
                r'Open job overview in your browser.*': 'no',
                r'Do you want to uninstall ucx.*': 'yes',
                r'Do you want to delete the inventory database.*': 'yes',
                r".*PRO or SERVERLESS SQL warehouse.*": "1",
                r"Choose how to map the workspace groups.*": "1",
                r".*connect to the external metastore?.*": "yes",
                r"Choose a cluster policy": "0",
                r".*Inventory Database.*": inventory_schema_name,
                r".*Backup prefix*": renamed_group_prefix,
                r".*": "",
            }
            | (extend_prompts or {})
        )

        logger.debug("Waiting for clusters to start...")
        default_cluster_id = env_or_skip("TEST_DEFAULT_CLUSTER_ID")
        tacl_cluster_id = env_or_skip("TEST_LEGACY_TABLE_ACL_CLUSTER_ID")
        table_migration_cluster_id = env_or_skip("TEST_USER_ISOLATION_CLUSTER_ID")
        Threads.strict(
            "ensure clusters running",
            [
                functools.partial(ws.clusters.ensure_cluster_is_running, default_cluster_id),
                functools.partial(ws.clusters.ensure_cluster_is_running, tacl_cluster_id),
                functools.partial(ws.clusters.ensure_cluster_is_running, table_migration_cluster_id),
            ],
        )
        logger.debug("Waiting for clusters to start...")

        if not installation:
            installation = Installation(ws, product_info.product_name())
        install_state = InstallState.from_installation(installation)
        installer = WorkspaceInstaller(prompts, installation, ws, product_info, environ)
        workspace_config = installer.configure()
        installation = product_info.current_installation(ws)
        overrides = {"main": default_cluster_id, "tacl": tacl_cluster_id, "table_migration": table_migration_cluster_id}
        workspace_config.override_clusters = overrides

        if workspace_config.workspace_start_path == '/':
            workspace_config.workspace_start_path = installation.install_folder()
        if config_transform:
            workspace_config = config_transform(workspace_config)

        installation.save(workspace_config)

        tasks = Workflows.all().tasks()

        # TODO: see if we want to move building wheel as a context manager for yield factory,
        # so that we can shave off couple of seconds and build wheel only once per session
        # instead of every test
        workflows_installation = WorkflowsDeployment(
            workspace_config,
            installation,
            install_state,
            ws,
            product_info.wheels(ws),
            product_info,
            timedelta(minutes=3),
            tasks,
            skip_dashboards=skip_dashboards,
        )
        workspace_installation = WorkspaceInstallation(
            workspace_config,
            installation,
            install_state,
            sql_backend,
            ws,
            workflows_installation,
            prompts,
            product_info,
            skip_dashboards=skip_dashboards,
        )
        workspace_installation.run()
        cleanup.append(workspace_installation)
        return workspace_installation, DeployedWorkflows(ws, install_state, timedelta(minutes=3))

    yield factory

    for pending in cleanup:
        pending.uninstall()


def test_experimental_permissions_migration_for_group_with_same_name(
        installation_ctx,
        ws,
        sql_backend,
        make_cluster_policy,
        make_cluster_policy_permissions,
):
    ws_group, acc_group = installation_ctx.make_ucx_group()
    migrated_group = MigratedGroup.partial_info(ws_group, acc_group)
    cluster_policy = make_cluster_policy()
    make_cluster_policy_permissions(
        object_id=cluster_policy.policy_id,
        permission_level=PermissionLevel.CAN_USE,
        group_name=migrated_group.name_in_workspace,
    )

    schema_a = installation_ctx.make_schema()
    table_a = installation_ctx.make_table(schema_name=schema_a.name)
    installation_ctx.make_grant(migrated_group.name_in_workspace, 'USAGE', schema_info=schema_a)
    installation_ctx.make_grant(migrated_group.name_in_workspace, 'OWN', schema_info=schema_a)
    installation_ctx.make_grant(migrated_group.name_in_workspace, 'SELECT', table_info=table_a)

    installation_ctx.workspace_installation.run()

    installation_ctx.deployed_workflows.run_workflow("migrate-groups-experimental")

    object_permissions = installation_ctx.generic_permissions_support.load_as_dict(
        "cluster-policies", cluster_policy.policy_id
    )
    new_schema_grants = installation_ctx.grants_crawler.for_schema_info(schema_a)

    assert {"USAGE", "OWN"} == new_schema_grants[migrated_group.name_in_account]
    assert object_permissions[migrated_group.name_in_account] == PermissionLevel.CAN_USE


@retried(on=[NotFound, TimeoutError], timeout=timedelta(minutes=3))
def test_job_failure_propagates_correct_error_message_and_logs(ws, sql_backend, installation_ctx):
    installation_ctx.workspace_installation.run()

    with pytest.raises(ManyError) as failure:
        installation_ctx.deployed_workflows.run_workflow("failing")

    assert "This is a test error message." in str(failure.value)
    assert "This task is supposed to fail." in str(failure.value)

    install_folder = installation_ctx.installation.install_folder()
    workflow_run_logs = list(ws.workspace.list(f"{install_folder}/logs"))
    assert len(workflow_run_logs) == 1

    inventory_database = installation_ctx.config.inventory_database
    (records,) = next(sql_backend.fetch(f"SELECT COUNT(*) AS cnt FROM {inventory_database}.logs"))
    assert records == 3


@retried(on=[NotFound, InvalidParameterValue], timeout=timedelta(minutes=3))
def test_job_cluster_policy(ws, installation_ctx):
    installation_ctx.workspace_installation.run()
    user_name = ws.current_user.me().user_name
    cluster_policy = ws.cluster_policies.get(policy_id=installation_ctx.config.policy_id)
    policy_definition = json.loads(cluster_policy.definition)

    assert (cluster_policy.name ==
            f"Unity Catalog Migration ({installation_ctx.config.inventory_database}) ({user_name})")

    spark_version = ws.clusters.select_spark_version(latest=True, long_term_support=True)
    assert policy_definition["spark_version"]["value"] == spark_version
    assert policy_definition["node_type_id"]["value"] == ws.clusters.select_node_type(local_disk=True)
    if ws.config.is_azure:
        assert (
                policy_definition["azure_attributes.availability"]["value"]
                == compute.AzureAvailability.ON_DEMAND_AZURE.value
        )
    if ws.config.is_aws:
        assert policy_definition["aws_attributes.availability"]["value"] == compute.AwsAvailability.ON_DEMAND.value


@retried(on=[NotFound, InvalidParameterValue], timeout=timedelta(minutes=5))
def test_running_real_assessment_job(ws, installation_ctx, make_cluster_policy, make_cluster_policy_permissions):
    ctx = installation_ctx.replace(skip_dashboards=False)
    ws_group_a, _ = ctx.make_ucx_group()

    cluster_policy = make_cluster_policy()
    make_cluster_policy_permissions(
        object_id=cluster_policy.policy_id,
        permission_level=PermissionLevel.CAN_USE,
        group_name=ws_group_a.display_name,
    )
    ctx.__dict__['include_object_permissions'] = [f"cluster-policies:{cluster_policy.policy_id}"]
    ctx.workspace_installation.run()

    ctx.deployed_workflows.run_workflow("assessment")

    after = ctx.generic_permissions_support.load_as_dict("cluster-policies", cluster_policy.policy_id)
    assert after[ws_group_a.display_name] == PermissionLevel.CAN_USE


@retried(on=[NotFound, InvalidParameterValue], timeout=timedelta(minutes=5))
def test_running_real_migrate_groups_job(
        ws, sql_backend, installation_ctx, make_cluster_policy, make_cluster_policy_permissions
):
    ws_group_a, acc_group_a = installation_ctx.make_ucx_group()

    # perhaps we also want to do table grants here (to test acl cluster)
    cluster_policy = make_cluster_policy()
    make_cluster_policy_permissions(
        object_id=cluster_policy.policy_id,
        permission_level=PermissionLevel.CAN_USE,
        group_name=ws_group_a.display_name,
    )

    installation_ctx.__dict__['include_object_permissions'] = [f"cluster-policies:{cluster_policy.policy_id}"]
    installation_ctx.workspace_installation.run()
    installation_ctx.permission_manager.inventorize_permissions()

    installation_ctx.deployed_workflows.run_workflow("migrate-groups")

    found = installation_ctx.generic_permissions_support.load_as_dict("cluster-policies", cluster_policy.policy_id)
    assert found[acc_group_a.display_name] == PermissionLevel.CAN_USE
    assert found[f"{installation_ctx.config.renamed_group_prefix}{ws_group_a.display_name}"] == PermissionLevel.CAN_USE


@retried(on=[NotFound, InvalidParameterValue], timeout=timedelta(minutes=5))
def test_running_real_validate_groups_permissions_job(
        ws, sql_backend, installation_ctx, make_query, make_query_permissions
):
    ws_group_a, _ = installation_ctx.make_ucx_group()

    query = make_query()
    make_query_permissions(
        object_id=query.id,
        permission_level=sql.PermissionLevel.CAN_EDIT,
        group_name=ws_group_a.display_name,
    )

    installation_ctx.__dict__['include_group_names'] = [ws_group_a.display_name]
    installation_ctx.workspace_installation.run()
    installation_ctx.permission_manager.inventorize_permissions()

    # assert the job does not throw any exception
    installation_ctx.deployed_workflows.run_workflow("validate-groups-permissions")


@retried(on=[NotFound], timeout=timedelta(minutes=5))
def test_running_real_validate_groups_permissions_job_fails(
        ws, sql_backend, installation_ctx, make_cluster_policy, make_cluster_policy_permissions
):
    ws_group_a, _ = installation_ctx.make_ucx_group()

    cluster_policy = make_cluster_policy()
    make_cluster_policy_permissions(
        object_id=cluster_policy.policy_id,
        permission_level=PermissionLevel.CAN_USE,
        group_name=ws_group_a.display_name,
    )

    installation_ctx.__dict__['include_group_names'] = [ws_group_a.display_name]
    installation_ctx.workspace_installation.run()
    installation_ctx.permission_manager.inventorize_permissions()

    # remove permission so the validation fails
    ws.permissions.set(
        request_object_type="cluster-policies", request_object_id=cluster_policy.policy_id, access_control_list=[]
    )

    with pytest.raises(ManyError):
        installation_ctx.deployed_workflows.run_workflow("validate-groups-permissions")


@retried(on=[NotFound, InvalidParameterValue], timeout=timedelta(minutes=5))
def test_running_real_remove_backup_groups_job(ws, sql_backend, installation_ctx):
    ws_group_a, _ = installation_ctx.make_ucx_group()

    installation_ctx.__dict__['include_group_names'] = [ws_group_a.display_name]
    installation_ctx.workspace_installation.run()

    installation_ctx.group_manager.snapshot()
    installation_ctx.group_manager.rename_groups()
    installation_ctx.group_manager.reflect_account_groups_on_workspace()

    installation_ctx.deployed_workflows.run_workflow("remove-workspace-local-backup-groups")

    with pytest.raises(NotFound):
        ws.groups.get(ws_group_a.id)


@retried(on=[NotFound, InvalidParameterValue], timeout=timedelta(minutes=3))
def test_repair_run_workflow_job(installation_ctx, mocker):
    mocker.patch("webbrowser.open")
    installation_ctx.workspace_installation.run()
    with pytest.raises(ManyError):
        installation_ctx.deployed_workflows.run_workflow("failing")

    installation_ctx.deployed_workflows.repair_run("failing")

    assert installation_ctx.deployed_workflows.validate_step("failing")


@retried(on=[NotFound], timeout=timedelta(minutes=2))
def test_uninstallation(ws, sql_backend, new_installation):
    install, _ = new_installation()
    installation = Installation(ws, product=os.path.basename(install.folder), install_folder=install.folder)
    state = InstallState.from_installation(installation)
    assessment_job_id = state.jobs["assessment"]
    install.uninstall()
    with pytest.raises(NotFound):
        ws.workspace.get_status(install.folder)
    with pytest.raises(InvalidParameterValue):
        ws.jobs.get(job_id=assessment_job_id)
    with pytest.raises(NotFound):
        sql_backend.execute(f"show tables from hive_metastore.{install.config.inventory_database}")


def test_fresh_global_installation(ws, new_installation):
    product_info = ProductInfo.for_testing(WorkspaceConfig)
    global_installation, _ = new_installation(
        product_info=product_info,
        installation=Installation.assume_global(ws, product_info.product_name()),
    )
    assert global_installation.folder == f"/Applications/{product_info.product_name()}"
    global_installation.uninstall()


def test_fresh_user_installation(ws, new_installation):
    product_info = ProductInfo.for_testing(WorkspaceConfig)
    user_installation, _ = new_installation(
        product_info=product_info,
        installation=Installation.assume_user_home(ws, product_info.product_name()),
    )
    assert user_installation.folder == f"/Users/{ws.current_user.me().user_name}/.{product_info.product_name()}"
    user_installation.uninstall()


def test_global_installation_on_existing_global_install(ws, new_installation):
    product_info = ProductInfo.for_testing(WorkspaceConfig)
    existing_global_installation, _ = new_installation(
        product_info=product_info,
        installation=Installation.assume_global(ws, product_info.product_name()),
    )
    assert existing_global_installation.folder == f"/Applications/{product_info.product_name()}"
    reinstall_global, _ = new_installation(
        product_info=product_info,
        installation=Installation.assume_global(ws, product_info.product_name()),
        extend_prompts={
            r".*Do you want to update the existing installation?.*": 'yes',
        },
    )
    assert reinstall_global.folder == f"/Applications/{product_info.product_name()}"
    reinstall_global.uninstall()


def test_user_installation_on_existing_global_install(ws, new_installation, make_random):
    # existing install at global level
    product_info = ProductInfo.for_testing(WorkspaceConfig)
    existing_global_installation, _ = new_installation(
        product_info=product_info,
        installation=Installation.assume_global(ws, product_info.product_name()),
    )

    # warning to be thrown by installer if override environment variable present but no confirmation
    with pytest.raises(RuntimeWarning, match="UCX is already installed, but no confirmation"):
        new_installation(
            product_info=product_info,
            installation=Installation.assume_global(ws, product_info.product_name()),
            environ={'UCX_FORCE_INSTALL': 'user'},
            extend_prompts={
                r".*UCX is already installed on this workspace.*": 'no',
                r".*Do you want to update the existing installation?.*": 'yes',
            },
        )

    # successful override with confirmation
    reinstall_user_force, _ = new_installation(
        product_info=product_info,
        installation=Installation.assume_global(ws, product_info.product_name()),
        environ={'UCX_FORCE_INSTALL': 'user'},
        extend_prompts={
            r".*UCX is already installed on this workspace.*": 'yes',
            r".*Do you want to update the existing installation?.*": 'yes',
        },
        inventory_schema_name=f"ucx_S{make_random(4)}_reinstall",
    )
    assert reinstall_user_force.folder == f"/Users/{ws.current_user.me().user_name}/.{product_info.product_name()}"
    reinstall_user_force.uninstall()
    existing_global_installation.uninstall()


def test_global_installation_on_existing_user_install(ws, new_installation):
    # existing installation at user level
    product_info = ProductInfo.for_testing(WorkspaceConfig)
    existing_user_installation, _ = new_installation(
        product_info=product_info, installation=Installation.assume_user_home(ws, product_info.product_name())
    )
    assert (
            existing_user_installation.folder == f"/Users/{ws.current_user.me().user_name}/.{product_info.product_name()}"
    )

    # warning to be thrown by installer if override environment variable present but no confirmation
    with pytest.raises(RuntimeWarning, match="UCX is already installed, but no confirmation"):
        new_installation(
            product_info=product_info,
            installation=Installation.assume_user_home(ws, product_info.product_name()),
            environ={'UCX_FORCE_INSTALL': 'global'},
            extend_prompts={
                r".*UCX is already installed on this workspace.*": 'no',
                r".*Do you want to update the existing installation?.*": 'yes',
            },
        )

    # not implemented error with confirmation
    with pytest.raises(databricks.sdk.errors.NotImplemented, match="Migration needed. Not implemented yet."):
        new_installation(
            product_info=product_info,
            installation=Installation.assume_user_home(ws, product_info.product_name()),
            environ={'UCX_FORCE_INSTALL': 'global'},
            extend_prompts={
                r".*UCX is already installed on this workspace.*": 'yes',
                r".*Do you want to update the existing installation?.*": 'yes',
            },
        )
    existing_user_installation.uninstall()


def test_check_inventory_database_exists(ws, new_installation, make_random):
    product_info = ProductInfo.for_testing(WorkspaceConfig)
    install, _ = new_installation(
        product_info=product_info,
        installation=Installation.assume_global(ws, product_info.product_name()),
        inventory_schema_name=f"ucx_S{make_random(4)}_exists",
    )
    inventory_database = install.config.inventory_database

    with pytest.raises(
            AlreadyExists, match=f"Inventory database '{inventory_database}' already exists in another installation"
    ):
        new_installation(
            product_info=product_info,
            installation=Installation.assume_global(ws, product_info.product_name()),
            environ={'UCX_FORCE_INSTALL': 'user'},
            extend_prompts={
                r".*UCX is already installed on this workspace.*": 'yes',
                r".*Do you want to update the existing installation?.*": 'yes',
            },
            inventory_schema_name=inventory_database,
        )


@retried(on=[NotFound], timeout=timedelta(minutes=5))
def test_table_migration_job(
        ws,
        installation_ctx,
        make_catalog,
        make_schema,
        make_table,
        env_or_skip,
        make_random,
        make_dbfs_data_copy,
        sql_backend,
):
    # skip this test if not in nightly test job or debug mode
    if os.path.basename(sys.argv[0]) not in {"_jb_pytest_runner.py", "testlauncher.py"}:
        env_or_skip("TEST_NIGHTLY")
    # create external and managed tables to be migrated
    schema = make_schema(catalog_name="hive_metastore", name=f"migrate_{make_random(5).lower()}")
    tables: dict[str, TableInfo] = {"src_managed_table": make_table(schema_name=schema.name)}
    tables["src_external_table"] = make_table(
        schema_name=schema.name, external_csv=f'dbfs:/mnt/{env_or_skip("TEST_MOUNT_NAME")}/a/b/c'
    )
    # create destination catalog and schema
    src_view1_text = f"SELECT * FROM {tables['src_managed_table'].full_name}"
    tables["src_view1"] = make_table(
        catalog_name=schema.catalog_name,
        schema_name=schema.name,
        ctas=src_view1_text,
        view=True,
    )
    src_view2_text = f"SELECT * FROM {tables['src_view1'].full_name}"
    tables["src_view2"] = make_table(
        catalog_name=schema.catalog_name,
        schema_name=schema.name,
        ctas=src_view2_text,
        view=True,
    )
    dst_catalog = make_catalog()
    make_schema(catalog_name=dst_catalog.name, name=schema.name)
    ctx = installation_ctx.replace(
        extend_prompts={
            r"Parallelism for migrating.*": "1000",
            r"Min workers for auto-scale.*": "2",
            r"Max workers for auto-scale.*": "20",
            r"Instance pool id to be set.*": env_or_skip("TEST_INSTANCE_POOL_ID"),
            r".*Do you want to update the existing installation?.*": 'yes',
        }
    )

    ctx.workspace_installation.run()

    rules = [Rule.from_src_dst(table, schema) for _, table in tables.items()]
    ctx.with_table_mapping_rules(rules)
    if ws.config.is_azure:
        ctx.with_azure_storage_permissions([
            StoragePermissionMapping(
                'abfss://things@labsazurethings.dfs.core.windows.net',
                'dummy_application_id',
                'principal_1',
                'WRITE_FILES',
                'Application',
                'directory_id_ss1',
            )
        ])
    if ws.config.is_aws:
        ctx.installation.save(
            [
                AWSRoleAction(
                    'arn:aws:iam::184784626197:instance-profile/labs-data-access',
                    's3',
                    'WRITE_FILES',
                    's3://labs-things/*',
                )
            ],
            filename='aws_instance_profile_info.csv',
        )

    ctx.deployed_workflows.run_workflow("migrate-tables")
    # assert the workflow is successful
    assert ctx.deployed_workflows.validate_step("migrate-tables")
    # assert the tables are migrated
    try:
        assert ws.tables.get(f"{dst_catalog.name}.{schema.name}.{tables['src_managed_table'].name}").name
        assert ws.tables.get(f"{dst_catalog.name}.{schema.name}.{tables['src_external_table'].name}").name
        assert ws.tables.get(f"{dst_catalog.name}.{schema.name}.{tables['src_view1'].name}").name
        assert ws.tables.get(f"{dst_catalog.name}.{schema.name}.{tables['src_view2'].name}").name
    except NotFound:
        assert False, f"Table or view not found in {dst_catalog.name}.{schema.name}"
    # assert the cluster is configured correctly
    for job_cluster in ws.jobs.get(
            ctx.installation.load(RawState).resources["jobs"]["migrate-tables"]
    ).settings.job_clusters:
        assert job_cluster.new_cluster.autoscale.min_workers == 2
        assert job_cluster.new_cluster.autoscale.max_workers == 20
        assert job_cluster.new_cluster.spark_conf["spark.sql.sources.parallelPartitionDiscovery.parallelism"] == "1000"


@retried(on=[NotFound], timeout=timedelta(minutes=5))
def test_table_migration_job_cluster_override(  # pylint: disable=too-many-locals
        ws,
        installation_ctx,
        make_catalog,
        env_or_skip,
        make_random,
        make_dbfs_data_copy,
        sql_backend,
):
    # create external and managed tables to be migrated
    schema = installation_ctx.make_schema(catalog_name="hive_metastore", name=f"migrate_{make_random(5).lower()}")
    src_managed_table = installation_ctx.make_table(schema_name=schema.name)
    existing_mounted_location = f'dbfs:/mnt/{env_or_skip("TEST_MOUNT_NAME")}/a/b/c'
    new_mounted_location = f'dbfs:/mnt/{env_or_skip("TEST_MOUNT_NAME")}/a/b/{make_random(4)}'
    make_dbfs_data_copy(src_path=existing_mounted_location, dst_path=new_mounted_location)
    src_external_table = installation_ctx.make_table(schema_name=schema.name, external_csv=new_mounted_location)
    # create destination catalog and schema
    dst_catalog = make_catalog()
    installation_ctx.make_schema(catalog_name=dst_catalog.name, name=schema.name)

    product_info = ProductInfo.from_class(WorkspaceConfig)
    ctx = installation_ctx.replace(
        extend_prompts={
            r".*Do you want to update the existing installation?.*": 'yes',
        },
    )

    ctx.workspace_installation.run()
    migrate_rules = [
        Rule.from_src_dst(src_managed_table, schema),
        Rule.from_src_dst(src_external_table, schema),
    ]
    ctx.with_table_mapping_rules(migrate_rules)
    if ws.config.is_azure:
        ctx.with_azure_storage_permissions([
                StoragePermissionMapping(
                    'abfss://things@labsazurethings.dfs.core.windows.net',
                    'dummy_application_id',
                    'principal_1',
                    'WRITE_FILES',
                    'Application',
                    'directory_id_ss1',
                )
            ])
    if ws.config.is_aws:
        ctx.installation.save(
            [
                AWSRoleAction(
                    'arn:aws:iam::184784626197:instance-profile/labs-data-access',
                    's3',
                    'WRITE_FILES',
                    's3://labs-things/*',
                )
            ],
            filename='aws_instance_profile_info.csv',
        )
    ctx.deployed_workflows.run_workflow("migrate-tables")
    # assert the workflow is successful
    assert ctx.deployed_workflows.validate_step("migrate-tables")
    # assert the tables are migrated
    try:
        assert ws.tables.get(f"{dst_catalog.name}.{schema.name}.{src_managed_table.name}").name
        assert ws.tables.get(f"{dst_catalog.name}.{schema.name}.{src_external_table.name}").name
    except NotFound:
        assert (
            False
        ), f"{src_managed_table.name} and {src_external_table.name} not found in {dst_catalog.name}.{schema.name}"
    # assert the cluster is configured correctly on the migrate tables tasks
    install_state = ctx.installation.load(RawState)
    job_id = install_state.resources["jobs"]["migrate-tables"]
    assert all(
        task.existing_cluster_id == env_or_skip("TEST_USER_ISOLATION_CLUSTER_ID")
        for task in ws.jobs.get(job_id).settings.tasks
        if task.task_key != "parse_logs"
    )


def test_compare_remote_local_install_versions(ws, installation_ctx):
    installation_ctx.workspace_installation.run()
    with pytest.raises(
            RuntimeWarning,
            match="UCX workspace remote and local install versions are same and no override is requested. Exiting...",
    ):
        installation_ctx.workspace_installation.run()
    ctx = installation_ctx.replace(extend_prompts={
            r".*Do you want to update the existing installation?.*": 'yes',
        },
    )
    ctx.workspace_installation.run()

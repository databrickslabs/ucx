import dataclasses
import json
import logging
from datetime import timedelta
from typing import NoReturn

import pytest

import databricks
from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.installer import InstallState
from databricks.labs.blueprint.parallel import ManyError
from databricks.labs.blueprint.tui import MockPrompts
from databricks.labs.blueprint.wheels import ProductInfo
from databricks.sdk import AccountClient, WorkspaceClient
from databricks.labs.lsql.backends import StatementExecutionBackend
from databricks.sdk.errors import (
    AlreadyExists,
    InvalidParameterValue,
    NotFound,
    ResourceConflict,
)
from databricks.sdk.retries import retried
from databricks.sdk.service import compute
from databricks.sdk.service.iam import PermissionLevel

from databricks.labs.ucx.__about__ import __version__
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.install import WorkspaceInstaller
from databricks.labs.ucx.workspace_access.groups import MigratedGroup

from ..conftest import MockInstallationContext

logger = logging.getLogger(__name__)


@pytest.fixture
def new_installation(ws, env_or_skip, make_random):
    # this fixture is used by test_user_installation_on_existing_global_install and
    # test_global_installation_on_existing_user_install
    cleanup = []

    def factory(
        installation: Installation | None = None,
        product_info: ProductInfo | None = None,
        environ: dict[str, str] | None = None,
        extend_prompts: dict[str, str] | None = None,
        inventory_schema_name: str | None = None,
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

        if not installation:
            installation = Installation(ws, product_info.product_name())
        installer = WorkspaceInstaller(ws, environ).replace(
            installation=installation,
            product_info=product_info,
            prompts=prompts,
        )
        workspace_config = installer.configure()
        installation = product_info.current_installation(ws)
        installation.save(workspace_config)
        cleanup.append(installation)
        return installation

    yield factory

    for pending in cleanup:
        pending.remove()


@retried(on=[NotFound, ResourceConflict], timeout=timedelta(minutes=10))
def test_experimental_permissions_migration_for_group_with_same_name(
    installation_ctx, make_cluster_policy, make_cluster_policy_permissions
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

    if {"USAGE", "OWN"} != new_schema_grants[migrated_group.name_in_account] or object_permissions[
        migrated_group.name_in_account
    ] != PermissionLevel.CAN_USE:
        installation_ctx.deployed_workflows.relay_logs("migrate-groups-experimental")
    assert {"USAGE", "OWN"} == new_schema_grants[
        migrated_group.name_in_account
    ], "Incorrect schema grants for migrated group"
    assert (
        object_permissions[migrated_group.name_in_account] == PermissionLevel.CAN_USE
    ), "Incorrect permissions for migrated group"


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

    inventory_database = installation_ctx.inventory_database
    (records,) = next(sql_backend.fetch(f"SELECT COUNT(*) AS cnt FROM {inventory_database}.logs"))
    assert records == 3


@retried(on=[NotFound, InvalidParameterValue], timeout=timedelta(minutes=3))
def test_job_cluster_policy(ws, installation_ctx):
    installation_ctx.workspace_installation.run()
    user_name = ws.current_user.me().user_name
    cluster_policy = ws.cluster_policies.get(policy_id=installation_ctx.config.policy_id)
    policy_definition = json.loads(cluster_policy.definition)

    assert cluster_policy.name == f"Unity Catalog Migration ({installation_ctx.inventory_database}) ({user_name})"

    spark_version = ws.clusters.select_spark_version(latest=True)
    assert policy_definition["spark_version"]["value"] == spark_version
    assert policy_definition["node_type_id"]["value"] == ws.clusters.select_node_type(local_disk=True, min_memory_gb=16)
    if ws.config.is_azure:
        assert (
            policy_definition["azure_attributes.availability"]["value"]
            == compute.AzureAvailability.ON_DEMAND_AZURE.value
        )
    if ws.config.is_aws:
        assert policy_definition["aws_attributes.availability"]["value"] == compute.AwsAvailability.ON_DEMAND.value


@retried(on=[NotFound, InvalidParameterValue])
def test_running_real_remove_backup_groups_job(ws: WorkspaceClient, installation_ctx: MockInstallationContext) -> None:
    ws_group_a, _ = installation_ctx.make_ucx_group(wait_for_provisioning=True)

    installation_ctx.__dict__['include_group_names'] = [ws_group_a.display_name]
    installation_ctx.workspace_installation.run()

    installation_ctx.group_manager.snapshot()
    installation_ctx.group_manager.rename_groups()
    installation_ctx.group_manager.reflect_account_groups_on_workspace()

    installation_ctx.deployed_workflows.run_workflow("remove-workspace-local-backup-groups")

    # Group deletion is eventually consistent. Although the group manager tries to wait for convergence, parts of the
    # API internals have a 60s timeout. As such we should wait at least that long before concluding deletion has not
    # happened.
    # Note: If you are adjusting this, also look at: test_running_real_remove_backup_groups_job
    @retried(on=[KeyError], timeout=timedelta(seconds=90))
    def get_group(group_id: str) -> NoReturn:
        _ = ws.groups.get(group_id)
        raise KeyError(f"Group is not deleted: {group_id}")

    with pytest.raises(NotFound, match=f"Group with id {ws_group_a.id} not found."):
        get_group(ws_group_a.id)


@retried(on=[NotFound, InvalidParameterValue], timeout=timedelta(minutes=5))
def test_repair_run_workflow_job(installation_ctx, mocker):
    mocker.patch("webbrowser.open")
    installation_ctx.workspace_installation.run()
    with pytest.raises(ManyError):
        installation_ctx.deployed_workflows.run_workflow("failing")

    installation_ctx.deployed_workflows.repair_run("failing")

    assert installation_ctx.deployed_workflows.validate_step("failing")


def test_installation_deletes_redash_dashboard_when_upgrading_to_lakeview(ws, installation_ctx, make_dashboard):
    """The installation should handle upgrading dashboards from redash."""

    @retried(on=[ValueError], timeout=timedelta(minutes=2))
    def check_dashboard_is_archived(dashboard_id: str):
        dashboard = ws.dashboards.get(dashboard_id)
        if not dashboard.is_archived:
            raise ValueError("Dashboard is not archived")

    dashboard = make_dashboard()
    installation_ctx.install_state.dashboards["assessment_main"] = dashboard.id
    installation_ctx.workspace_installation.run()
    try:
        check_dashboard_is_archived(dashboard.id)
    except TimeoutError:
        assert False, "Lakeview dashboard was not deleted"
    assert True, "Lakeview dashboard was deleted"


def test_installation_when_dashboard_is_trashed(ws, installation_ctx):
    """A dashboard might be trashed (manually), the upgrade should handle this."""
    installation_ctx.workspace_installation.run()
    dashboard_id = list(installation_ctx.install_state.dashboards.values())[0]
    ws.lakeview.trash(dashboard_id)
    try:
        installation_ctx.workspace_installation.run()
    except NotFound:
        assert False, "Installation failed when dashboard was trashed"
    assert True, "Installation succeeded when dashboard was trashed"


@pytest.mark.parametrize("dashboard_id", ["01ef4d7b294112968fa07ffae17dd55f", "invalid-dashboard-id", ""])
def test_installation_when_dashboard_id_is_invalid(ws, installation_ctx, dashboard_id):
    """A dashboard reference might be invalid (after manual changes), the upgrade should handle this."""
    dashboard_key = "assessment_main"
    installation_ctx.install_state.dashboards[dashboard_key] = dashboard_id
    installation_ctx.workspace_installation.run()
    new_dashboard_id = installation_ctx.install_state.dashboards[dashboard_key]
    assert dashboard_id != new_dashboard_id, "Dashboard id is not updated"


def test_installation_stores_install_state_keys(ws, installation_ctx):
    """The installation should store the keys in the installation state."""
    expected_keys = "jobs", "dashboards"
    installation_ctx.workspace_installation.run()
    # Refresh the installation state, the installation context uses `@cached_property`
    install_state = InstallState.from_installation(installation_ctx.installation)
    for key in expected_keys:
        assert hasattr(install_state, key), f"Missing key in install state: {key}"
        assert getattr(install_state, key), f"Installation state is empty: {key}"


@retried(on=[NotFound], timeout=timedelta(minutes=2))
def test_uninstallation(ws, sql_backend, installation_ctx):
    installation_ctx.workspace_installation.run()
    assessment_job_id = installation_ctx.install_state.jobs["assessment"]
    installation_ctx.workspace_installation.uninstall()
    with pytest.raises(NotFound):
        ws.workspace.get_status(installation_ctx.workspace_installation.folder)
    with pytest.raises(NotFound):
        ws.jobs.get(job_id=assessment_job_id)
    with pytest.raises(NotFound):
        sql_backend.execute(f"show tables from hive_metastore.{installation_ctx.inventory_database}")


def test_uninstallation_after_warehouse_is_deleted(ws, installation_ctx):
    """A warehouse might be deleted (manually), the uninstallation should reset the warehouse."""
    non_existing_warehouse_id = "00aa00aa00a00a00"
    config = dataclasses.replace(installation_ctx.config, warehouse_id=non_existing_warehouse_id)
    sql_backend = StatementExecutionBackend(ws, config.warehouse_id)
    installation_ctx = installation_ctx.replace(sql_backend=sql_backend, config=config)

    installation_ctx.workspace_installation.uninstall()
    with pytest.raises(NotFound):
        sql_backend.execute(f"show tables from hive_metastore.{installation_ctx.inventory_database}")


def test_fresh_global_installation(ws, installation_ctx):
    installation_ctx.installation = Installation.assume_global(ws, installation_ctx.product_info.product_name())
    installation_ctx.installation.save(installation_ctx.config)
    assert (
        installation_ctx.workspace_installation.folder
        == f"/Applications/{installation_ctx.product_info.product_name()}"
    )


def test_fresh_user_installation(ws, installation_ctx):
    installation_ctx.installation.save(installation_ctx.config)
    assert (
        installation_ctx.workspace_installation.folder
        == f"/Users/{ws.current_user.me().user_name}/.{installation_ctx.product_info.product_name()}"
    )


def test_global_installation_on_existing_global_install(ws, installation_ctx):
    installation_ctx.installation = Installation.assume_global(ws, installation_ctx.product_info.product_name())
    installation_ctx.installation.save(installation_ctx.config)
    assert (
        installation_ctx.workspace_installation.folder
        == f"/Applications/{installation_ctx.product_info.product_name()}"
    )
    installation_ctx.replace(
        extend_prompts={
            r".*Do you want to update the existing installation?.*": 'yes',
        },
    )
    installation_ctx.__dict__.pop("workspace_installer")
    installation_ctx.__dict__.pop("prompts")
    installation_ctx.workspace_installer.configure()


def test_user_installation_on_existing_global_install(ws, new_installation, make_random):
    # existing install at global level
    product_info = ProductInfo.for_testing(WorkspaceConfig)
    new_installation(
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
    reinstall_user_force = new_installation(
        product_info=product_info,
        installation=Installation.assume_global(ws, product_info.product_name()),
        environ={'UCX_FORCE_INSTALL': 'user'},
        extend_prompts={
            r".*UCX is already installed on this workspace.*": 'yes',
            r".*Do you want to update the existing installation?.*": 'yes',
        },
        inventory_schema_name=f"ucx_S{make_random(4)}_reinstall",
    )
    assert (
        reinstall_user_force.install_folder()
        == f"/Users/{ws.current_user.me().user_name}/.{product_info.product_name()}"
    )


def test_global_installation_on_existing_user_install(ws, new_installation):
    # existing installation at user level
    product_info = ProductInfo.for_testing(WorkspaceConfig)
    existing_user_installation = new_installation(
        product_info=product_info, installation=Installation.assume_user_home(ws, product_info.product_name())
    )
    assert (
        existing_user_installation.install_folder()
        == f"/Users/{ws.current_user.me().user_name}/.{product_info.product_name()}"
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


def test_check_inventory_database_exists(ws, installation_ctx):
    installation_ctx.installation = Installation.assume_global(ws, installation_ctx.product_info.product_name())
    installation_ctx.installation.save(installation_ctx.config)
    inventory_database = installation_ctx.inventory_database

    with pytest.raises(
        AlreadyExists, match=f"Inventory database '{inventory_database}' already exists in another installation"
    ):
        installation_ctx.installation = Installation.assume_user_home(ws, installation_ctx.product_info.product_name())
        installation_ctx.__dict__.pop("workspace_installer")
        installation_ctx.__dict__.pop("prompts")
        installation_ctx.replace(
            extend_prompts={
                r".*UCX is already installed on this workspace.*": 'yes',
                r".*Do you want to update the existing installation?.*": 'yes',
            },
        )
        installation_ctx.workspace_installer.configure()


def test_compare_remote_local_install_versions(ws, installation_ctx):
    installation_ctx.workspace_installation.run()
    with pytest.raises(
        RuntimeWarning,
        match="UCX workspace remote and local install versions are same and no override is requested. Exiting...",
    ):
        installation_ctx.workspace_installer.configure()

    installation_ctx.replace(
        extend_prompts={
            r".*Do you want to update the existing installation?.*": 'yes',
        },
    )
    installation_ctx.__dict__.pop("workspace_installer")
    installation_ctx.__dict__.pop("prompts")
    installation_ctx.workspace_installer.configure()


def test_new_collection(ws, sql_backend, installation_ctx, env_or_skip):
    host = ws.config.environment.deployment_url("accounts")
    acc_client = AccountClient(
        host=host, account_id=env_or_skip("DATABRICKS_ACCOUNT_ID"), product='ucx', product_version=__version__
    )
    workspace = acc_client.workspaces.get(ws.get_workspace_id())
    installation_ctx.with_workspace_info([workspace])
    installation_ctx.workspace_installer.run()
    workspace_id = installation_ctx.workspace_installer.workspace_client.get_workspace_id()
    acc_installer = installation_ctx.account_installer
    prompts = MockPrompts(
        {
            r"Do you want to join the current.*": "yes",
            r"Please provide the Databricks account id.*": env_or_skip("DATABRICKS_ACCOUNT_ID"),
            r"Please select a workspace, the current installation.*": 0,
        }
    )
    acc_installer.replace(
        prompts=prompts,
        product_info=installation_ctx.product_info,
    )
    acc_installer.join_collection([workspace_id], True)
    config = installation_ctx.installation.load(WorkspaceConfig)
    workspace_id = installation_ctx.workspace_installer.workspace_client.get_workspace_id()
    assert config.installed_workspace_ids == [workspace_id]


def test_installation_with_dependency_upload(ws, installation_ctx, mocker):
    config = dataclasses.replace(installation_ctx.config, upload_dependencies=True)
    installation_ctx = installation_ctx.replace(config=config)
    mocker.patch("webbrowser.open")
    installation_ctx.workspace_installation.run()
    with pytest.raises(ManyError):
        installation_ctx.deployed_workflows.run_workflow("failing")

    installation_ctx.deployed_workflows.repair_run("failing")
    assert installation_ctx.deployed_workflows.validate_step("failing")

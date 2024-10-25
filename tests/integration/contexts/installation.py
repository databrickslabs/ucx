import functools
import logging
import os
from dataclasses import replace
from functools import cached_property
from collections.abc import Callable

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.parallel import Threads
from databricks.labs.blueprint.tui import MockPrompts

from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.framework.tasks import Task
from databricks.labs.ucx.install import AccountInstaller, WorkspaceInstaller, WorkspaceInstallation
from databricks.labs.ucx.installer.workflows import WorkflowsDeployment
from databricks.labs.ucx.progress.install import ProgressTrackingInstallation
from databricks.labs.ucx.runtime import Workflows
from tests.integration.contexts.runtime import MockRuntimeContext

logger = logging.getLogger(__name__)


class MockInstallationContext(MockRuntimeContext):
    def __init__(self, make_acc_group_fixture, make_user_fixture, watchdog_purge_suffix, *args):
        super().__init__(*args)
        self._make_acc_group = make_acc_group_fixture
        self._make_user = make_user_fixture
        self._watchdog_purge_suffix = watchdog_purge_suffix

    def make_ucx_group(self, workspace_group_name=None, account_group_name=None, wait_for_provisioning=False):
        if not workspace_group_name:
            workspace_group_name = f"ucx-{self._make_random(4)}-{self._watchdog_purge_suffix}"  # noqa: F405
        if not account_group_name:
            account_group_name = workspace_group_name
        user = self._make_user()
        members = [user.id]
        ws_group = self.make_group(
            display_name=workspace_group_name,
            members=members,
            entitlements=["allow-cluster-create"],
            wait_for_provisioning=wait_for_provisioning,
        )
        acc_group = self._make_acc_group(
            display_name=account_group_name, members=members, wait_for_provisioning=wait_for_provisioning
        )
        return ws_group, acc_group

    @cached_property
    def running_clusters(self) -> tuple[str, str, str]:
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
    def installation(self) -> Installation:
        return Installation(self.workspace_client, self.product_info.product_name())

    @cached_property
    def account_installer(self) -> AccountInstaller:
        return AccountInstaller(self.account_client)

    @cached_property
    def environ(self) -> dict[str, str]:
        return {**os.environ}

    @cached_property
    def workspace_installer(self) -> WorkspaceInstaller:
        return WorkspaceInstaller(
            self.workspace_client,
            self.environ,
        ).replace(prompts=self.prompts, installation=self.installation, product_info=self.product_info)

    @cached_property
    def config_transform(self) -> Callable[[WorkspaceConfig], WorkspaceConfig]:
        return lambda wc: wc

    @cached_property
    def include_object_permissions(self) -> None:
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
            include_job_ids=self.created_jobs,
            include_dashboard_ids=self.created_dashboards,
            include_object_permissions=self.include_object_permissions,
            warehouse_id=self._env_or_skip("TEST_DEFAULT_WAREHOUSE_ID"),
            ucx_catalog=self.ucx_catalog,
        )
        workspace_config = self.config_transform(workspace_config)
        self.installation.save(workspace_config)
        return workspace_config

    @cached_property
    def tasks(self) -> list[Task]:
        return Workflows.all().tasks()

    @cached_property
    def workflows_deployment(self) -> WorkflowsDeployment:
        return WorkflowsDeployment(
            self.config,
            self.installation,
            self.install_state,
            self.workspace_client,
            self.product_info.wheels(self.workspace_client),
            self.product_info,
            self.tasks,
        )

    @cached_property
    def workspace_installation(self) -> WorkspaceInstallation:
        return WorkspaceInstallation(
            self.config,
            self.installation,
            self.install_state,
            self.sql_backend,
            self.workspace_client,
            self.workflows_deployment,
            self.prompts,
            self.product_info,
        )

    @cached_property
    def progress_tracking_installation(self) -> ProgressTrackingInstallation:
        return ProgressTrackingInstallation(self.sql_backend, self.ucx_catalog)

    @cached_property
    def extend_prompts(self) -> dict[str, str]:
        return {}

    @cached_property
    def renamed_group_prefix(self) -> str:
        return f"rename-{self.product_info.product_name()}-"

    @cached_property
    def prompts(self) -> MockPrompts:
        return MockPrompts(
            {
                r'Open job overview in your browser.*': 'no',
                r'Do you want to uninstall ucx.*': 'yes',
                r'Do you want to delete the inventory database.*': 'yes',
                r".*PRO or SERVERLESS SQL warehouse.*": "1",
                r"Choose how to map the workspace groups.*": "1",
                r".*Inventory Database.*": self.inventory_database,
                r".*Backup prefix*": self.renamed_group_prefix,
                r"If hive_metastore contains managed table with external.*": "1",
                r".*": "",
            }
            | (self.extend_prompts or {})
        )

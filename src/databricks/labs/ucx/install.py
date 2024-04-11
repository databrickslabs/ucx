import dataclasses
import functools
import logging
import os
import re
import time
import webbrowser
from collections.abc import Callable
from datetime import timedelta
from typing import Any

import databricks.sdk.errors
from databricks.labs.blueprint.entrypoint import get_logger
from databricks.labs.blueprint.installation import Installation, SerdeError
from databricks.labs.blueprint.installer import InstallState
from databricks.labs.blueprint.parallel import ManyError, Threads
from databricks.labs.blueprint.tui import Prompts
from databricks.labs.blueprint.upgrades import Upgrades
from databricks.labs.blueprint.wheels import (
    ProductInfo,
    Version,
    WheelsV2,
    find_project_root,
)
from databricks.labs.lsql.backends import SqlBackend, StatementExecutionBackend
from databricks.labs.lsql.deployment import SchemaDeployer
from databricks.sdk import WorkspaceClient, AccountClient
from databricks.sdk.errors import AlreadyExists, BadRequest, InvalidParameterValue, NotFound, PermissionDenied
from databricks.sdk.service.provisioning import Workspace
from databricks.sdk.service.sql import (
    CreateWarehouseRequestWarehouseType,
    EndpointInfoWarehouseType,
    SpotInstancePolicy,
)

from databricks.labs.ucx.__about__ import __version__
from databricks.labs.ucx.assessment.azure import AzureServicePrincipalInfo
from databricks.labs.ucx.assessment.clusters import ClusterInfo, PolicyInfo
from databricks.labs.ucx.assessment.init_scripts import GlobalInitScriptInfo
from databricks.labs.ucx.assessment.jobs import JobInfo, SubmitRunInfo
from databricks.labs.ucx.assessment.pipelines import PipelineInfo
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.contexts.cli_command import AccountContext
from databricks.labs.ucx.framework.dashboards import DashboardFromFiles
from databricks.labs.ucx.framework.tasks import Task
from databricks.labs.ucx.hive_metastore.grants import Grant
from databricks.labs.ucx.hive_metastore.locations import ExternalLocation, Mount
from databricks.labs.ucx.hive_metastore.table_migrate import MigrationStatus
from databricks.labs.ucx.hive_metastore.table_size import TableSize
from databricks.labs.ucx.hive_metastore.tables import Table, TableError
from databricks.labs.ucx.hive_metastore.udfs import Udf
from databricks.labs.ucx.installer.hms_lineage import HiveMetastoreLineageEnabler
from databricks.labs.ucx.installer.logs import LogRecord
from databricks.labs.ucx.installer.mixins import InstallationMixin
from databricks.labs.ucx.installer.policy import ClusterPolicyInstaller
from databricks.labs.ucx.installer.workflows import WorkflowsDeployment
from databricks.labs.ucx.runtime import Workflows
from databricks.labs.ucx.workspace_access.base import Permissions
from databricks.labs.ucx.workspace_access.generic import WorkspaceObjectInfo
from databricks.labs.ucx.workspace_access.groups import ConfigureGroups, MigratedGroup

TAG_STEP = "step"
WAREHOUSE_PREFIX = "Unity Catalog Migration"
NUM_USER_ATTEMPTS = 10  # number of attempts user gets at answering a question

logger = logging.getLogger(__name__)


def deploy_schema(sql_backend: SqlBackend, inventory_schema: str):
    # we need to import it like this because we expect a module instance
    from databricks.labs import ucx  # pylint: disable=import-outside-toplevel

    logger.info("Creating ucx schemas...")
    deployer = SchemaDeployer(sql_backend, inventory_schema, ucx)
    deployer.deploy_schema()
    table = functools.partial(deployer.deploy_table)
    Threads.strict(
        "deploy tables",
        [
            functools.partial(table, "azure_service_principals", AzureServicePrincipalInfo),
            functools.partial(table, "clusters", ClusterInfo),
            functools.partial(table, "global_init_scripts", GlobalInitScriptInfo),
            functools.partial(table, "jobs", JobInfo),
            functools.partial(table, "pipelines", PipelineInfo),
            functools.partial(table, "external_locations", ExternalLocation),
            functools.partial(table, "mounts", Mount),
            functools.partial(table, "grants", Grant),
            functools.partial(table, "groups", MigratedGroup),
            functools.partial(table, "tables", Table),
            functools.partial(table, "table_size", TableSize),
            functools.partial(table, "table_failures", TableError),
            functools.partial(table, "workspace_objects", WorkspaceObjectInfo),
            functools.partial(table, "permissions", Permissions),
            functools.partial(table, "submit_runs", SubmitRunInfo),
            functools.partial(table, "policies", PolicyInfo),
            functools.partial(table, "migration_status", MigrationStatus),
            functools.partial(table, "udfs", Udf),
            functools.partial(table, "logs", LogRecord),
        ],
    )
    deployer.deploy_view("objects", "queries/views/objects.sql")
    deployer.deploy_view("grant_detail", "queries/views/grant_detail.sql")
    deployer.deploy_view("table_estimates", "queries/views/table_estimates.sql")
    deployer.deploy_view("misc_patterns", "queries/views/misc_patterns.sql")
    deployer.deploy_view("code_patterns", "queries/views/code_patterns.sql")


def extract_major_minor(version_string):
    match = re.search(r'(\d+\.\d+)', version_string)
    if match:
        return match.group(1)
    return None


class WorkspaceInstaller:
    def __init__(
        self,
        prompts: Prompts,
        installation: Installation,
        ws: WorkspaceClient,
        product_info: ProductInfo,
        environ: dict[str, str] | None = None,
        tasks: list[Task] | None = None,
    ):
        if not environ:
            environ = dict(os.environ.items())
        if "DATABRICKS_RUNTIME_VERSION" in environ:
            msg = "WorkspaceInstaller is not supposed to be executed in Databricks Runtime"
            raise SystemExit(msg)
        self._ws = ws
        self._installation = installation
        self._prompts = prompts
        self._policy_installer = ClusterPolicyInstaller(installation, ws, prompts)
        self._product_info = product_info
        self._force_install = environ.get("UCX_FORCE_INSTALL")
        self._is_account_install = environ.get("UCX_FORCE_INSTALL") == "account"
        self._tasks = tasks if tasks else Workflows.all().tasks()

    def run(
        self,
        default_config: WorkspaceConfig | None = None,
        verify_timeout=timedelta(minutes=2),
        sql_backend_factory: Callable[[WorkspaceConfig], SqlBackend] | None = None,
        wheel_builder_factory: Callable[[], WheelsV2] | None = None,
        config: WorkspaceConfig | None = None,
    ) -> WorkspaceConfig:
        logger.info(f"Installing UCX v{self._product_info.version()}")
        if config is None:
            config = self.configure(default_config)
        if not sql_backend_factory:
            sql_backend_factory = self._new_sql_backend
        if not wheel_builder_factory:
            wheel_builder_factory = self._new_wheel_builder
        wheels = wheel_builder_factory()
        install_state = InstallState.from_installation(self._installation)
        if self._is_testing():
            return config
        workflows_deployment = WorkflowsDeployment(
            config,
            self._installation,
            install_state,
            self._ws,
            wheels,
            self._product_info,
            verify_timeout,
            self._tasks,
        )
        workspace_installation = WorkspaceInstallation(
            config,
            self._installation,
            install_state,
            sql_backend_factory(config),
            self._ws,
            workflows_deployment,
            self._prompts,
            self._product_info,
        )
        try:
            workspace_installation.run()
        except ManyError as err:
            if len(err.errs) == 1:
                raise err.errs[0] from None
            raise err
        return config

    def _is_testing(self):
        return self._product_info.product_name() != "ucx"

    def _prompt_for_new_installation(self) -> WorkspaceConfig:
        logger.info("Please answer a couple of questions to configure Unity Catalog migration")
        inventory_database = self._prompts.question(
            "Inventory Database stored in hive_metastore", default="ucx", valid_regex=r"^\w+$"
        )
        log_level = self._prompts.question("Log level", default="INFO").upper()
        num_threads = int(self._prompts.question("Number of threads", default="8", valid_number=True))
        configure_groups = ConfigureGroups(self._prompts)
        configure_groups.run()
        # Check if terraform is being used
        is_terraform_used = self._prompts.confirm("Do you use Terraform to deploy your infrastructure?")
        include_databases = self._select_databases()
        trigger_job = self._prompts.confirm("Do you want to trigger assessment job after installation?")
        return WorkspaceConfig(
            inventory_database=inventory_database,
            workspace_group_regex=configure_groups.workspace_group_regex,
            workspace_group_replace=configure_groups.workspace_group_replace,
            account_group_regex=configure_groups.account_group_regex,
            group_match_by_external_id=configure_groups.group_match_by_external_id,  # type: ignore[arg-type]
            include_group_names=configure_groups.include_group_names,
            renamed_group_prefix=configure_groups.renamed_group_prefix,
            log_level=log_level,
            num_threads=num_threads,
            is_terraform_used=is_terraform_used,
            include_databases=include_databases,
            trigger_job=trigger_job,
        )

    def _compare_remote_local_versions(self):
        try:
            local_version = self._product_info.released_version()
            remote_version = self._installation.load(Version).version
            if extract_major_minor(remote_version) == extract_major_minor(local_version):
                logger.info(f"UCX v{self._product_info.version()} is already installed on this workspace")
                msg = "Do you want to update the existing installation?"
                if not self._prompts.confirm(msg):
                    raise RuntimeWarning(
                        "UCX workspace remote and local install versions are same and no override is requested. Exiting..."
                    )
        except NotFound as err:
            logger.warning(f"UCX workspace remote version not found: {err}")

    def _new_wheel_builder(self):
        return WheelsV2(self._installation, self._product_info)

    def _new_sql_backend(self, config: WorkspaceConfig) -> SqlBackend:
        return StatementExecutionBackend(self._ws, config.warehouse_id)

    def _confirm_force_install(self) -> bool:
        if not self._force_install:
            return False
        msg = "[ADVANCED] UCX is already installed on this workspace. Do you want to create a new installation?"
        if not self._prompts.confirm(msg):
            raise RuntimeWarning("UCX is already installed, but no confirmation")
        if not self._installation.is_global() and self._force_install == "global":
            # TODO:
            # Logic for forced global over user install
            # Migration logic will go here
            # verify complains without full path, asks to raise NotImplementedError builtin
            raise databricks.sdk.errors.NotImplemented("Migration needed. Not implemented yet.")
        if self._installation.is_global() and self._force_install == "user":
            # Logic for forced user install over global install
            self._installation = Installation.assume_user_home(self._ws, self._product_info.product_name())
            return True
        return False

    def configure(self, default_config: WorkspaceConfig | None = None) -> WorkspaceConfig:
        try:
            config = self._installation.load(WorkspaceConfig)
            self._compare_remote_local_versions()
            if self._confirm_force_install():
                return self._configure_new_installation(default_config)
            self._apply_upgrades()
            return config
        except NotFound as err:
            logger.debug(f"Cannot find previous installation: {err}")
        return self._configure_new_installation(default_config)

    def replace_config(self, **changes: Any):
        """
        Persist the list of workspaces where UCX is successfully installed in the config
        """
        try:
            config = self._installation.load(WorkspaceConfig)
            new_config = dataclasses.replace(config, **changes)
            self._installation.save(new_config)
        except (PermissionDenied, NotFound, ValueError):
            logger.warning(f"Failed to replace config for {self._ws.config.host}")

    def _apply_upgrades(self):
        try:
            upgrades = Upgrades(self._product_info, self._installation)
            upgrades.apply(self._ws)
        except NotFound as err:
            logger.warning(f"Installed version is too old: {err}")
            return

    def _configure_new_installation(self, default_config: WorkspaceConfig | None = None) -> WorkspaceConfig:
        if default_config is None:
            default_config = self._prompt_for_new_installation()
        HiveMetastoreLineageEnabler(self._ws).apply(self._prompts, self._is_account_install)
        self._check_inventory_database_exists(default_config.inventory_database)
        warehouse_id = self._configure_warehouse()

        policy_id, instance_profile, spark_conf_dict, instance_pool_id = self._policy_installer.create(
            default_config.inventory_database
        )

        # Save configurable values for table migration cluster
        min_workers, max_workers, spark_conf_dict = self._config_table_migration(spark_conf_dict)

        config = dataclasses.replace(
            default_config,
            warehouse_id=warehouse_id,
            instance_profile=instance_profile,
            spark_conf=spark_conf_dict,
            min_workers=min_workers,
            max_workers=max_workers,
            policy_id=policy_id,
            instance_pool_id=instance_pool_id,
        )
        self._installation.save(config)
        if self._is_account_install:
            return config
        ws_file_url = self._installation.workspace_link(config.__file__)
        if self._prompts.confirm(f"Open config file in the browser and continue installing? {ws_file_url}"):
            webbrowser.open(ws_file_url)
        return config

    def _config_table_migration(self, spark_conf_dict) -> tuple[int, int, dict]:
        # parallelism will not be needed if backlog is fixed in https://databricks.atlassian.net/browse/ES-975874
        if self._is_account_install:
            return 1, 10, spark_conf_dict
        parallelism = self._prompts.question(
            "Parallelism for migrating dbfs root delta tables with deep clone", default="200", valid_number=True
        )
        if int(parallelism) > 200:
            spark_conf_dict.update({'spark.sql.sources.parallelPartitionDiscovery.parallelism': parallelism})
        # mix max workers for auto-scale migration job cluster
        min_workers = int(
            self._prompts.question(
                "Min workers for auto-scale job cluster for table migration", default="1", valid_number=True
            )
        )
        max_workers = int(
            self._prompts.question(
                "Max workers for auto-scale job cluster for table migration", default="10", valid_number=True
            )
        )
        return min_workers, max_workers, spark_conf_dict

    def _select_databases(self):
        selected_databases = self._prompts.question(
            "Comma-separated list of databases to migrate. If not specified, we'll use all "
            "databases in hive_metastore",
            default="<ALL>",
        )
        if selected_databases != "<ALL>":
            return [x.strip() for x in selected_databases.split(",")]
        return None

    def _configure_warehouse(self) -> str:
        def warehouse_type(_):
            return _.warehouse_type.value if not _.enable_serverless_compute else "SERVERLESS"

        pro_warehouses = {"[Create new PRO SQL warehouse]": "create_new"} | {
            f"{_.name} ({_.id}, {warehouse_type(_)}, {_.state.value})": _.id
            for _ in self._ws.warehouses.list()
            if _.warehouse_type == EndpointInfoWarehouseType.PRO
        }
        if self._is_account_install:
            warehouse_id = "create_new"
        else:
            warehouse_id = self._prompts.choice_from_dict(
                "Select PRO or SERVERLESS SQL warehouse to run assessment dashboards on", pro_warehouses
            )
        if warehouse_id == "create_new":
            new_warehouse = self._ws.warehouses.create(
                name=f"{WAREHOUSE_PREFIX} {time.time_ns()}",
                spot_instance_policy=SpotInstancePolicy.COST_OPTIMIZED,
                warehouse_type=CreateWarehouseRequestWarehouseType.PRO,
                cluster_size="Small",
                max_num_clusters=1,
            )
            warehouse_id = new_warehouse.id
        return warehouse_id

    def _check_inventory_database_exists(self, inventory_database: str):
        logger.info("Fetching installations...")
        for installation in Installation.existing(self._ws, self._product_info.product_name()):
            try:
                config = installation.load(WorkspaceConfig)
                if config.inventory_database == inventory_database:
                    raise AlreadyExists(
                        f"Inventory database '{inventory_database}' already exists in another installation"
                    )
            except (PermissionDenied, NotFound, SerdeError):
                continue


class WorkspaceInstallation(InstallationMixin):
    def __init__(
        self,
        config: WorkspaceConfig,
        installation: Installation,
        install_state: InstallState,
        sql_backend: SqlBackend,
        ws: WorkspaceClient,
        workflows_installer: WorkflowsDeployment,
        prompts: Prompts,
        product_info: ProductInfo,
    ):
        self._config = config
        self._installation = installation
        self._install_state = install_state
        self._ws = ws
        self._sql_backend = sql_backend
        self._workflows_installer = workflows_installer
        self._prompts = prompts
        self._product_info = product_info
        environ = dict(os.environ.items())
        self._is_account_install = environ.get("UCX_FORCE_INSTALL") == "account"
        super().__init__(config, installation, ws)

    @classmethod
    def current(cls, ws: WorkspaceClient):
        # TODO: remove this method, it's no longer needed
        product_info = ProductInfo.from_class(WorkspaceConfig)
        installation = product_info.current_installation(ws)
        install_state = InstallState.from_installation(installation)
        config = installation.load(WorkspaceConfig)
        sql_backend = StatementExecutionBackend(ws, config.warehouse_id)
        wheels = product_info.wheels(ws)
        prompts = Prompts()
        timeout = timedelta(minutes=2)
        tasks = Workflows.all().tasks()
        workflows_installer = WorkflowsDeployment(
            config,
            installation,
            install_state,
            ws,
            wheels,
            product_info,
            timeout,
            tasks,
        )

        return cls(
            config,
            installation,
            install_state,
            sql_backend,
            ws,
            workflows_installer,
            prompts,
            product_info,
        )

    @property
    def config(self):
        return self._config

    @property
    def folder(self):
        return self._installation.install_folder()

    def run(self):
        Threads.strict(
            "installing components",
            [
                self._create_dashboards,
                self._create_database,
            ],
        )
        readme_url = self._workflows_installer.create_jobs(self._prompts)
        if not self._is_account_install and self._prompts.confirm(f"Open job overview in your browser? {readme_url}"):
            webbrowser.open(readme_url)
        logger.info(f"Installation completed successfully! Please refer to the {readme_url} for the next steps.")

        if self.config.trigger_job:
            logger.info("Triggering the assessment workflow")
            self._trigger_workflow("assessment")

    def _create_database(self):
        try:
            deploy_schema(self._sql_backend, self._config.inventory_database)
        except Exception as err:
            if "UNRESOLVED_COLUMN.WITH_SUGGESTION" in str(err):
                msg = (
                    "The UCX version is not matching with the installed version."
                    "Kindly uninstall and reinstall UCX.\n"
                    "Please Follow the Below Command to uninstall and Install UCX\n"
                    "UCX Uninstall: databricks labs uninstall ucx.\n"
                    "UCX Install: databricks labs install ucx"
                )
                raise BadRequest(msg) from err
            raise err

    def _create_dashboards(self):
        logger.info("Creating dashboards...")
        local_query_files = find_project_root(__file__) / "src/databricks/labs/ucx/queries"
        dash = DashboardFromFiles(
            self._ws,
            state=self._install_state,
            local_folder=local_query_files,
            remote_folder=f"{self._installation.install_folder()}/queries",
            name_prefix=self._name("UCX "),
            warehouse_id=self._warehouse_id,
            query_text_callback=self._config.replace_inventory_variable,
        )
        dash.create_dashboards()

    def uninstall(self):
        if self._prompts and not self._prompts.confirm(
            "Do you want to uninstall ucx from the workspace too, this would "
            "remove ucx project folder, dashboards, queries and jobs"
        ):
            return
        # TODO: this is incorrect, fetch the remote version (that appeared only in Feb 2024)
        logger.info(f"Deleting UCX v{self._product_info.version()} from {self._ws.config.host}")
        try:
            self._installation.files()
        except NotFound:
            logger.error(f"Check if {self._installation.install_folder()} is present")
            return
        self._remove_database()
        self._remove_jobs()
        self._remove_warehouse()
        self._remove_policies()
        self._remove_secret_scope()
        self._installation.remove()
        logger.info("UnInstalling UCX complete")

    def _remove_database(self):
        if self._prompts and not self._prompts.confirm(
            f"Do you want to delete the inventory database {self._config.inventory_database} too?"
        ):
            return
        logger.info(f"Deleting inventory database {self._config.inventory_database}")
        deployer = SchemaDeployer(self._sql_backend, self._config.inventory_database, Any)
        deployer.delete_schema()

    def _remove_policies(self):
        logger.info("Deleting cluster policy")
        try:
            self._ws.cluster_policies.delete(policy_id=self.config.policy_id)
        except NotFound:
            logger.error("UCX Policy already deleted")

    def _remove_secret_scope(self):
        logger.info("Deleting secret scope")
        try:
            if self.config.uber_spn_id is not None:
                self._ws.secrets.delete_scope(self.config.inventory_database)
        except NotFound:
            logger.error("Secret scope already deleted")

    def _remove_jobs(self):
        logger.info("Deleting jobs")
        if not self._install_state.jobs:
            logger.error("No jobs present or jobs already deleted")
            return
        for step_name, job_id in self._install_state.jobs.items():
            try:
                logger.info(f"Deleting {step_name} job_id={job_id}.")
                self._ws.jobs.delete(job_id)
            except InvalidParameterValue:
                logger.error(f"Already deleted: {step_name} job_id={job_id}.")
                continue

    def _remove_warehouse(self):
        try:
            warehouse_name = self._ws.warehouses.get(self._config.warehouse_id).name
            if warehouse_name.startswith(WAREHOUSE_PREFIX):
                logger.info(f"Deleting {warehouse_name}.")
                self._ws.warehouses.delete(id=self._config.warehouse_id)
        except InvalidParameterValue:
            logger.error("Error accessing warehouse details")

    def _trigger_workflow(self, step: str):
        job_id = int(self._install_state.jobs[step])
        job_url = f"{self._ws.config.host}#job/{job_id}"
        logger.debug(f"triggering {step} job: {self._ws.config.host}#job/{job_id}")
        self._ws.jobs.run_now(job_id)
        if self._prompts.confirm(f"Open {step} Job url that just triggered ? {job_url}"):
            webbrowser.open(job_url)


class AccountInstaller(AccountContext):
    def _get_safe_account_client(self) -> AccountClient:
        """
        Get account client with the correct host based on the cloud provider
        """
        if self.account_client.config.is_account_client:
            return self.account_client
        w = WorkspaceClient(product="ucx", product_version=__version__)
        host = w.config.environment.deployment_url("accounts")
        account_id = self.prompts.question("Please provide the Databricks account id")
        return AccountClient(host=host, account_id=account_id, product="ucx", product_version=__version__)

    def _can_administer(self, workspace: Workspace):
        try:
            # check if user is a workspace admin
            ws = self.account_client.get_workspace_client(workspace)
            current_user = ws.current_user.me()
            if current_user.groups is None:
                return False
            if "admins" not in [g.display for g in current_user.groups]:
                logger.warning(
                    f"{workspace.deployment_name}: User {current_user.user_name} is not a workspace admin. Skipping..."
                )
                return False
            # check if user has access to workspace
        except (PermissionDenied, NotFound, ValueError) as err:
            logger.warning(f"{workspace.deployment_name}: Encounter error {err}. Skipping...")
            return False
        return True

    def _get_accessible_workspaces(self):
        """
        Get all workspaces that the user has access to
        """
        accessible_workspaces = []
        for workspace in self.account_client.workspaces.list():
            if self._can_administer(workspace):
                accessible_workspaces.append(workspace)
        return accessible_workspaces

    def _get_installer(self, app: ProductInfo, workspace: Workspace) -> WorkspaceInstaller:
        workspace_client = self.account_client.get_workspace_client(workspace)
        logger.info(f"Installing UCX on workspace {workspace.deployment_name}")
        try:
            current = app.current_installation(workspace_client)
        except NotFound:
            current = Installation.assume_global(workspace_client, app.product_name())
        return WorkspaceInstaller(self.prompts, current, workspace_client, app)

    def install_on_account(self, app: ProductInfo | None = None):
        ctx = AccountContext(self._get_safe_account_client())
        if app is None:
            app = ProductInfo.from_class(WorkspaceConfig)
        default_config = None
        confirmed = False
        accessible_workspaces = self._get_accessible_workspaces()
        msg = "\n".join([w.deployment_name for w in accessible_workspaces])
        installed_workspaces = []
        if not self.prompts.confirm(
            f"UCX has detected the following workspaces available to install. \n{msg}\nDo you want to continue?"
        ):
            return

        for workspace in accessible_workspaces:
            logger.info(f"Installing UCX on workspace {workspace.deployment_name}")
            installer = self._get_installer(app, workspace)
            if not confirmed:
                default_config = None
            try:
                default_config = installer.run(default_config)
            except RuntimeWarning:
                logger.info("Skipping workspace...")
            # if user confirms to install on remaining workspaces with the same config, don't prompt again
            installed_workspaces.append(workspace)
            if confirmed:
                continue
            confirmed = self.prompts.confirm(
                "Do you want to install UCX on the remaining workspaces with the same config?"
            )

        installed_workspace_ids = [w.workspace_id for w in installed_workspaces if w.workspace_id is not None]
        for workspace in installed_workspaces:
            installer = self._get_installer(app, workspace)
            installer.replace_config(installed_workspace_ids=installed_workspace_ids)

        # upload the json dump of workspace info in the .ucx folder
        ctx.account_workspaces.sync_workspace_info(installed_workspaces)


def install_on_workspace(app: ProductInfo | None = None):
    if app is None:
        app = ProductInfo.from_class(WorkspaceConfig)
    prompts = Prompts()
    workspace_client = WorkspaceClient(product="ucx", product_version=__version__)
    try:
        current = app.current_installation(workspace_client)
    except NotFound:
        current = Installation.assume_global(workspace_client, app.product_name())
    installer = WorkspaceInstaller(prompts, current, workspace_client, app)
    installer.run()


if __name__ == "__main__":
    logger = get_logger(__file__)

    env = dict(os.environ.items())
    force_install = env.get("UCX_FORCE_INSTALL")
    if force_install == "account":
        account_installer = AccountInstaller(AccountClient(product="ucx", product_version=__version__))
        account_installer.install_on_account()
    else:
        install_on_workspace()

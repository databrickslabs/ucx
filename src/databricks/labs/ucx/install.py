import functools
import logging
import os
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
from databricks.labs.blueprint.wheels import ProductInfo, WheelsV2, find_project_root
from databricks.labs.lsql.backends import SqlBackend, StatementExecutionBackend
from databricks.labs.lsql.deployment import SchemaDeployer
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import (  # pylint: disable=redefined-builtin
    AlreadyExists,
    BadRequest,
    InvalidParameterValue,
    NotFound,
)
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
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
from databricks.labs.ucx.framework.dashboards import DashboardFromFiles
from databricks.labs.ucx.hive_metastore.grants import Grant
from databricks.labs.ucx.hive_metastore.locations import ExternalLocation, Mount
from databricks.labs.ucx.hive_metastore.table_migrate import MigrationStatus
from databricks.labs.ucx.hive_metastore.table_size import TableSize
from databricks.labs.ucx.hive_metastore.tables import Table, TableError
from databricks.labs.ucx.installer.hms_lineage import HiveMetastoreLineageEnabler
from databricks.labs.ucx.installer.mixins import InstallationMixin
from databricks.labs.ucx.installer.policy import ClusterPolicyInstaller
from databricks.labs.ucx.installer.workflows import WorkflowsInstallation
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
        ],
    )
    deployer.deploy_view("objects", "queries/views/objects.sql")
    deployer.deploy_view("grant_detail", "queries/views/grant_detail.sql")
    deployer.deploy_view("table_estimates", "queries/views/table_estimates.sql")


class WorkspaceInstaller:
    def __init__(
        self,
        prompts: Prompts,
        installation: Installation,
        ws: WorkspaceClient,
        product_info: ProductInfo,
        environ: dict[str, str] | None = None,
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

    def run(
        self,
        verify_timeout=timedelta(minutes=2),
        sql_backend_factory: Callable[[WorkspaceConfig], SqlBackend] | None = None,
        wheel_builder_factory: Callable[[], WheelsV2] | None = None,
    ):
        logger.info(f"Installing UCX v{self._product_info.version()}")
        config = self.configure()
        if not sql_backend_factory:
            sql_backend_factory = self._new_sql_backend
        if not wheel_builder_factory:
            wheel_builder_factory = self._new_wheel_builder
        wheels = wheel_builder_factory()
        workflows_installer = WorkflowsInstallation(
            config, self._installation, self._ws, wheels, self._prompts, self._product_info, verify_timeout
        )
        workspace_installation = WorkspaceInstallation(
            config,
            self._installation,
            sql_backend_factory(config),
            self._ws,
            workflows_installer,
            self._prompts,
            self._product_info,
        )
        try:
            workspace_installation.run()
        except ManyError as err:
            if len(err.errs) == 1:
                raise err.errs[0] from None
            raise err

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

    def configure(self) -> WorkspaceConfig:
        try:
            config = self._installation.load(WorkspaceConfig)
            if self._confirm_force_install():
                return self._configure_new_installation()
            self._apply_upgrades()
            return config
        except NotFound as err:
            logger.debug(f"Cannot find previous installation: {err}")
        return self._configure_new_installation()

    def _apply_upgrades(self):
        try:
            upgrades = Upgrades(self._product_info, self._installation)
            upgrades.apply(self._ws)
        except NotFound as err:
            logger.warning(f"Installed version is too old: {err}")
            return

    def _configure_new_installation(self) -> WorkspaceConfig:
        logger.info("Please answer a couple of questions to configure Unity Catalog migration")
        HiveMetastoreLineageEnabler(self._ws).apply(self._prompts)
        inventory_database = self._prompts.question(
            "Inventory Database stored in hive_metastore", default="ucx", valid_regex=r"^\w+$"
        )

        self._check_inventory_database_exists(inventory_database)
        warehouse_id = self._configure_warehouse()
        configure_groups = ConfigureGroups(self._prompts)
        configure_groups.run()
        log_level = self._prompts.question("Log level", default="INFO").upper()
        num_threads = int(self._prompts.question("Number of threads", default="8", valid_number=True))

        policy_id, instance_profile, spark_conf_dict = self._policy_installer.create(inventory_database)

        # Check if terraform is being used
        is_terraform_used = self._prompts.confirm("Do you use Terraform to deploy your infrastructure?")

        config = WorkspaceConfig(
            inventory_database=inventory_database,
            workspace_group_regex=configure_groups.workspace_group_regex,
            workspace_group_replace=configure_groups.workspace_group_replace,
            account_group_regex=configure_groups.account_group_regex,
            group_match_by_external_id=configure_groups.group_match_by_external_id,  # type: ignore[arg-type]
            include_group_names=configure_groups.include_group_names,
            renamed_group_prefix=configure_groups.renamed_group_prefix,
            warehouse_id=warehouse_id,
            log_level=log_level,
            num_threads=num_threads,
            instance_profile=instance_profile,
            spark_conf=spark_conf_dict,
            policy_id=policy_id,
            is_terraform_used=is_terraform_used,
            include_databases=self._select_databases(),
        )
        self._installation.save(config)
        ws_file_url = self._installation.workspace_link(config.__file__)
        if self._prompts.confirm(f"Open config file in the browser and continue installing? {ws_file_url}"):
            webbrowser.open(ws_file_url)
        return config

    def _select_databases(self):
        selected_databases = self._prompts.question(
            "Comma-separated list of databases to migrate. If not specified, we'll use all "
            "databases in hive_metastore",
            default="<ALL>",
        )
        if selected_databases != "<ALL>":
            return [x.strip() for x in selected_databases.split(",")]
        return None

    def _configure_warehouse(self):
        def warehouse_type(_):
            return _.warehouse_type.value if not _.enable_serverless_compute else "SERVERLESS"

        pro_warehouses = {"[Create new PRO SQL warehouse]": "create_new"} | {
            f"{_.name} ({_.id}, {warehouse_type(_)}, {_.state.value})": _.id
            for _ in self._ws.warehouses.list()
            if _.warehouse_type == EndpointInfoWarehouseType.PRO
        }
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
            except NotFound:
                continue
            except SerdeError:
                continue


class WorkspaceInstallation(InstallationMixin):
    def __init__(
        self,
        config: WorkspaceConfig,
        installation: Installation,
        sql_backend: SqlBackend,
        ws: WorkspaceClient,
        workflows_installer: WorkflowsInstallation,
        prompts: Prompts,
        product_info: ProductInfo,
    ):
        self._config = config
        self._installation = installation
        self._ws = ws
        self._sql_backend = sql_backend
        self._workflows_installer = workflows_installer
        self._prompts = prompts
        self._state = InstallState.from_installation(installation)
        self._product_info = product_info
        super().__init__(config, installation, ws)

    @classmethod
    def current(cls, ws: WorkspaceClient):
        product_info = ProductInfo.from_class(WorkspaceConfig)
        installation = product_info.current_installation(ws)
        config = installation.load(WorkspaceConfig)
        sql_backend = StatementExecutionBackend(ws, config.warehouse_id)
        wheels = product_info.wheels(ws)
        prompts = Prompts()
        timeout = timedelta(minutes=2)
        workflows_installer = WorkflowsInstallation(config, installation, ws, wheels, prompts, product_info, timeout)

        return cls(config, installation, sql_backend, ws, workflows_installer, prompts, product_info)

    @property
    def config(self):
        return self._config

    @property
    def folder(self):
        return self._installation.install_folder()

    def run(self):
        logger.info(f"Installing UCX v{self._product_info.version()}")
        Threads.strict(
            "installing components",
            [
                self._create_dashboards,
                self._create_database,
                self._workflows_installer.create_jobs,
            ],
        )

        readme_url = self._create_readme()
        logger.info(f"Installation completed successfully! Please refer to the {readme_url} for the next steps.")

        if self._prompts.confirm("Do you want to trigger assessment job ?"):
            logger.info("Triggering the assessment workflow")
            self._trigger_workflow("assessment")

    def config_file_link(self):
        return self._installation.workspace_link('config.yml')

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
            state=self._state,
            local_folder=local_query_files,
            remote_folder=f"{self._installation.install_folder()}/queries",
            name_prefix=self._name("UCX "),
            warehouse_id=self._warehouse_id,
            query_text_callback=self._config.replace_inventory_variable,
        )
        dash.create_dashboards()

    def _create_readme(self) -> str:
        debug_notebook_link = self._installation.workspace_markdown_link('debug notebook', 'DEBUG.py')
        markdown = [
            "# UCX - The Unity Catalog Migration Assistant",
            f'To troubleshoot, see {debug_notebook_link}.\n',
            "Here are the URLs and descriptions of workflows that trigger various stages of migration.",
            "All jobs are defined with necessary cluster configurations and DBR versions.\n",
        ]
        for step_name in self.step_list():
            if step_name not in self._state.jobs:
                logger.warning(f"Skipping step '{step_name}' since it was not deployed.")
                continue
            job_id = self._state.jobs[step_name]
            dashboard_link = ""
            dashboards_per_step = [d for d in self._state.dashboards.keys() if d.startswith(step_name)]
            for dash in dashboards_per_step:
                if len(dashboard_link) == 0:
                    dashboard_link += "Go to the one of the following dashboards after running the job:\n"
                first, second = dash.replace("_", " ").title().split()
                dashboard_url = f"{self._ws.config.host}/sql/dashboards/{self._state.dashboards[dash]}"
                dashboard_link += f"  - [{first} ({second}) dashboard]({dashboard_url})\n"
            job_link = f"[{self._name(step_name)}]({self._ws.config.host}#job/{job_id})"
            markdown.append("---\n\n")
            markdown.append(f"## {job_link}\n\n")
            markdown.append(f"{dashboard_link}")
            markdown.append("\nThe workflow consists of the following separate tasks:\n\n")
            for task in self.sorted_tasks():
                if task.workflow != step_name:
                    continue
                doc = self._config.replace_inventory_variable(task.doc)
                markdown.append(f"### `{task.name}`\n\n")
                markdown.append(f"{doc}\n")
                markdown.append("\n\n")
        preamble = ["# Databricks notebook source", "# MAGIC %md"]
        intro = "\n".join(preamble + [f"# MAGIC {line}" for line in markdown])
        self._installation.upload('README.py', intro.encode('utf8'))
        readme_url = self._installation.workspace_link('README')
        if self._prompts and self._prompts.confirm(f"Open job overview in your browser? {readme_url}"):
            webbrowser.open(readme_url)
        return readme_url

    def _replace_inventory_variable(self, text: str) -> str:
        return text.replace("$inventory", f"hive_metastore.{self._config.inventory_database}")

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
        if not self._state.jobs:
            logger.error("No jobs present or jobs already deleted")
            return
        for step_name, job_id in self._state.jobs.items():
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

    def validate_step(self, step: str) -> bool:
        job_id = int(self._state.jobs[step])
        logger.debug(f"Validating {step} workflow: {self._ws.config.host}#job/{job_id}")
        current_runs = list(self._ws.jobs.list_runs(completed_only=False, job_id=job_id))
        for run in current_runs:
            if run.state and run.state.result_state == RunResultState.SUCCESS:
                return True
        for run in current_runs:
            if (
                run.run_id
                and run.state
                and run.state.life_cycle_state in (RunLifeCycleState.RUNNING, RunLifeCycleState.PENDING)
            ):
                logger.info("Identified a run in progress waiting for run completion")
                self._ws.jobs.wait_get_run_job_terminated_or_skipped(run_id=run.run_id)
                run_new_state = self._ws.jobs.get_run(run_id=run.run_id).state
                return run_new_state is not None and run_new_state.result_state == RunResultState.SUCCESS
        return False

    def validate_and_run(self, step: str):
        if not self.validate_step(step):
            self._workflows_installer.run_workflow(step)

    def _trigger_workflow(self, step: str):
        job_id = int(self._state.jobs[step])
        job_url = f"{self._ws.config.host}#job/{job_id}"
        logger.debug(f"triggering {step} job: {self._ws.config.host}#job/{job_id}")
        self._ws.jobs.run_now(job_id)
        if self._prompts.confirm(f"Open {step} Job url that just triggered ? {job_url}"):
            webbrowser.open(job_url)


if __name__ == "__main__":
    logger = get_logger(__file__)
    logger.setLevel("INFO")
    app = ProductInfo.from_class(WorkspaceConfig)
    workspace_client = WorkspaceClient(product="ucx", product_version=__version__)
    try:
        current = app.current_installation(workspace_client)
    except NotFound:
        current = Installation.assume_global(workspace_client, app.product_name())
    prmpts = Prompts()
    installer = WorkspaceInstaller(prmpts, current, workspace_client, app)
    installer.run()

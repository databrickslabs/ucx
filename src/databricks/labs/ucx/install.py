import functools
import json
import logging
import os
import re
import sys
import time
import webbrowser
from collections.abc import Callable
from dataclasses import replace
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from databricks.labs.blueprint.entrypoint import get_logger
from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.installer import InstallState
from databricks.labs.blueprint.parallel import ManyError, Threads
from databricks.labs.blueprint.tui import Prompts
from databricks.labs.blueprint.upgrades import Upgrades
from databricks.labs.blueprint.wheels import ProductInfo, WheelsV2, find_project_root
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import (  # pylint: disable=redefined-builtin
    Aborted,
    AlreadyExists,
    BadRequest,
    Cancelled,
    DataLoss,
    DeadlineExceeded,
    InternalError,
    InvalidParameterValue,
    NotFound,
    NotImplemented,
    OperationFailed,
    PermissionDenied,
    RequestLimitExceeded,
    ResourceAlreadyExists,
    ResourceConflict,
    ResourceDoesNotExist,
    ResourceExhausted,
    TemporarilyUnavailable,
    TooManyRequests,
    Unauthenticated,
    Unknown,
)
from databricks.sdk.retries import retried
from databricks.sdk.service import compute, jobs
from databricks.sdk.service.jobs import RunLifeCycleState, RunResultState
from databricks.sdk.service.sql import (
    CreateWarehouseRequestWarehouseType,
    EndpointInfoWarehouseType,
    SpotInstancePolicy,
)

from databricks.labs.ucx.__about__ import __version__
from databricks.labs.ucx.assessment.azure import AzureServicePrincipalInfo
from databricks.labs.ucx.assessment.clusters import ClusterInfo
from databricks.labs.ucx.assessment.init_scripts import GlobalInitScriptInfo
from databricks.labs.ucx.assessment.jobs import JobInfo, SubmitRunInfo
from databricks.labs.ucx.assessment.pipelines import PipelineInfo
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.configure import ConfigureClusterOverrides
from databricks.labs.ucx.framework.crawlers import (
    SchemaDeployer,
    SqlBackend,
    StatementExecutionBackend,
)
from databricks.labs.ucx.framework.dashboards import DashboardFromFiles
from databricks.labs.ucx.framework.tasks import _TASKS, Task
from databricks.labs.ucx.hive_metastore.grants import Grant
from databricks.labs.ucx.hive_metastore.locations import ExternalLocation, Mount
from databricks.labs.ucx.hive_metastore.table_size import TableSize
from databricks.labs.ucx.hive_metastore.tables import Table, TableError
from databricks.labs.ucx.installer.hms_lineage import HiveMetastoreLineageEnabler
from databricks.labs.ucx.runtime import main
from databricks.labs.ucx.workspace_access.base import Permissions
from databricks.labs.ucx.workspace_access.generic import WorkspaceObjectInfo
from databricks.labs.ucx.workspace_access.groups import ConfigureGroups, MigratedGroup

TAG_STEP = "step"
WAREHOUSE_PREFIX = "Unity Catalog Migration"
NUM_USER_ATTEMPTS = 10  # number of attempts user gets at answering a question

EXTRA_TASK_PARAMS = {
    "job_id": "{{job_id}}",
    "run_id": "{{run_id}}",
    "parent_run_id": "{{parent_run_id}}",
}
DEBUG_NOTEBOOK = """# Databricks notebook source
# MAGIC %md
# MAGIC # Debug companion for UCX installation (see [README]({readme_link}))
# MAGIC
# MAGIC Production runs are supposed to be triggered through the following jobs: {job_links}
# MAGIC
# MAGIC **This notebook is overwritten with each UCX update/(re)install.**

# COMMAND ----------

# MAGIC %pip install /Workspace{remote_wheel}
dbutils.library.restartPython()

# COMMAND ----------

import logging
from pathlib import Path
from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.logger import install_logger
from databricks.labs.ucx.__about__ import __version__
from databricks.labs.ucx.config import WorkspaceConfig
from databricks.sdk import WorkspaceClient

install_logger()
logging.getLogger("databricks").setLevel("DEBUG")

cfg = Installation.load_local(WorkspaceConfig, Path("/Workspace{config_file}"))
ws = WorkspaceClient()

print(__version__)
"""

TEST_RUNNER_NOTEBOOK = """# Databricks notebook source
# MAGIC %pip install /Workspace{remote_wheel}
dbutils.library.restartPython()

# COMMAND ----------

from databricks.labs.ucx.runtime import main

main(f'--config=/Workspace{config_file}',
     f'--task=' + dbutils.widgets.get('task'),
     f'--job_id=' + dbutils.widgets.get('job_id'),
     f'--run_id=' + dbutils.widgets.get('run_id'),
     f'--parent_run_id=' + dbutils.widgets.get('parent_run_id'))
"""

logger = logging.getLogger(__name__)

PRODUCT_INFO = ProductInfo(__file__)


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
        ],
    )
    deployer.deploy_view("objects", "queries/views/objects.sql")
    deployer.deploy_view("grant_detail", "queries/views/grant_detail.sql")


class WorkspaceInstaller:
    def __init__(self, prompts: Prompts, installation: Installation, ws: WorkspaceClient):
        if "DATABRICKS_RUNTIME_VERSION" in os.environ:
            msg = "WorkspaceInstaller is not supposed to be executed in Databricks Runtime"
            raise SystemExit(msg)
        self._ws = ws
        self._installation = installation
        self._prompts = prompts

    def run(
        self,
        verify_timeout=timedelta(minutes=2),
        sql_backend_factory: Callable[[WorkspaceConfig], SqlBackend] | None = None,
        wheel_builder_factory: Callable[[], WheelsV2] | None = None,
    ):
        logger.info(f"Installing UCX v{PRODUCT_INFO.version()}")
        config = self.configure()
        if not sql_backend_factory:
            sql_backend_factory = self._new_sql_backend
        if not wheel_builder_factory:
            wheel_builder_factory = self._new_wheel_builder
        workspace_installation = WorkspaceInstallation(
            config,
            self._installation,
            sql_backend_factory(config),
            wheel_builder_factory(),
            self._ws,
            self._prompts,
            verify_timeout=verify_timeout,
        )
        try:
            workspace_installation.run()
        except ManyError as err:
            if len(err.errs) == 1:
                raise err.errs[0] from None
            raise err

    def _new_wheel_builder(self):
        return WheelsV2(self._installation, PRODUCT_INFO)

    def _new_sql_backend(self, config: WorkspaceConfig) -> SqlBackend:
        return StatementExecutionBackend(self._ws, config.warehouse_id)

    def configure(self) -> WorkspaceConfig:
        try:
            config = self._installation.load(WorkspaceConfig)
            self._apply_upgrades()
            return config
        except NotFound as err:
            logger.debug(f"Cannot find previous installation: {err}")
        return self._configure_new_installation()

    def _apply_upgrades(self):
        try:
            upgrades = Upgrades(PRODUCT_INFO, self._installation)
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

        warehouse_id = self._configure_warehouse()
        configure_groups = ConfigureGroups(self._prompts)
        configure_groups.run()
        log_level = self._prompts.question("Log level", default="INFO").upper()
        num_threads = int(self._prompts.question("Number of threads", default="8", valid_number=True))

        # Checking for external HMS
        instance_profile = None
        spark_conf_dict = {}
        policies_with_external_hms = list(self._get_cluster_policies_with_external_hive_metastores())
        if len(policies_with_external_hms) > 0 and self._prompts.confirm(
            "We have identified one or more cluster policies set up for an external metastore"
            "Would you like to set UCX to connect to the external metastore?"
        ):
            logger.info("Setting up an external metastore")
            cluster_policies = {conf.name: conf.definition for conf in policies_with_external_hms}
            if len(cluster_policies) >= 1:
                cluster_policy = json.loads(self._prompts.choice_from_dict("Choose a cluster policy", cluster_policies))
                instance_profile, spark_conf_dict = self._get_ext_hms_conf_from_policy(cluster_policy)

        policy_id = self._create_cluster_policy(inventory_database, spark_conf_dict, instance_profile)

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
        )
        self._installation.save(config)
        ws_file_url = self._installation.workspace_link(config.__file__)
        if self._prompts.confirm(f"Open config file in the browser and continue installing? {ws_file_url}"):
            webbrowser.open(ws_file_url)
        return config

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

    @staticmethod
    def _policy_config(value: str):
        return {"type": "fixed", "value": value}

    def _create_cluster_policy(
        self, inventory_database: str, spark_conf: dict, instance_profile: str | None
    ) -> str | None:
        policy_name = f"Unity Catalog Migration ({inventory_database}) ({self._ws.current_user.me().user_name})"
        policies = self._ws.cluster_policies.list()
        policy_id = None
        for policy in policies:
            if policy.name == policy_name:
                policy_id = policy.policy_id
                logger.info(f"Cluster policy {policy_name} already present, reusing the same.")
                break
        if not policy_id:
            logger.info("Creating UCX cluster policy.")
            policy_id = self._ws.cluster_policies.create(
                name=policy_name,
                definition=self._cluster_policy_definition(conf=spark_conf, instance_profile=instance_profile),
                description="Custom cluster policy for Unity Catalog Migration (UCX)",
            ).policy_id
        return policy_id

    def _cluster_policy_definition(self, conf: dict, instance_profile: str | None) -> str:
        policy_definition = {
            "spark_version": self._policy_config(self._ws.clusters.select_spark_version(latest=True)),
            "node_type_id": self._policy_config(self._ws.clusters.select_node_type(local_disk=True)),
        }
        if conf:
            for key, value in conf.items():
                policy_definition[f"spark_conf.{key}"] = self._policy_config(value)
        if self._ws.config.is_aws:
            policy_definition["aws_attributes.availability"] = self._policy_config(
                compute.AwsAvailability.ON_DEMAND.value
            )
            if instance_profile:
                policy_definition["aws_attributes.instance_profile_arn"] = self._policy_config(instance_profile)
        elif self._ws.config.is_azure:  # pylint: disable=confusing-consecutive-elif
            policy_definition["azure_attributes.availability"] = self._policy_config(
                compute.AzureAvailability.ON_DEMAND_AZURE.value
            )
        else:
            policy_definition["gcp_attributes.availability"] = self._policy_config(
                compute.GcpAvailability.ON_DEMAND_GCP.value
            )
        return json.dumps(policy_definition)

    @staticmethod
    def _get_ext_hms_conf_from_policy(cluster_policy):
        spark_conf_dict = {}
        instance_profile = None
        if cluster_policy.get("aws_attributes.instance_profile_arn") is not None:
            instance_profile = cluster_policy.get("aws_attributes.instance_profile_arn").get("value")
            logger.info(f"Instance Profile is Set to {instance_profile}")
        for key in cluster_policy.keys():
            if (
                key.startswith("spark_conf.spark.sql.hive.metastore")
                or key.startswith("spark_conf.spark.hadoop.javax.jdo.option")
                or key.startswith("spark_conf.spark.databricks.hive.metastore")
                or key.startswith("spark_conf.spark.hadoop.hive.metastore.glue")
            ):
                spark_conf_dict[key[11:]] = cluster_policy[key]["value"]
        return instance_profile, spark_conf_dict

    def _get_cluster_policies_with_external_hive_metastores(self):
        for policy in self._ws.cluster_policies.list():
            def_json = json.loads(policy.definition)
            glue_node = def_json.get("spark_conf.spark.databricks.hive.metastore.glueCatalog.enabled")
            if glue_node is not None and glue_node.get("value") == "true":
                yield policy
                continue
            for key in def_json.keys():
                if key.startswith("spark_conf.spark.sql.hive.metastore"):
                    yield policy
                    break


class WorkspaceInstallation:
    def __init__(
        self,
        config: WorkspaceConfig,
        installation: Installation,
        sql_backend: SqlBackend,
        wheels: WheelsV2,
        ws: WorkspaceClient,
        prompts: Prompts,
        verify_timeout: timedelta,
    ):
        self._config = config
        self._installation = installation
        self._ws = ws
        self._wheels = wheels
        self._sql_backend = sql_backend
        self._prompts = prompts
        self._verify_timeout = verify_timeout
        self._state = InstallState.from_installation(installation)
        self._this_file = Path(__file__)

    @classmethod
    def current(cls, ws: WorkspaceClient):
        installation = PRODUCT_INFO.current_installation(ws)
        config = installation.load(WorkspaceConfig)
        sql_backend = StatementExecutionBackend(ws, config.warehouse_id)
        wheels = WheelsV2(installation, PRODUCT_INFO)
        prompts = Prompts()
        timeout = timedelta(minutes=2)
        return WorkspaceInstallation(config, installation, sql_backend, wheels, ws, prompts, timeout)

    @property
    def config(self):
        return self._config

    @property
    def folder(self):
        return self._installation.install_folder()

    def run(self):
        logger.info(f"Installing UCX v{PRODUCT_INFO.version()}")
        Threads.strict(
            "installing components",
            [
                self._create_dashboards,
                self._create_database,
                self.create_jobs,
            ],
        )
        readme_url = self._create_readme()
        logger.info(f"Installation completed successfully! Please refer to the {readme_url} for the next steps.")

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

    def run_workflow(self, step: str):
        job_id = int(self._state.jobs[step])
        logger.debug(f"starting {step} job: {self._ws.config.host}#job/{job_id}")
        job_run_waiter = self._ws.jobs.run_now(job_id)
        try:
            job_run_waiter.result()
        except OperationFailed as err:
            # currently we don't have any good message from API, so we have to work around it.
            job_run = self._ws.jobs.get_run(job_run_waiter.run_id)
            raise self._infer_error_from_job_run(job_run) from err

    def _infer_error_from_job_run(self, job_run) -> Exception:
        errors: list[Exception] = []
        timeouts: list[DeadlineExceeded] = []
        assert job_run.tasks is not None
        for run_task in job_run.tasks:
            error = self._infer_error_from_task_run(run_task)
            if not error:
                continue
            if isinstance(error, DeadlineExceeded):
                timeouts.append(error)
                continue
            errors.append(error)
        assert job_run.state is not None
        assert job_run.state.state_message is not None
        if len(errors) == 1:
            return errors[0]
        all_errors = errors + timeouts
        if len(all_errors) == 0:
            return Unknown(job_run.state.state_message)
        return ManyError(all_errors)

    def _infer_error_from_task_run(self, run_task: jobs.RunTask) -> Exception | None:
        if not run_task.state:
            return None
        if run_task.state.result_state == jobs.RunResultState.TIMEDOUT:
            msg = f"{run_task.task_key}: The run was stopped after reaching the timeout"
            return DeadlineExceeded(msg)
        if run_task.state.result_state != jobs.RunResultState.FAILED:
            return None
        assert run_task.run_id is not None
        run_output = self._ws.jobs.get_run_output(run_task.run_id)
        if not run_output:
            msg = f'No run output. {run_task.state.state_message}'
            return InternalError(msg)
        if logger.isEnabledFor(logging.DEBUG):
            if run_output.error_trace:
                sys.stderr.write(run_output.error_trace)
        if not run_output.error:
            msg = f'No error in run output. {run_task.state.state_message}'
            return InternalError(msg)
        return self._infer_task_exception(f"{run_task.task_key}: {run_output.error}")

    @staticmethod
    def _infer_task_exception(haystack: str) -> Exception:
        needles = [
            BadRequest,
            Unauthenticated,
            PermissionDenied,
            NotFound,
            ResourceConflict,
            TooManyRequests,
            Cancelled,
            InternalError,
            NotImplemented,
            TemporarilyUnavailable,
            DeadlineExceeded,
            InvalidParameterValue,
            ResourceDoesNotExist,
            Aborted,
            AlreadyExists,
            ResourceAlreadyExists,
            ResourceExhausted,
            RequestLimitExceeded,
            Unknown,
            DataLoss,
            ValueError,
            KeyError,
        ]
        constructors: dict[re.Pattern, type[Exception]] = {
            re.compile(r".*\[TABLE_OR_VIEW_NOT_FOUND] (.*)"): NotFound,
            re.compile(r".*\[SCHEMA_NOT_FOUND] (.*)"): NotFound,
        }
        for klass in needles:
            constructors[re.compile(f".*{klass.__name__}: (.*)")] = klass
        for pattern, klass in constructors.items():
            match = pattern.match(haystack)
            if match:
                return klass(match.group(1))
        return Unknown(haystack)

    @property
    def _warehouse_id(self) -> str:
        if self._config.warehouse_id is not None:
            logger.info("Fetching warehouse_id from a config")
            return self._config.warehouse_id
        warehouses = [_ for _ in self._ws.warehouses.list() if _.warehouse_type == EndpointInfoWarehouseType.PRO]
        warehouse_id = self._config.warehouse_id
        if not warehouse_id and not warehouses:
            msg = "need either configured warehouse_id or an existing PRO SQL warehouse"
            raise ValueError(msg)
        if not warehouse_id:
            warehouse_id = warehouses[0].id
        self._config.warehouse_id = warehouse_id
        return warehouse_id

    @property
    def _my_username(self):
        if not hasattr(self, "_me"):
            self._me = self._ws.current_user.me()
            is_workspace_admin = any(g.display == "admins" for g in self._me.groups)
            if not is_workspace_admin:
                msg = "Current user is not a workspace admin"
                raise PermissionError(msg)
        return self._me.user_name

    @property
    def _short_name(self):
        if "@" in self._my_username:
            username = self._my_username.split("@")[0]
        else:
            username = self._me.display_name
        return username

    @property
    def _config_file(self):
        return f"{self._installation.install_folder()}/config.yml"

    def _name(self, name: str) -> str:
        prefix = os.path.basename(self._installation.install_folder()).removeprefix('.')
        return f"[{prefix.upper()}] {name}"

    def _upload_wheel(self):
        with self._wheels:
            try:
                self._wheels.upload_to_dbfs()
            except PermissionDenied as err:
                if not self._prompts:
                    raise RuntimeWarning("no Prompts instance found") from err
                logger.warning(f"Uploading wheel file to DBFS failed, DBFS is probably write protected. {err}")
                configure_cluster_overrides = ConfigureClusterOverrides(self._ws, self._prompts.choice_from_dict)
                self._config.override_clusters = configure_cluster_overrides.configure()
                self._installation.save(self._config)
            return self._wheels.upload_to_wsfs()

    def _upload_cluster_policy(self, remote_wheel: str):
        try:
            if self.config.policy_id is None:
                msg = "Cluster policy not present, please uninstall and reinstall ucx completely."
                raise InvalidParameterValue(msg)
            policy = self._ws.cluster_policies.get(policy_id=self.config.policy_id)
        except NotFound as err:
            msg = f"UCX Policy {self.config.policy_id} not found, please reinstall UCX"
            logger.error(msg)
            raise NotFound(msg) from err
        if policy.name is not None:
            self._ws.cluster_policies.edit(
                policy_id=self.config.policy_id,
                name=policy.name,
                definition=policy.definition,
                libraries=[compute.Library(whl=f"dbfs:{remote_wheel}")],
            )

    def create_jobs(self):
        logger.debug(f"Creating jobs from tasks in {main.__name__}")
        remote_wheel = self._upload_wheel()
        self._upload_cluster_policy(remote_wheel)
        desired_steps = {t.workflow for t in _TASKS.values() if t.cloud_compatible(self._ws.config)}
        wheel_runner = None

        if self._config.override_clusters:
            wheel_runner = self._upload_wheel_runner(remote_wheel)
        for step_name in desired_steps:
            settings = self._job_settings(step_name)
            if self._config.override_clusters:
                settings = self._apply_cluster_overrides(settings, self._config.override_clusters, wheel_runner)
            self._deploy_workflow(step_name, settings)

        for step_name, job_id in self._state.jobs.items():
            if step_name not in desired_steps:
                try:
                    logger.info(f"Removing job_id={job_id}, as it is no longer needed")
                    self._ws.jobs.delete(job_id)
                except InvalidParameterValue:
                    logger.warning(f"step={step_name} does not exist anymore for some reason")
                    continue

        self._state.save()
        self._create_debug(remote_wheel)

    def _deploy_workflow(self, step_name: str, settings):
        if step_name in self._state.jobs:
            try:
                job_id = int(self._state.jobs[step_name])
                logger.info(f"Updating configuration for step={step_name} job_id={job_id}")
                return self._ws.jobs.reset(job_id, jobs.JobSettings(**settings))
            except InvalidParameterValue:
                del self._state.jobs[step_name]
                logger.warning(f"step={step_name} does not exist anymore for some reason")
                return self._deploy_workflow(step_name, settings)
        logger.info(f"Creating new job configuration for step={step_name}")
        new_job = self._ws.jobs.create(**settings)
        assert new_job.job_id is not None
        self._state.jobs[step_name] = str(new_job.job_id)
        return None

    @staticmethod
    def _sorted_tasks() -> list[Task]:
        return sorted(_TASKS.values(), key=lambda x: x.task_id)

    @classmethod
    def _step_list(cls) -> list[str]:
        step_list = []
        for task in cls._sorted_tasks():
            if task.workflow not in step_list:
                step_list.append(task.workflow)
        return step_list

    def _create_readme(self) -> str:
        debug_notebook_link = self._installation.workspace_markdown_link('debug notebook', 'DEBUG.py')
        markdown = [
            "# UCX - The Unity Catalog Migration Assistant",
            f'To troubleshoot, see {debug_notebook_link}.\n',
            "Here are the URLs and descriptions of workflows that trigger various stages of migration.",
            "All jobs are defined with necessary cluster configurations and DBR versions.\n",
        ]
        for step_name in self._step_list():
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
            for task in self._sorted_tasks():
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

    def _create_debug(self, remote_wheel: str):
        readme_link = self._installation.workspace_link('README')
        job_links = ", ".join(
            f"[{self._name(step_name)}]({self._ws.config.host}#job/{job_id})"
            for step_name, job_id in self._state.jobs.items()
        )
        content = DEBUG_NOTEBOOK.format(
            remote_wheel=remote_wheel, readme_link=readme_link, job_links=job_links, config_file=self._config_file
        ).encode("utf8")
        self._installation.upload('DEBUG.py', content)

    def _job_settings(self, step_name: str):
        email_notifications = None
        if not self._config.override_clusters and "@" in self._my_username:
            # set email notifications only if we're running the real
            # installation and not the integration test.
            email_notifications = jobs.JobEmailNotifications(
                on_success=[self._my_username], on_failure=[self._my_username]
            )
        tasks = sorted(
            [t for t in _TASKS.values() if t.workflow == step_name],
            key=lambda _: _.name,
        )
        version = PRODUCT_INFO.version()
        version = version if not self._ws.config.is_gcp else version.replace("+", "-")
        return {
            "name": self._name(step_name),
            "tags": {"version": f"v{version}"},
            "job_clusters": self._job_clusters({t.job_cluster for t in tasks}),
            "email_notifications": email_notifications,
            "tasks": [self._job_task(task) for task in tasks],
        }

    def _upload_wheel_runner(self, remote_wheel: str):
        # TODO: we have to be doing this workaround until ES-897453 is solved in the platform
        code = TEST_RUNNER_NOTEBOOK.format(remote_wheel=remote_wheel, config_file=self._config_file).encode("utf8")
        return self._installation.upload(f"wheels/wheel-test-runner-{PRODUCT_INFO.version()}.py", code)

    @staticmethod
    def _apply_cluster_overrides(settings: dict[str, Any], overrides: dict[str, str], wheel_runner: str) -> dict:
        settings["job_clusters"] = [_ for _ in settings["job_clusters"] if _.job_cluster_key not in overrides]
        for job_task in settings["tasks"]:
            if job_task.job_cluster_key is None:
                continue
            if job_task.job_cluster_key in overrides:
                job_task.existing_cluster_id = overrides[job_task.job_cluster_key]
                job_task.job_cluster_key = None
                job_task.libraries = None
            if job_task.python_wheel_task is not None:
                job_task.python_wheel_task = None
                params = {"task": job_task.task_key} | EXTRA_TASK_PARAMS
                job_task.notebook_task = jobs.NotebookTask(notebook_path=wheel_runner, base_parameters=params)
        return settings

    def _job_task(self, task: Task) -> jobs.Task:
        jobs_task = jobs.Task(
            task_key=task.name,
            job_cluster_key=task.job_cluster,
            depends_on=[jobs.TaskDependency(task_key=d) for d in _TASKS[task.name].dependencies()],
        )
        if task.dashboard:
            # dashboards are created in parallel to wheel uploads, so we'll just retry
            retry_on_attribute_error = retried(on=[KeyError], timeout=self._verify_timeout)
            retried_job_dashboard_task = retry_on_attribute_error(self._job_dashboard_task)
            return retried_job_dashboard_task(jobs_task, task)
        if task.notebook:
            return self._job_notebook_task(jobs_task, task)
        return self._job_wheel_task(jobs_task, task)

    def _job_dashboard_task(self, jobs_task: jobs.Task, task: Task) -> jobs.Task:
        assert task.dashboard is not None
        dashboard_id = self._state.dashboards[task.dashboard]
        return replace(
            jobs_task,
            job_cluster_key=None,
            sql_task=jobs.SqlTask(
                warehouse_id=self._warehouse_id,
                dashboard=jobs.SqlTaskDashboard(dashboard_id=dashboard_id),
            ),
        )

    def _job_notebook_task(self, jobs_task: jobs.Task, task: Task) -> jobs.Task:
        assert task.notebook is not None
        local_notebook = self._this_file.parent / task.notebook
        with local_notebook.open("rb") as f:
            remote_notebook = self._installation.upload(local_notebook.name, f.read())
        return replace(
            jobs_task,
            notebook_task=jobs.NotebookTask(
                notebook_path=remote_notebook,
                # ES-872211: currently, we cannot read WSFS files from Scala context
                base_parameters={
                    "task": task.name,
                    "config": f"/Workspace{self._config_file}",
                }
                | EXTRA_TASK_PARAMS,
            ),
        )

    def _job_wheel_task(self, jobs_task: jobs.Task, task: Task) -> jobs.Task:
        return replace(
            jobs_task,
            # TODO: check when we can install wheels from WSFS properly
            python_wheel_task=jobs.PythonWheelTask(
                package_name="databricks_labs_ucx",
                entry_point="runtime",  # [project.entry-points.databricks] in pyproject.toml
                named_parameters={"task": task.name, "config": f"/Workspace{self._config_file}"} | EXTRA_TASK_PARAMS,
            ),
        )

    def _job_clusters(self, names: set[str]):
        clusters = []
        spark_conf = {
            "spark.databricks.cluster.profile": "singleNode",
            "spark.master": "local[*]",
        }
        if self._config.spark_conf is not None:
            spark_conf = spark_conf | self._config.spark_conf
        spec = compute.ClusterSpec(
            data_security_mode=compute.DataSecurityMode.LEGACY_SINGLE_USER,
            spark_conf=spark_conf,
            custom_tags={"ResourceClass": "SingleNode"},
            num_workers=0,
            policy_id=self.config.policy_id,
        )
        if "main" in names:
            clusters.append(
                jobs.JobCluster(
                    job_cluster_key="main",
                    new_cluster=spec,
                )
            )
        if "tacl" in names:
            clusters.append(
                jobs.JobCluster(
                    job_cluster_key="tacl",
                    new_cluster=replace(
                        spec,
                        data_security_mode=compute.DataSecurityMode.LEGACY_TABLE_ACL,
                        spark_conf={"spark.databricks.acl.sqlOnly": "true"},
                        num_workers=1,  # ShowPermissionsCommand needs a worker
                        custom_tags={},
                    ),
                )
            )
        return clusters

    @staticmethod
    def _readable_timedelta(epoch):
        when = datetime.utcfromtimestamp(epoch)
        duration = datetime.now() - when
        data = {}
        data["days"], remaining = divmod(duration.total_seconds(), 86_400)
        data["hours"], remaining = divmod(remaining, 3_600)
        data["minutes"], data["seconds"] = divmod(remaining, 60)

        time_parts = ((name, round(value)) for name, value in data.items())
        time_parts = [f"{value} {name[:-1] if value == 1 else name}" for name, value in time_parts if value > 0]
        if len(time_parts) > 0:
            time_parts.append("ago")
        if time_parts:
            return " ".join(time_parts)
        return "less than 1 second ago"

    def latest_job_status(self) -> list[dict]:
        latest_status = []
        for step, job_id in self._state.jobs.items():
            job_state = None
            start_time = None
            try:
                job_runs = list(self._ws.jobs.list_runs(job_id=int(job_id), limit=1))
            except InvalidParameterValue as e:
                logger.warning(f"skipping {step}: {e}")
                continue
            if job_runs:
                state = job_runs[0].state
                if state and state.result_state:
                    job_state = state.result_state.name
                elif state and state.life_cycle_state:
                    job_state = state.life_cycle_state.name
                if job_runs[0].start_time:
                    start_time = job_runs[0].start_time / 1000
            latest_status.append(
                {
                    "step": step,
                    "state": "UNKNOWN" if not (job_runs and job_state) else job_state,
                    "started": (
                        "<never run>" if not (job_runs and start_time) else self._readable_timedelta(start_time)
                    ),
                }
            )
        return latest_status

    def _get_result_state(self, job_id):
        job_runs = list(self._ws.jobs.list_runs(job_id=job_id, limit=1))
        latest_job_run = job_runs[0]
        if not latest_job_run.state.result_state:
            raise AttributeError("no result state in job run")
        job_state = latest_job_run.state.result_state.value
        return job_state

    def repair_run(self, workflow):
        try:
            job_id, run_id = self._repair_workflow(workflow)
            run_details = self._ws.jobs.get_run(run_id=run_id, include_history=True)
            latest_repair_run_id = run_details.repair_history[-1].id
            job_url = f"{self._ws.config.host}#job/{job_id}/run/{run_id}"
            logger.debug(f"Repair Running {workflow} job: {job_url}")
            self._ws.jobs.repair_run(run_id=run_id, rerun_all_failed_tasks=True, latest_repair_id=latest_repair_run_id)
            webbrowser.open(job_url)
        except InvalidParameterValue as e:
            logger.warning(f"skipping {workflow}: {e}")
        except TimeoutError:
            logger.warning(f"Skipping the {workflow} due to time out. Please try after sometime")

    def _repair_workflow(self, workflow):
        job_id = self._state.jobs.get(workflow)
        if not job_id:
            raise InvalidParameterValue("job does not exists hence skipping repair")
        job_runs = list(self._ws.jobs.list_runs(job_id=job_id, limit=1))
        if not job_runs:
            raise InvalidParameterValue("job is not initialized yet. Can't trigger repair run now")
        latest_job_run = job_runs[0]
        retry_on_attribute_error = retried(on=[AttributeError], timeout=self._verify_timeout)
        retried_check = retry_on_attribute_error(self._get_result_state)
        state_value = retried_check(job_id)
        logger.info(f"The status for the latest run is {state_value}")
        if state_value != "FAILED":
            raise InvalidParameterValue("job is not in FAILED state hence skipping repair")
        run_id = latest_job_run.run_id
        return job_id, run_id

    def uninstall(self):
        if self._prompts and not self._prompts.confirm(
            "Do you want to uninstall ucx from the workspace too, this would "
            "remove ucx project folder, dashboards, queries and jobs"
        ):
            return
        # TODO: this is incorrect, fetch the remote version (that appeared only in Feb 2024)
        logger.info(f"Deleting UCX v{PRODUCT_INFO.version()} from {self._ws.config.host}")
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
            self.run_workflow(step)


if __name__ == "__main__":
    logger = get_logger(__file__)
    logger.setLevel("INFO")

    workspace_client = WorkspaceClient(product="ucx", product_version=__version__)
    current = Installation(workspace_client, PRODUCT_INFO.product_name())
    installer = WorkspaceInstaller(Prompts(), current, workspace_client)
    installer.run()

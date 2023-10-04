import datetime
import json
import logging
import re
from dataclasses import dataclass
from hashlib import sha256

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import DatabricksError
from databricks.sdk.service.compute import ClusterDetails, ClusterSource
from databricks.sdk.service.jobs import (
    BaseJob,
    BaseRun,
    ClusterSpec,
    JobCluster,
    RunTask,
    RunType,
)

from databricks.labs.ucx.framework.crawlers import CrawlerBase, SqlBackend

logger = logging.getLogger(__name__)

INCOMPATIBLE_SPARK_CONFIG_KEYS = [
    "spark.databricks.passthrough.enabled",
    "spark.hadoop.javax.jdo.option.ConnectionURL",
    "spark.databricks.hive.metastore.glueCatalog.enabled",
]

_AZURE_SP_CONF = [
    "fs.azure.account.key",
    "fs.azure.account.auth.type",
    "fs.azure.account.oauth.provider.type",
    "fs.azure.account.oauth2.client.id",
    "fs.azure.account.oauth2.client.secret",
    "fs.azure.account.oauth2.client.endpoint",
]

_AZURE_SP_CONF_FAILURE_MSG = "Uses azure service principal credentials config in "


@dataclass
class JobInfo:
    job_id: str
    job_name: str
    creator: str
    success: int
    failures: str


@dataclass
class ClusterInfo:
    cluster_id: str
    cluster_name: str
    creator: str
    success: int
    failures: str


@dataclass
class PipelineInfo:
    pipeline_id: str
    pipeline_name: str
    creator_name: str
    success: int
    failures: str


@dataclass
class ExternallyOrchestratedJobTask:
    run_id: int
    task_key: str
    run_type: str
    hashed_id: str
    spark_version: str
    data_security_mode: str


def _azure_sp_conf_present_check(config: dict) -> bool:
    for key in config.keys():
        for conf in _AZURE_SP_CONF:
            if re.search(conf, key):
                return True
    return False


def spark_version_compatibility(spark_version: str) -> str:
    first_comp_custom_rt = 3
    first_comp_custom_x = 2
    dbr_version_components = spark_version.split("-")
    first_components = dbr_version_components[0].split(".")
    if len(first_components) != first_comp_custom_rt:
        # custom runtime
        return "unsupported"
    if first_components[first_comp_custom_x] != "x":
        # custom runtime
        return "unsupported"
    version = int(first_components[0]), int(first_components[1])
    if version < (10, 0):
        return "unsupported"
    if (10, 0) <= version < (11, 3):
        return "kinda works"
    return "supported"


class PipelinesCrawler(CrawlerBase):
    def __init__(self, ws: WorkspaceClient, sbe: SqlBackend, schema):
        super().__init__(sbe, "hive_metastore", schema, "pipelines", PipelineInfo)
        self._ws = ws

    def _crawl(self) -> list[PipelineInfo]:
        all_pipelines = list(self._ws.pipelines.list_pipelines())
        return list(self._assess_pipelines(all_pipelines))

    def _assess_pipelines(self, all_pipelines):
        for pipeline in all_pipelines:
            pipeline_info = PipelineInfo(pipeline.pipeline_id, pipeline.name, pipeline.creator_user_name, 1, "")
            failures = []
            pipeline_config = self._ws.pipelines.get(pipeline.pipeline_id).spec.configuration
            if pipeline_config:
                if _azure_sp_conf_present_check(pipeline_config):
                    failures.append(f"{_AZURE_SP_CONF_FAILURE_MSG} pipeline.")

            pipeline_info.failures = json.dumps(failures)
            if len(failures) > 0:
                pipeline_info.success = 0
            yield pipeline_info

    def snapshot(self) -> list[PipelineInfo]:
        return self._snapshot(self._try_fetch, self._crawl)

    def _try_fetch(self) -> list[PipelineInfo]:
        for row in self._fetch(f"SELECT * FROM {self._schema}.{self._table}"):
            yield PipelineInfo(*row)


class ClustersCrawler(CrawlerBase):
    def __init__(self, ws: WorkspaceClient, sbe: SqlBackend, schema):
        super().__init__(sbe, "hive_metastore", schema, "clusters", ClusterInfo)
        self._ws = ws

    def _crawl(self) -> list[ClusterInfo]:
        all_clusters = list(self._ws.clusters.list())
        return list(self._assess_clusters(all_clusters))

    def _assess_clusters(self, all_clusters):
        for cluster in all_clusters:
            if cluster.cluster_source == ClusterSource.JOB:
                continue
            cluster_info = ClusterInfo(cluster.cluster_id, cluster.cluster_name, cluster.creator_user_name, 1, "")
            support_status = spark_version_compatibility(cluster.spark_version)
            failures = []
            if support_status != "supported":
                failures.append(f"not supported DBR: {cluster.spark_version}")

            if cluster.spark_conf is not None:
                for k in INCOMPATIBLE_SPARK_CONFIG_KEYS:
                    if k in cluster.spark_conf:
                        failures.append(f"unsupported config: {k}")

                for value in cluster.spark_conf.values():
                    if "dbfs:/mnt" in value or "/dbfs/mnt" in value:
                        failures.append(f"using DBFS mount in configuration: {value}")

                # Checking if Azure cluster config is present in spark config
                if _azure_sp_conf_present_check(cluster.spark_conf):
                    failures.append(f"{_AZURE_SP_CONF_FAILURE_MSG} cluster.")

            # Checking if Azure cluster config is present in cluster policies
            if cluster.policy_id:
                try:
                    policy = self._ws.cluster_policies.get(cluster.policy_id)
                    if _azure_sp_conf_present_check(json.loads(policy.definition)):
                        failures.append(f"{_AZURE_SP_CONF_FAILURE_MSG} cluster.")
                    if policy.policy_family_definition_overrides:
                        if _azure_sp_conf_present_check(json.loads(policy.policy_family_definition_overrides)):
                            failures.append(f"{_AZURE_SP_CONF_FAILURE_MSG} cluster.")
                except DatabricksError as err:
                    logger.warning(f"Error retrieving cluster policy {cluster.policy_id}. Error: {err}")

            cluster_info.failures = json.dumps(failures)
            if len(failures) > 0:
                cluster_info.success = 0
            yield cluster_info

    def snapshot(self) -> list[ClusterInfo]:
        return self._snapshot(self._try_fetch, self._crawl)

    def _try_fetch(self) -> list[ClusterInfo]:
        for row in self._fetch(f"SELECT * FROM {self._schema}.{self._table}"):
            yield ClusterInfo(*row)


class JobsCrawler(CrawlerBase):
    def __init__(self, ws: WorkspaceClient, sbe: SqlBackend, schema):
        super().__init__(sbe, "hive_metastore", schema, "jobs", JobInfo)
        self._ws = ws

    @staticmethod
    def _get_cluster_configs_from_all_jobs(all_jobs, all_clusters_by_id):
        for j in all_jobs:
            if j.settings.job_clusters is not None:
                for jc in j.settings.job_clusters:
                    if jc.new_cluster is None:
                        continue
                    yield j, jc.new_cluster

            for t in j.settings.tasks:
                if t.existing_cluster_id is not None:
                    interactive_cluster = all_clusters_by_id.get(t.existing_cluster_id, None)
                    if interactive_cluster is None:
                        continue
                    yield j, interactive_cluster

                elif t.new_cluster is not None:
                    yield j, t.new_cluster

    def _crawl(self) -> list[JobInfo]:
        all_jobs = list(self._ws.jobs.list(expand_tasks=True))
        all_clusters = {c.cluster_id: c for c in self._ws.clusters.list()}
        return self._assess_jobs(all_jobs, all_clusters)

    def _assess_jobs(self, all_jobs: list[BaseJob], all_clusters_by_id) -> list[JobInfo]:
        job_assessment = {}
        job_details = {}
        for job in all_jobs:
            job_assessment[job.job_id] = set()
            job_details[job.job_id] = JobInfo(str(job.job_id), job.settings.name, job.creator_user_name, 1, "")

        for job, cluster_config in self._get_cluster_configs_from_all_jobs(all_jobs, all_clusters_by_id):
            support_status = spark_version_compatibility(cluster_config.spark_version)
            if support_status != "supported":
                job_assessment[job.job_id].add(f"not supported DBR: {cluster_config.spark_version}")

            if cluster_config.spark_conf is not None:
                for k in INCOMPATIBLE_SPARK_CONFIG_KEYS:
                    if k in cluster_config.spark_conf:
                        job_assessment[job.job_id].add(f"unsupported config: {k}")

                for value in cluster_config.spark_conf.values():
                    if "dbfs:/mnt" in value or "/dbfs/mnt" in value:
                        job_assessment[job.job_id].add(f"using DBFS mount in configuration: {value}")

                # Checking if Azure cluster config is present in spark config
                if _azure_sp_conf_present_check(cluster_config.spark_conf):
                    job_assessment[job.job_id].add(f"{_AZURE_SP_CONF_FAILURE_MSG} Job cluster.")

            # Checking if Azure cluster config is present in cluster policies
            if cluster_config.policy_id:
                try:
                    policy = self._ws.cluster_policies.get(cluster_config.policy_id)
                    if _azure_sp_conf_present_check(json.loads(policy.definition)):
                        job_assessment[job.job_id].add(f"{_AZURE_SP_CONF_FAILURE_MSG} Job cluster.")
                    if policy.policy_family_definition_overrides:
                        if _azure_sp_conf_present_check(json.loads(policy.policy_family_definition_overrides)):
                            job_assessment[job.job_id].add(f"{_AZURE_SP_CONF_FAILURE_MSG} Job cluster.")
                except DatabricksError as err:
                    logger.warning(f"Error retrieving cluster policy {cluster_config.policy_id}. Error: {err}")

        for job_key in job_details.keys():
            job_details[job_key].failures = json.dumps(list(job_assessment[job_key]))
            if len(job_assessment[job_key]) > 0:
                job_details[job_key].success = 0
        return list(job_details.values())

    def snapshot(self) -> list[ClusterInfo]:
        return self._snapshot(self._try_fetch, self._crawl)

    def _try_fetch(self) -> list[ClusterInfo]:
        for row in self._fetch(f"SELECT * FROM {self._schema}.{self._table}"):
            yield JobInfo(*row)


class ExternallyOrchestratedJobCrawler(CrawlerBase):
    def __init__(self, ws: WorkspaceClient, sbe: SqlBackend, schema):
        super().__init__(sbe, "hive_metastore", schema, "job_runs")
        self._ws = ws

    def _crawl(self) -> list[ExternallyOrchestratedJobTask]:
        no_of_days_back = datetime.timedelta(days=30)  # todo make configurable in yaml?
        start_time_from = datetime.datetime.now() - no_of_days_back
        # todo figure out if we need to specify a default timezone
        all_job_runs = list(
            self._ws.jobs.list_runs(
                expand_tasks=True,
                start_time_from=start_time_from,
                start_time_to=datetime.datetime.now(),
                run_type=RunType.SUBMIT_RUN,
            )
        )
        all_jobs = list(self._ws.jobs.list())
        all_clusters: dict[str, ClusterDetails] = {c.cluster_id: c for c in self._ws.clusters.list()}
        return self._assess_job_runs(all_clusters, all_job_runs, all_jobs)

    def _create_hash_from_job_run_task(self, task: RunTask) -> str:
        """
        Check all hashable attributes, append to a singular list, remove None's,
        pipe separate and hash the value to provide a uniqueness identifier
        - specifically ignore parameters as these change.
        """
        hash_values = []
        if task.notebook_task is not None:
            hash_values.append(task.notebook_task.notebook_path)
        if task.spark_python_task is not None:
            hash_values.append(task.spark_python_task.python_file)
        if task.spark_submit_task is not None:
            hash_values.append(task.spark_submit_task.parameters)
        if task.spark_jar_task is not None:
            hash_values.append(task.spark_jar_task.jar_uri)
            hash_values.append(task.spark_jar_task.main_class_name)
        if task.pipeline_task is not None:
            hash_values.append(task.pipeline_task.pipeline_id)
        if task.python_wheel_task is not None:
            hash_values.append(task.python_wheel_task.package_name)
            hash_values.append(task.python_wheel_task.entry_point)
        if task.sql_task is not None:
            hash_values.append(task.sql_task.file.path)
            hash_values.append(task.sql_task.alert.alert_id)
            hash_values.append(task.sql_task.dashboard.dashboard_id)
            hash_values.append(task.sql_task.query.query_id)
        if task.dbt_task is not None:
            task.dbt_task.commands.sort()
            hash_values.append(task.dbt_task.schema)
            hash_values.append(task.dbt_task.catalog)
            hash_values.append(task.dbt_task.warehouse_id)
            hash_values.append(task.dbt_task.project_directory)
            hash_values.append(",".join(task.dbt_task.commands))
        if task.condition_task is not None:
            hash_values.append(task.condition_task.op.value)
            hash_values.append(task.condition_task.right)
            hash_values.append(task.condition_task.left)
            hash_values.append(task.condition_task.outcome)
        if task.run_job_task is not None:
            hash_values.append(task.run_job_task.job_id)
        if task.git_source is not None:
            hash_values.append(task.git_source.git_url)
            hash_values.append(task.git_source.git_tag)
            hash_values.append(task.git_source.git_branch)
            hash_values.append(task.git_source.git_commit)
            hash_values.append(task.git_source.git_provider)
            hash_values.append(task.git_source.git_snapshot.used_commit)

        hash_value_string = "|".join([value for value in hash_values if value is not None])
        return sha256(bytes(hash_value_string.encode("utf-8"))).hexdigest()

    def _get_cluster_from_task(
        self, task: RunTask, job_run: BaseRun, all_clusters: dict[str, ClusterDetails]
    ) -> ClusterSpec | ClusterDetails:
        """
        Determine the cluster associated with the task
        1) Look for new_cluster on the task
        2) Look for existing cluster on the task
        3) Look for cluster_instance on the task (filled when the task is run)
        4) Look for cluster instance on the job (filled when the job is run)
        """
        if task.new_cluster is not None:
            return task.new_cluster
        elif task.existing_cluster_id is not None:
            # from api docs If existing_cluster_id, the ID of an existing cluster that is used for all runs of this job.
            job_clusters: dict[str, JobCluster] = {x.job_cluster_key: x for x in job_run.job_clusters}
            return job_clusters[task.existing_cluster_id].new_cluster
        elif task.cluster_instance is not None:
            # fall back option 1
            return all_clusters[task.cluster_instance.cluster_id]
        elif job_run.cluster_instance is not None:
            # fall back option 2
            return all_clusters[job_run.cluster_instance.cluster_id]

    def _get_spark_version_from_task(
        self, task: RunTask, job_run: BaseRun, all_clusters: dict[str, ClusterDetails]
    ) -> str:
        """
        Returns the spark version of the task cluster
        """
        return self._get_cluster_from_task(task, job_run, all_clusters).spark_version

    def _get_data_security_mode_from_task(
        self, task: RunTask, job_run: BaseRun, all_clusters: dict[str, ClusterDetails]
    ) -> str:
        """
        Returns the security mode of the task cluster
        """
        data_security_mode = self._get_cluster_from_task(task, job_run, all_clusters).data_security_mode
        return data_security_mode.value if data_security_mode is not None else None

    def _assess_job_runs(
        self, all_clusters: dict[str, ClusterDetails], all_job_runs: list[BaseRun], all_jobs: list[BaseJob]
    ) -> list[ExternallyOrchestratedJobTask]:
        """
        Returns a list of ExternallyOrchestratedJobs
        """
        ext_orc_job_tasks: list[ExternallyOrchestratedJobTask] = list[ExternallyOrchestratedJobTask]()
        all_persisted_job_ids = [x.job_id for x in all_jobs]
        not_persisted_job_runs = list(
            filter(lambda jr: jr.job_id is None or jr.job_id not in all_persisted_job_ids, all_job_runs)
        )
        for job_run in not_persisted_job_runs:
            for task in job_run.tasks:
                spark_version = self._get_spark_version_from_task(task, job_run, all_clusters)
                data_security_mode = self._get_data_security_mode_from_task(task, job_run, all_clusters)
                hashed_id = self._create_hash_from_job_run_task(task)
                ext_orc_job_task = ExternallyOrchestratedJobTask(
                    run_id=job_run.run_id,
                    task_key=task.task_key,
                    hashed_id=hashed_id,
                    run_type=str(job_run.run_type.value),
                    spark_version=spark_version,
                    data_security_mode=data_security_mode,
                )
                ext_orc_job_tasks.append(ext_orc_job_task)
        return ext_orc_job_tasks

    def snapshot(self) -> list[ExternallyOrchestratedJobTask]:
        return self._snapshot(self._try_fetch, self._crawl)

    def _try_fetch(self) -> list[ExternallyOrchestratedJobTask]:
        for row in self._fetch(f"SELECT * FROM {self._schema}.{self._table}"):
            yield ExternallyOrchestratedJobTask(*row)

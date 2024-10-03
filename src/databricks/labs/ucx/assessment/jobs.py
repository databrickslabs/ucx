import json
import logging
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from hashlib import sha256

from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import compute
from databricks.sdk.service.compute import ClusterDetails
from databricks.sdk.service.jobs import (
    BaseJob,
    BaseRun,
    DbtTask,
    GitSource,
    PythonWheelTask,
    RunConditionTask,
    RunTask,
    RunType,
    SparkJarTask,
    SqlTask,
)

from databricks.labs.ucx.assessment.clusters import CheckClusterMixin
from databricks.labs.ucx.assessment.crawlers import spark_version_compatibility
from databricks.labs.ucx.framework.crawlers import CrawlerBase
from databricks.labs.ucx.framework.utils import escape_sql_identifier

logger = logging.getLogger(__name__)


@dataclass
class JobInfo:
    job_id: str
    success: int
    failures: str
    job_name: str | None = None
    creator: str | None = None


class JobsMixin:
    @classmethod
    def _get_cluster_configs_from_all_jobs(cls, all_jobs, all_clusters_by_id):
        for job in all_jobs:
            if job.settings is None:
                continue
            if job.settings.job_clusters is not None:
                yield from cls._job_clusters(job)
            if job.settings.tasks is None:
                continue
            yield from cls._task_clusters(job, all_clusters_by_id)

    @classmethod
    def _task_clusters(cls, job, all_clusters_by_id):
        for task in job.settings.tasks:
            if task.existing_cluster_id is not None:
                interactive_cluster = all_clusters_by_id.get(task.existing_cluster_id, None)
                if interactive_cluster is None:
                    continue
                yield job, interactive_cluster
            elif task.new_cluster is not None:
                yield job, task.new_cluster

    @staticmethod
    def _job_clusters(job):
        for job_cluster in job.settings.job_clusters:
            if job_cluster.new_cluster is None:
                continue
            yield job, job_cluster.new_cluster


class JobsCrawler(CrawlerBase[JobInfo], JobsMixin, CheckClusterMixin):
    def __init__(self, ws: WorkspaceClient, sbe: SqlBackend, schema):
        super().__init__(sbe, "hive_metastore", schema, "jobs", JobInfo)
        self._ws = ws

    def _crawl(self) -> Iterable[JobInfo]:
        all_jobs = list(self._ws.jobs.list(expand_tasks=True))
        all_clusters = {c.cluster_id: c for c in self._ws.clusters.list()}
        return self._assess_jobs(all_jobs, all_clusters)

    def _assess_jobs(self, all_jobs: list[BaseJob], all_clusters_by_id) -> Iterable[JobInfo]:
        job_assessment, job_details = self._prepare(all_jobs)
        for job, cluster_config in self._get_cluster_configs_from_all_jobs(all_jobs, all_clusters_by_id):
            job_id = job.job_id
            if not job_id:
                continue
            cluster_details = ClusterDetails.from_dict(cluster_config.as_dict())
            cluster_failures = self._check_cluster_failures(cluster_details, "Job cluster")
            cluster_failures.extend(self._check_jar_task(job.settings.tasks))
            job_assessment[job_id].update(cluster_failures)

        # TODO: next person looking at this - rewrite, as this code makes no sense
        for job_key in job_details.keys():  # pylint: disable=consider-using-dict-items,consider-iterating-dictionary
            job_details[job_key].failures = json.dumps(list(job_assessment[job_key]))
            if len(job_assessment[job_key]) > 0:
                job_details[job_key].success = 0
        return list(job_details.values())

    @staticmethod
    def _prepare(all_jobs) -> tuple[dict[int, set[str]], dict[int, JobInfo]]:
        job_assessment: dict[int, set[str]] = {}
        job_details: dict[int, JobInfo] = {}
        for job in all_jobs:
            if not job.job_id:
                continue
            job_assessment[job.job_id] = set()
            if not job.creator_user_name:
                logger.warning(
                    f"Job {job.job_id} have Unknown creator, it means that the original creator has been deleted "
                    f"and should be re-created"
                )

            job_settings = job.settings
            if not job_settings:
                continue
            job_name = job_settings.name
            if not job_name:
                job_name = "Unknown"

            job_details[job.job_id] = JobInfo(
                job_id=str(job.job_id),
                job_name=job_name,
                creator=job.creator_user_name,
                success=1,
                failures="[]",
            )
        return job_assessment, job_details

    def _try_fetch(self) -> Iterable[JobInfo]:
        for row in self._fetch(f"SELECT * FROM {escape_sql_identifier(self.full_name)}"):
            yield JobInfo(*row)

    def _check_jar_task(self, all_task: list[RunTask]) -> list[str]:
        task_failures: list[str] = []
        for task in all_task:
            if task.spark_jar_task:
                task_failures.append(f"task {task.task_key} is a jar task")
        return task_failures


@dataclass
class SubmitRunInfo:
    run_ids: str  # JSON-encoded list of run ids
    hashed_id: str  # a pseudo id that combines all the hashable attributes of the run
    failures: str = "[]"  # JSON-encoded list of failures


class SubmitRunsCrawler(CrawlerBase[SubmitRunInfo], JobsMixin, CheckClusterMixin):
    _FS_LEVEL_CONF_SETTING_PATTERNS = [
        "fs.s3a",
        "fs.s3n",
        "fs.s3",
        "fs.azure",
        "fs.wasb",
        "fs.abfs",
        "fs.adl",
    ]

    def __init__(self, ws: WorkspaceClient, sbe: SqlBackend, schema: str, num_days_history: int):
        super().__init__(sbe, "hive_metastore", schema, "submit_runs", SubmitRunInfo)
        self._ws = ws
        self._num_days_history = num_days_history

    @staticmethod
    def _dt_to_ms(date_time: datetime):
        return int(date_time.timestamp() * 1000)

    @staticmethod
    def _get_current_dttm() -> datetime:
        return datetime.now(timezone.utc)

    def _crawl(self) -> Iterable[SubmitRunInfo]:
        end = self._dt_to_ms(self._get_current_dttm())
        start = self._dt_to_ms(self._get_current_dttm() - timedelta(days=self._num_days_history))
        submit_runs = self._ws.jobs.list_runs(
            expand_tasks=True,
            completed_only=True,
            run_type=RunType.SUBMIT_RUN,
            start_time_from=start,
            start_time_to=end,
        )
        all_clusters = {c.cluster_id: c for c in self._ws.clusters.list()}
        return self._assess_job_runs(submit_runs, all_clusters)

    def _try_fetch(self) -> Iterable[SubmitRunInfo]:
        for row in self._fetch(f"SELECT * FROM {escape_sql_identifier(self.full_name)}"):
            yield SubmitRunInfo(*row)

    def _check_spark_conf(self, conf: dict[str, str], source: str) -> list[str]:
        failures: list[str] = []
        for key in conf.keys():
            if any(pattern in key for pattern in self._FS_LEVEL_CONF_SETTING_PATTERNS):
                failures.append(f"Potentially unsupported config property: {key}")

        failures.extend(super()._check_spark_conf(conf, source))
        return failures

    def _check_cluster_failures(self, cluster: ClusterDetails, source: str) -> list[str]:
        failures: list[str] = []
        if cluster.aws_attributes and cluster.aws_attributes.instance_profile_arn:
            failures.append(f"using instance profile: {cluster.aws_attributes.instance_profile_arn}")

        failures.extend(super()._check_cluster_failures(cluster, source))
        return failures

    @staticmethod
    def _needs_compatibility_check(spec: compute.ClusterSpec) -> bool:
        """
        # we recognize a task as a potentially incompatible one if:
        # 1. cluster is not configured with data security mode
        # 2. cluster's DBR version is greater than 11.3
        """
        if not spec.data_security_mode:
            compatibility = spark_version_compatibility(spec.spark_version)
            return compatibility == "supported"
        return False

    def _get_hash_from_run(self, run: BaseRun) -> str:
        hashable_items = []
        all_tasks: list[RunTask] = run.tasks if run.tasks is not None else []
        for task in sorted(all_tasks, key=lambda x: x.task_key if x.task_key is not None else ""):
            hashable_items.extend(self._run_task_values(task))

        if run.git_source:
            hashable_items.extend(self._git_source_values(run.git_source))

        return sha256(bytes("|".join(hashable_items).encode("utf-8"))).hexdigest()

    @classmethod
    def _sql_task_values(cls, task: SqlTask) -> list[str]:
        hash_values = [
            task.file.path if task.file else None,
            task.alert.alert_id if task.alert else None,
            task.dashboard.dashboard_id if task.dashboard else None,
            task.query.query_id if task.query else None,
        ]
        return [str(value) for value in hash_values if value is not None]

    @classmethod
    def _git_source_values(cls, source: GitSource) -> list[str]:
        hash_values = [source.git_url]
        return [str(value) for value in hash_values if value is not None]

    @classmethod
    def _dbt_task_values(cls, dbt_task: DbtTask) -> list[str]:
        hash_values = [
            dbt_task.schema,
            dbt_task.catalog,
            dbt_task.warehouse_id,
            dbt_task.project_directory,
            ",".join(sorted(dbt_task.commands)),
        ]
        return [str(value) for value in hash_values if value is not None]

    @classmethod
    def _jar_task_values(cls, spark_jar_task: SparkJarTask) -> list[str]:
        hash_values = [spark_jar_task.jar_uri, spark_jar_task.main_class_name]
        return [str(value) for value in hash_values if value is not None]

    @classmethod
    def _python_wheel_task_values(cls, pw_task: PythonWheelTask) -> list[str]:
        hash_values = [pw_task.package_name, pw_task.entry_point]
        return [str(value) for value in hash_values if value is not None]

    @classmethod
    def _run_condition_task_values(cls, c_task: RunConditionTask) -> list[str]:
        hash_values = [c_task.op.value if c_task.op else None, c_task.right, c_task.left, c_task.outcome]
        return [str(value) for value in hash_values if value is not None]

    @classmethod
    def _run_task_values(cls, task: RunTask) -> list[str]:
        """
        Retrieve all hashable attributes and append to a list with None removed
        - specifically ignore parameters as these change.
        """
        hash_values = [
            task.notebook_task.notebook_path if task.notebook_task else None,
            task.spark_python_task.python_file if task.spark_python_task else None,
            (
                '|'.join(task.spark_submit_task.parameters)
                if (task.spark_submit_task and task.spark_submit_task.parameters)
                else None
            ),
            task.pipeline_task.pipeline_id if task.pipeline_task is not None else None,
            task.run_job_task.job_id if task.run_job_task else None,
        ]
        hash_lists = [
            cls._jar_task_values(task.spark_jar_task) if task.spark_jar_task else None,
            (cls._python_wheel_task_values(task.python_wheel_task) if (task.python_wheel_task) else None),
            cls._sql_task_values(task.sql_task) if task.sql_task else None,
            cls._dbt_task_values(task.dbt_task) if task.dbt_task else None,
            cls._run_condition_task_values(task.condition_task) if task.condition_task else None,
            cls._git_source_values(task.git_source) if task.git_source else None,
        ]
        # combining all the values from the lists where the list is not "None"
        hash_values_from_lists = sum([hash_list for hash_list in hash_lists if hash_list], [])
        return [str(value) for value in hash_values + hash_values_from_lists]

    def _assess_job_runs(self, submit_runs: Iterable[BaseRun], all_clusters_by_id) -> Iterable[SubmitRunInfo]:
        """
        Assessment logic:
        1. For eaxch submit run, we analyze all tasks inside this run.
        2. Per each task, we calculate a unique hash based on the _retrieve_hash_values_from_task function
        3. Then we coalesce all task hashes into a single hash for the submit run
        4. Coalesce all runs under the same hash into a single pseudo-job
        5. Return a list of pseudo-jobs with their assessment results
        """
        result: dict[str, SubmitRunInfo] = {}
        runs_per_hash: dict[str, list[int | None]] = {}

        for submit_run in submit_runs:
            task_failures: list[str] = []
            # v2.1+ API, with tasks
            if submit_run.tasks:
                self._check_run_task(submit_run.tasks, all_clusters_by_id, task_failures)
            # v2.0 API, without tasks
            elif submit_run.cluster_spec:
                _cluster_details = ClusterDetails.from_dict(submit_run.cluster_spec.as_dict())
                task_failures.extend(self._check_cluster_failures(_cluster_details, "root_task"))
            hashed_id = self._get_hash_from_run(submit_run)
            if hashed_id in runs_per_hash:
                runs_per_hash[hashed_id].append(submit_run.run_id)
            else:
                runs_per_hash[hashed_id] = [submit_run.run_id]
            result[hashed_id] = SubmitRunInfo(
                run_ids=json.dumps(runs_per_hash[hashed_id]),
                hashed_id=hashed_id,
                failures=json.dumps(list(set(task_failures))),
            )

        return list(result.values())

    def _check_run_task(self, all_tasks: list[RunTask], clusters: dict[str, ClusterDetails], task_failures: list[str]):
        for task in sorted(all_tasks, key=lambda x: x.task_key if x.task_key is not None else ""):
            _task_key = task.task_key if task.task_key is not None else ""
            cluster_details = None
            if task.new_cluster:
                cluster_details = ClusterDetails.from_dict(task.new_cluster.as_dict())
                if self._needs_compatibility_check(task.new_cluster):
                    task_failures.append("no data security mode specified")
            if task.existing_cluster_id:
                cluster_details = clusters.get(task.existing_cluster_id, None)
            if cluster_details:
                task_failures.extend(self._check_cluster_failures(cluster_details, _task_key))
            if task.spark_jar_task:
                task_failures.append(f"task {task.task_key} is a jar task")

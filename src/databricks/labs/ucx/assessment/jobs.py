import json
import logging
from collections.abc import Iterable
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import ClusterDetails
from databricks.sdk.service.jobs import BaseJob

from databricks.labs.ucx.assessment.clusters import CheckClusterMixin
from databricks.labs.ucx.framework.crawlers import CrawlerBase, SqlBackend

logger = logging.getLogger(__name__)


@dataclass
class JobInfo:
    job_id: str
    success: int
    failures: str
    job_name: str | None = None
    creator: str | None = None


class JobsMixin:
    @staticmethod
    def _get_cluster_configs_from_all_jobs(all_jobs, all_clusters_by_id):  # pylint: disable=too-complex
        for j in all_jobs:
            if j.settings is None:
                continue
            if j.settings.job_clusters is not None:
                for job_cluster in j.settings.job_clusters:
                    if job_cluster.new_cluster is None:
                        continue
                    yield j, job_cluster.new_cluster
            if j.settings.tasks is None:
                continue
            for task in j.settings.tasks:
                if task.existing_cluster_id is not None:
                    interactive_cluster = all_clusters_by_id.get(task.existing_cluster_id, None)
                    if interactive_cluster is None:
                        continue
                    yield j, interactive_cluster

                elif task.new_cluster is not None:
                    yield j, task.new_cluster


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
            cluster_failures = self.check_cluster_failures(cluster_details, "Job cluster")
            job_assessment[job_id].update(cluster_failures)

        # TODO: next person looking at this - rewrite, as this code makes no sense
        for job_key in job_details.keys():  # pylint: disable=consider-using-dict-items,consider-iterating-dictionary
            job_details[job_key].failures = json.dumps(list(job_assessment[job_key]))
            if len(job_assessment[job_key]) > 0:
                job_details[job_key].success = 0
        return list(job_details.values())

    @staticmethod
    def _prepare(all_jobs):
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

    def snapshot(self) -> Iterable[JobInfo]:
        return self._snapshot(self._try_fetch, self._crawl)

    def _try_fetch(self) -> Iterable[JobInfo]:
        for row in self._fetch(f"SELECT * FROM {self._schema}.{self._table}"):
            yield JobInfo(*row)

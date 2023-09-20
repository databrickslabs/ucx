import json
from dataclasses import dataclass
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import BaseJob

from databricks.labs.ucx.framework.crawlers import CrawlerBase, StatementExecutionBackend
from databricks.labs.ucx.hive_metastore.data_objects import ExternalLocationCrawler
from databricks.labs.ucx.hive_metastore.table_acls import (
    SqlBackend
)


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


def spark_version_compatibility(spark_version: str) -> str:
    dbr_version_components = spark_version.split('-')
    first_components = dbr_version_components[0].split('.')
    if len(first_components) != 3:
        # custom runtime
        return 'unsupported'
    if first_components[2] != 'x':
        # custom runtime
        return 'unsupported'
    version = int(first_components[0]), int(first_components[1])
    if version < (10, 0):
        return 'unsupported'
    if (10, 0) <= version < (11, 3):
        return 'kinda works'
    return 'supported'


class AssessmentToolkit:
    incompatible_spark_config_keys = {
        'spark.databricks.passthrough.enabled',
        'spark.hadoop.javax.jdo.option.ConnectionURL',
        'spark.databricks.hive.metastore.glueCatalog.enabled'
    }

    def __init__(self, ws: WorkspaceClient, inventory_schema, backend=None):
        self._all_jobs = None
        self._all_clusters_by_id = None
        self._ws = ws
        self._inventory_schema = inventory_schema
        self._backend = backend
        self._external_locations = None

    @staticmethod
    def _verify_ws_client(w: WorkspaceClient):
        _me = w.current_user.me()
        is_workspace_admin = any(g.display == "admins" for g in _me.groups)
        if not is_workspace_admin:
            msg = "Current user is not a workspace admin"
            raise RuntimeError(msg)

    def generate_ext_loc_list(self):
        crawler = ExternalLocationCrawler(self._ws, self._backend, self._inventory_schema)
        return crawler.snapshot()

    def generate_job_assessment(self):
        crawler = JobsCrawler(self._ws, self._backend, self._inventory_schema)
        return crawler.snapshot()

    def generate_cluster_assessment(self):
        crawler = ClustersCrawler(self._ws, self._backend, self._inventory_schema)
        return crawler.snapshot()


class ClustersCrawler(CrawlerBase):

    def __init__(self, ws: WorkspaceClient, sbe: SqlBackend, schema):
        super().__init__(sbe, "hive_metastore", schema, "clusters")
        self._ws = ws

    def _crawl(self) -> list[ClusterInfo]:
        all_clusters = list(self._ws.clusters.list())
        return list(self._assess_clusters(all_clusters))

    def _assess_clusters(self, all_clusters):
        for cluster in all_clusters:
            cluster_info = ClusterInfo(cluster.cluster_id, cluster.cluster_name, cluster.creator_user_name, 1, "")
            support_status = spark_version_compatibility(cluster.spark_version)
            failures = []
            if support_status != 'supported':
                failures.append(f'not supported DBR: {cluster.spark_version}')

            if cluster.spark_conf is not None:
                for k in AssessmentToolkit.incompatible_spark_config_keys:
                    if k in cluster.spark_conf:
                        failures.append(f'unsupported config: {k}')

                for value in cluster.spark_conf.values():
                    if 'dbfs:/mnt' in value or '/dbfs/mnt' in value:
                        failures.append(f'using DBFS mount in configuration: {value}')
            cluster_info.failures = json.dumps(failures)
            if len(failures) > 0:
                cluster_info.success = 0
            yield cluster_info

    def snapshot(self) -> list[ClusterInfo]:
        return self._snapshot(self._try_fetch, self._crawl)

    def _try_fetch(self) -> list[ClusterInfo]:
        for row in self._fetch(
                f'SELECT * FROM {self._schema}.{self._table}'
        ):
            yield ClusterInfo(*row)


class JobsCrawler(CrawlerBase):

    def __init__(self, ws: WorkspaceClient, sbe: SqlBackend, schema):
        super().__init__(sbe, "hive_metastore", schema, "jobs")
        self._ws = ws

    def _get_cluster_configs_from_all_jobs(self, all_jobs, all_clusters_by_id):
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
        incompatible_spark_config_keys = {
            'spark.databricks.passthrough.enabled',
            'spark.hadoop.javax.jdo.option.ConnectionURL',
            'spark.databricks.hive.metastore.glueCatalog.enabled'
        }
        job_assessment = {}
        job_details = {}
        for job in all_jobs:
            job_assessment[job.job_id] = set()
            job_details[job.job_id] = JobInfo(str(job.job_id), job.settings.name, job.creator_user_name, 1, "")

        for job, cluster_config in \
                self._get_cluster_configs_from_all_jobs(all_jobs, all_clusters_by_id):
            support_status = spark_version_compatibility(cluster_config.spark_version)
            if support_status != 'supported':
                job_assessment[job.job_id].add(f'not supported DBR: {cluster_config.spark_version}')

            if cluster_config.spark_conf is not None:
                for k in incompatible_spark_config_keys:
                    if k in cluster_config.spark_conf:
                        job_assessment[job.job_id].add(f'unsupported config: {k}')

                for value in cluster_config.spark_conf.values():
                    if 'dbfs:/mnt' in value or '/dbfs/mnt' in value:
                        job_assessment[job.job_id].add(f'using DBFS mount in configuration: {value}')
        for job_key in job_details.keys():
            job_details[job_key].failures = json.dumps(list(job_assessment[job_key]))
            if len(job_assessment[job_key]) > 0:
                job_details[job_key].success = 0
        return list(job_details.values())

    def snapshot(self) -> list[ClusterInfo]:
        return self._snapshot(self._try_fetch, self._crawl)

    def _try_fetch(self) -> list[ClusterInfo]:
        for row in self._fetch(
                f'SELECT * FROM {self._schema}.{self._table}'
        ):
            yield JobInfo(*row)


if __name__ == "__main__":
    print("Databricks UC Assessment")
    ws = WorkspaceClient(cluster_id="0919-184725-qa0q5jkc")
    #
    assess = AssessmentToolkit(ws, "ucx", StatementExecutionBackend(ws, "cdae2fd48f8d4841"))
    print(assess.generate_cluster_assessment())
    # tables = StatementExecutionBackend(ws, "cdae2fd48f8d4841").fetch(
    #     f"SELECT location FROM ucx.tables WHERE location IS NOT NULL")

    # print(list(tables))

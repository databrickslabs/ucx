import concurrent.futures
import os
import os.path
import json
import functools
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import Language, ClusterDetails

from databricks.labs.ucx.framework.crawlers import CrawlerBase
from databricks.labs.ucx.hive_metastore.tables import Table
from databricks.labs.ucx.hive_metastore.table_acls import (
    RuntimeBackend,
    SqlBackend,
    StatementExecutionBackend,
)
from databricks.labs.ucx.mixins.sql import StatementExecutionExt


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
class ExtLoc:
    location: str

@dataclass
class Mount:
    name: str
    source: str
    instance_profile: str


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

    def __init__(self, ws: WorkspaceClient, inventory_catalog, inventory_schema, backend=None):
        self._all_jobs = None
        self._all_clusters_by_id = None
        self._ws = ws
        self._inventory_catalog = inventory_catalog
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
        crawler = ExternalLocationCrawler(self._backend, ws, self._inventory_catalog,
                                          self._inventory_schema)
        return crawler.snapshot()

    def generate_job_assessment(self):
        crawler = JobsCrawler(self._backend, ws, "hive_metastore",
                              self._inventory_catalog)
        return crawler.snapshot()

    def generate_cluster_assessment(self):
        crawler = ClustersCrawler(self._backend, ws, "hive_metastore",
                                  self._inventory_catalog)
        return crawler.snapshot()


    # @staticmethod
    # def _backend(ws: WorkspaceClient, warehouse_id: str | None = None) -> SqlBackend:
    #     if warehouse_id is None:
    #         return RuntimeBackend()
    #     return StatementExecutionBackend(ws, warehouse_id)


class InventoryTableCrawler(CrawlerBase):

    def __init__(self, sbe: SqlBackend, catalog, schema):
        super().__init__(sbe, catalog, schema, "tables")

    def snapshot(self) -> list[Table]:
        return self._snapshot(self._try_fetch, self._mock_loader)

    def _mock_loader(self):
        return None

    def _try_fetch(self) -> list[Table]:
        for row in self._fetch(
            f'SELECT * FROM {self._schema}.{self._table}'
        ):
            yield Table(*row)


class ExternalLocationCrawler(CrawlerBase):

    def __init__(self, sbe: SqlBackend, ws: WorkspaceClient, catalog, schema):
        super().__init__(sbe, catalog, schema, "external_locations")
        self._ws = ws

    def _external_locations(self, tables: [ExtLoc]):
        mounts = MountsCrawler(self._backend, self._ws, self._schema).snapshot()
        ext_locations = []
        for table in tables:
            location = table.location
            if location is not None and len(location) > 0:
                if location.startswith("dbfs:/mnt"):
                    for mount in mounts:
                        if location[5:0].startswith(mount.name):
                            location = location.replace(mount.name,mount.source)
                            break
                if not location.startswith("dbfs"):
                    dupe = False
                    loc = 0
                    while loc < len(ext_locations) and not dupe:
                        common = os.path.commonprefix([ext_locations[loc].location, os.path.dirname(location) + '/'])
                        if common.count("/") > 2:
                            ext_locations[loc] = ExtLoc(common)
                            dupe = True
                        loc += 1
                    if not dupe:
                        ext_locations.append(ExtLoc(os.path.dirname(location) + '/'))
        return ext_locations

    def _ext_loc_list(self):
        crawler = InventoryTableCrawler(self._backend, self._catalog, self._schema)
        table_list = crawler.snapshot()
        return self._external_locations(table_list)

    def snapshot(self) -> list[ExtLoc]:
        return self._snapshot(self._try_fetch, self._ext_loc_list)

    def _try_fetch(self) -> list[ExtLoc]:
        for row in self._fetch(
            f'SELECT * FROM {self._schema}.{self._table}'
        ):
            yield ExtLoc(*row)


class MountsCrawler(CrawlerBase):
    def __init__(self, backend: SqlBackend, ws: WorkspaceClient, inventory_database: str):
        super().__init__(backend, "hive_metastore", inventory_database, "mounts")
        self._dbutils = ws.dbutils

    def inventorize_mounts(self):
        self._append_records(self._list_mounts())

    def _list_mounts(self):
        mounts = []
        for mount_point, source, _ in self._dbutils.fs.mounts():
            mounts.append(Mount(mount_point, source))
        return mounts

    def snapshot(self) -> list[Mount]:
        return self._snapshot(self._try_fetch, self._list_mounts)

    def _try_fetch(self) -> list[Mount]:
        for row in self._fetch(
            f'SELECT * FROM {self._schema}.{self._table}'
        ):
            yield Mount(*row)


class ClustersCrawler(CrawlerBase):

    def __init__(self, sbe: SqlBackend, ws: WorkspaceClient, catalog, schema):
        super().__init__(sbe, catalog, schema, "clusters")
        self._ws = ws

    def _crawl(self) -> list[ClusterInfo]:
        all_clusters = list(self._ws.clusters.list())
        for cluster in all_clusters:
            cluster_info = ClusterInfo(cluster.cluster_id, cluster.cluster_name, cluster.creator_user_name, 1, [])
            support_status = spark_version_compatibility(cluster.spark_version)
            failures = []
            if support_status != 'supported':
                failures.append(f'not supported DBR: {cluster.spark_version}')

            if cluster.spark_conf is not None:
                for k in AssessmentToolkit.incompatible_spark_config_keys:
                    if k in cluster.spark_conf:
                        using_incompatible_config = True
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
            yield ExtLoc(*row)


class JobsCrawler(CrawlerBase):

    def __init__(self, sbe: SqlBackend, ws: WorkspaceClient, catalog, schema):
        super().__init__(sbe, catalog, schema, "jobs")
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
                self._get_cluster_configs_from_all_jobs(all_jobs, all_clusters):
            support_status = spark_version_compatibility(cluster_config.spark_version)
            if support_status != 'supported':
                job_assessment[job.job_id].add(f'not supported DBR: {cluster_config.spark_version}')

            if cluster_config.spark_conf is not None:
                for k in incompatible_spark_config_keys:
                    if k in cluster_config.spark_conf:
                        using_incompatible_config = True
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
    ws = WorkspaceClient(cluster_id="0919-184725-qa0q5jkc")

    assess = AssessmentToolkit(ws, "hive_metastore", "ucx", StatementExecutionBackend(ws, "2e39d99c480ac668"))
    print(assess.generate_ext_loc_list())



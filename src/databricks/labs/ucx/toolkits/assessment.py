import concurrent.futures
import os
import os.path
import json
import functools

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import Language
from databricks.labs.ucx.tacl.tables import Table
from databricks.labs.ucx.providers.mixins.compute import CommandExecutor
from databricks.labs.ucx.tacl._internal import (
    RuntimeBackend,
    SqlBackend,
    StatementExecutionBackend,
)


class AssessmentToolkit:
    def __init__(self, ws: WorkspaceClient, cluster_id, inventory_catalog, inventory_schema, warehouse_id=None):
        self._all_jobs = None
        self._all_clusters_by_id = None
        self._ws = ws
        self._inventory_catalog = inventory_catalog
        self._inventory_schema = inventory_schema
        self._warehouse_id = warehouse_id
        self._cluster_id = cluster_id
        self._external_locations = None
        self._incompatible_spark_config_keys = {
            'spark.databricks.passthrough.enabled',
            'spark.hadoop.javax.jdo.option.ConnectionURL',
            'spark.databricks.hive.metastore.glueCatalog.enabled'
        }

    @staticmethod
    def _verify_ws_client(w: WorkspaceClient):
        _me = w.current_user.me()
        is_workspace_admin = any(g.display == "admins" for g in _me.groups)
        if not is_workspace_admin:
            msg = "Current user is not a workspace admin"
            raise RuntimeError(msg)

    def table_inventory(self):
        commands = CommandExecutor(self._ws, language=Language.SCALA, cluster_id=self._cluster_id)

        from importlib import resources as impresources
        from databricks.labs.ucx.assessment import scala

        inp_file = (impresources.files(scala) / 'assessment.scala')
        with inp_file.open("rt") as f:
            template = f.read()
        setup_code = f"""
        val schema="{self._inventory_schema}";
        """
        command_output = commands.run(setup_code + template)
        print(command_output)

    @staticmethod
    def external_locations(tables: [Table]):
        ext_locations = []
        for table in tables:
            dupe = False
            loc = 0
            while loc < len(ext_locations) and not dupe:
                common = os.path.commonprefix([ext_locations[loc], os.path.dirname(table.location) + '/'])
                if common.count("/") > 2:
                    ext_locations[loc] = common
                    dupe = True
                loc += 1
            if not dupe:
                ext_locations.append((os.path.dirname(table.location) + '/'))
        return ext_locations

    def retrieve_jobs(self):
        return list(self._ws.jobs.list(expand_tasks=True))

    def retrieve_clusters(self):
        return {c.cluster_id: c for c in self._ws.clusters.list()}

    @staticmethod
    def get_cluster_configs_from_all_jobs(all_jobs, all_clusters_by_id):
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

    def generate_job_assessment(self):
        return AssessmentToolkit.parse_jobs(self.retrieve_jobs(), self.retrieve_clusters())

    @staticmethod
    def parse_jobs(all_jobs, all_clusters):
        incompatible_spark_config_keys = {
            'spark.databricks.passthrough.enabled',
            'spark.hadoop.javax.jdo.option.ConnectionURL',
            'spark.databricks.hive.metastore.glueCatalog.enabled'
        }
        job_assessment = {}
        for job in all_jobs:
            job_assessment[job.job_id] = set()

        for job, cluster_config in \
                AssessmentToolkit.get_cluster_configs_from_all_jobs(all_jobs, all_clusters):
            support_status = AssessmentToolkit.spark_version_compatibility(cluster_config.spark_version)
            if support_status != 'supported':
                job_assessment[job.job_id].add(f'not supported DBR: {cluster_config.spark_version}')

            if cluster_config.spark_conf is not None:
                for k in incompatible_spark_config_keys:
                    if k in cluster_config.spark_conf:
                        using_incompatible_config = True
                        job_assessment[job.job_id].add(f'unsupported config: {k}')

                for value in cluster_config.spark_conf.values():
                    if 'dbfs:/mnt' in value or '/dbfs/mnt' in value:
                        job_assessment[job.job_id].add(f'using DBFS mount in configu: {k}={value}')
        return job_assessment
    @staticmethod
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

    @staticmethod
    def _backend(ws: WorkspaceClient, warehouse_id: str | None = None) -> SqlBackend:
        if warehouse_id is None:
            return RuntimeBackend()
        return StatementExecutionBackend(ws, warehouse_id)


if __name__ == "__main__":
    ws = WorkspaceClient()
    cluster_id = os.getenv("CLUSTER_ID")
    print(cluster_id)
    assess = AssessmentToolkit(ws, cluster_id, "UCX", "UCX_assessment")
    # assess.table_inventory()
    print(assess.generate_job_assessment())

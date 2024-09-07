import json
import logging
from collections.abc import Iterable
from dataclasses import dataclass

from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.assessment.clusters import CheckClusterMixin
from databricks.labs.ucx.framework.crawlers import CrawlerBase
from databricks.labs.ucx.framework.utils import escape_sql_identifier

logger = logging.getLogger(__name__)


@dataclass
class PipelineInfo:
    pipeline_id: str
    success: int
    failures: str
    pipeline_name: str | None = None
    creator_name: str | None = None


class PipelinesCrawler(CrawlerBase[PipelineInfo], CheckClusterMixin):
    def __init__(self, ws: WorkspaceClient, sbe: SqlBackend, schema):
        super().__init__(sbe, "hive_metastore", schema, "pipelines", PipelineInfo)
        self._ws = ws

    def _crawl(self) -> Iterable[PipelineInfo]:
        all_pipelines = list(self._ws.pipelines.list_pipelines())
        return list(self._assess_pipelines(all_pipelines))

    def _assess_pipelines(self, all_pipelines) -> Iterable[PipelineInfo]:
        for pipeline in all_pipelines:
            if not pipeline.creator_user_name:
                logger.warning(
                    f"Pipeline {pipeline.name} have Unknown creator, it means that the original creator "
                    f"has been deleted and should be re-created"
                )
            pipeline_info = PipelineInfo(
                pipeline_id=pipeline.pipeline_id,
                pipeline_name=pipeline.name,
                creator_name=pipeline.creator_user_name,
                success=1,
                failures="[]",
            )

            failures = []
            pipeline_response = self._ws.pipelines.get(pipeline.pipeline_id)
            assert pipeline_response.spec is not None
            pipeline_config = pipeline_response.spec.configuration
            if pipeline_config:
                failures.extend(self._check_spark_conf(pipeline_config, "pipeline"))
            clusters = pipeline_response.spec.clusters
            if clusters:
                self._pipeline_clusters(clusters, failures)
            pipeline_info.failures = json.dumps(failures)
            if len(failures) > 0:
                pipeline_info.success = 0
            yield pipeline_info

    def _pipeline_clusters(self, clusters, failures):
        for cluster in clusters:
            if cluster.spark_conf:
                failures.extend(self._check_spark_conf(cluster.spark_conf, "pipeline cluster"))
            # Checking if cluster config is present in cluster policies
            if cluster.policy_id:
                failures.extend(self._check_cluster_policy(cluster.policy_id, "pipeline cluster"))
            if cluster.init_scripts:
                failures.extend(self._check_cluster_init_script(cluster.init_scripts, "pipeline cluster"))

    def _try_fetch(self) -> Iterable[PipelineInfo]:
        for row in self._fetch(f"SELECT * FROM {escape_sql_identifier(self.full_name)}"):
            yield PipelineInfo(*row)

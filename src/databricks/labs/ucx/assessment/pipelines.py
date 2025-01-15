import json
import logging
from collections.abc import Iterable
from dataclasses import dataclass
from typing import ClassVar

from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound

from databricks.labs.ucx.assessment.clusters import CheckClusterMixin
from databricks.labs.ucx.framework.crawlers import CrawlerBase
from databricks.labs.ucx.framework.owners import Ownership
from databricks.labs.ucx.framework.utils import escape_sql_identifier

logger = logging.getLogger(__name__)


@dataclass
class PipelineInfo:
    pipeline_id: str
    success: int
    failures: str
    pipeline_name: str | None = None
    creator_name: str | None = None
    """User-name of the creator of the pipeline, if known."""

    __id_attributes__: ClassVar[tuple[str, ...]] = ("pipeline_id",)


class PipelinesCrawler(CrawlerBase[PipelineInfo], CheckClusterMixin):
    def __init__(
        self, ws: WorkspaceClient, sql_backend: SqlBackend, schema, include_pipeline_ids: list[str] | None = None
    ):
        super().__init__(sql_backend, "hive_metastore", schema, "pipelines", PipelineInfo)
        self._ws = ws
        self._include_pipeline_ids = include_pipeline_ids

    def _crawl(self) -> Iterable[PipelineInfo]:

        pipeline_ids = []
        if self._include_pipeline_ids is not None:
            pipeline_ids = self._include_pipeline_ids
        else:
            pipeline_ids = [p.pipeline_id for p in self._ws.pipelines.list_pipelines() if p.pipeline_id]

        for pipeline_id in pipeline_ids:
            try:
                pipeline = self._ws.pipelines.get(pipeline_id)
                assert pipeline.pipeline_id is not None
                assert pipeline.spec is not None
            except NotFound:
                logger.warning(f"Pipeline not found: {pipeline_id}")
                continue

            creator_name = pipeline.creator_user_name or None
            if not creator_name:
                logger.warning(
                    f"Pipeline {pipeline.name} have Unknown creator, it means that the original creator "
                    f"has been deleted and should be re-created"
                )
            pipeline_config = pipeline.spec.configuration
            failures = []
            if pipeline_config:
                failures.extend(self._check_spark_conf(pipeline_config, "pipeline"))
            clusters = pipeline.spec.clusters
            if clusters:
                self._pipeline_clusters(clusters, failures)
            failures_as_json = json.dumps(failures)
            yield PipelineInfo(
                pipeline_id=pipeline.pipeline_id,
                pipeline_name=pipeline.name,
                creator_name=creator_name,
                success=int(not failures),
                failures=failures_as_json,
            )

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


class PipelineOwnership(Ownership[PipelineInfo]):
    """Determine ownership of pipelines in the inventory.

    This is the pipeline creator (if known).
    """

    def _maybe_direct_owner(self, record: PipelineInfo) -> str | None:
        return record.creator_name

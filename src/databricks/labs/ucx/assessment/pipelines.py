import json
from collections.abc import Iterable
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.assessment.crawlers import (
    _AZURE_SP_CONF_FAILURE_MSG,
    _azure_sp_conf_present_check,
    logger,
)
from databricks.labs.ucx.framework.crawlers import CrawlerBase, SqlBackend


@dataclass
class PipelineInfo:
    pipeline_id: str
    success: int
    failures: str
    pipeline_name: str | None = None
    creator_name: str | None = None


class PipelinesCrawler(CrawlerBase[PipelineInfo]):
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
                if _azure_sp_conf_present_check(pipeline_config):
                    failures.append(f"{_AZURE_SP_CONF_FAILURE_MSG} pipeline.")

            pipeline_info.failures = json.dumps(failures)
            if len(failures) > 0:
                pipeline_info.success = 0
            yield pipeline_info

    def snapshot(self) -> Iterable[PipelineInfo]:
        return self._snapshot(self._try_fetch, self._crawl)

    def _try_fetch(self) -> Iterable[PipelineInfo]:
        for row in self._fetch(f"SELECT * FROM {self._schema}.{self._table}"):
            yield PipelineInfo(*row)

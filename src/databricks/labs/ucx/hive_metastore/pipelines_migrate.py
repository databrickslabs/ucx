import dataclasses
import logging
from dataclasses import dataclass

from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.assessment.pipelines import PipelinesCrawler

logger = logging.getLogger(__name__)

@dataclass
class Rule:
    workspace_name: str
    source_pipeline_id: str
    source_pipeline_name: str
    target_schema_name: str
    target_pipeline_name: str


class PipelinesMigrator:
    def __init__(self,
                 pipeline_crawler: PipelinesCrawler,
                 ws: WorkspaceClient):
        self._pc = pipeline_crawler
        self._ws = ws


    def migrate_pipelines(self):
        pipelines = self._pc.snapshot()
        logger.info(f"Found {len(pipelines)} pipelines to migrate")


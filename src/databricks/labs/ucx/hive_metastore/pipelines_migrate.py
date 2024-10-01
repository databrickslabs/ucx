import dataclasses
import logging

from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.assessment.pipelines import PipelinesCrawler

logger = logging.getLogger(__name__)

class PipelinesMigrator:
    def __init__(self,
                 pipeline_crawler: PipelinesCrawler,
                 ws: WorkspaceClient):
        self._pc = pipeline_crawler
        self._ws = ws


    def migrate_pipelines(self):
        pipelines = self._pc.snapshot()
        logger.info(f"Found {len(pipelines)} pipelines to migrate")


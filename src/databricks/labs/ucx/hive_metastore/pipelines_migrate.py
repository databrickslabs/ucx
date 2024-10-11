import logging
from dataclasses import dataclass
from functools import partial

from databricks.labs.blueprint.parallel import Threads
from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.marketplace import Installation

from databricks.labs.ucx.assessment.pipelines import PipelinesCrawler, PipelineInfo

logger = logging.getLogger(__name__)

@dataclass
class PipelineRule:
    workspace_name: str
    source_pipeline_id: str
    source_pipeline_name: str
    target_schema_name: str
    target_pipeline_name: str

@dataclass
class PipelineToMigrate:
    src: PipelineInfo
    rule: PipelineRule

    def __hash__(self):
        return hash(self.src)

    def __eq__(self, other):
        return isinstance(other, PipelineToMigrate) and self.src == other.src

class PipelineMapping:
    FILENAME = "pipeline_mapping.csv"

    def __init__(self,
                 installation: Installation,
                 ws: WorkspaceClient,
                 sql_backend: SqlBackend,
                 ):
        self._installation = installation
        self._ws = ws
        self._sql_backend = sql_backend

    def current_pipelines(self,
                          pipelines: PipelinesCrawler,
                          workspace_name: str,
                          catalog_name: str):
        pipeline_snapshot = list(pipelines.snapshot())
        if not pipeline_snapshot:
            msg = "No pipelines found."
            raise ValueError(msg)
        for pipelines in pipeline_snapshot:
            yield PipelineRule.initial()

        return self._pc.snapshot()

    def get_pipelines_to_migrate(self, _pc):
        pass


class PipelinesMigrator:
    def __init__(self,
                 ws: WorkspaceClient,
                 pipeline_crawler: PipelinesCrawler,
                 pipeline_mapping: PipelineMapping):
        self._ws = ws
        self._pc = pipeline_crawler
        self._pm = pipeline_mapping

    def migrate_pipelines(self):
        self._migrate_pipelines()

    def _migrate_pipelines(self):
        pipelines = self._pc.snapshot()
        logger.info(f"Found {len(pipelines)} pipelines to migrate")

        # get pipelines to migrate
        pipelines_to_migrate = self._pm.get_pipelines_to_migrate(self._pc)

        tasks = []
        for pipeline in pipelines_to_migrate:
            tasks.append(partial(self._migrate_pipeline, pipeline))
        Threads.strict("migrate pipelines", tasks)
        if not tasks:
            logger.info(f"No pipelines found to migrate")
        return tasks

    def _migrate_pipeline(self, pipeline: PipelineToMigrate):
        pass


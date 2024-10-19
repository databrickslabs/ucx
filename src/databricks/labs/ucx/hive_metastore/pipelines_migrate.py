import logging
from dataclasses import dataclass
from functools import partial

from databricks.labs.blueprint.parallel import Threads
from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service.marketplace import Installation

from databricks.labs.ucx.assessment.pipelines import PipelinesCrawler, PipelineInfo

logger = logging.getLogger(__name__)


@dataclass
class PipelineRule:
    workspace_name: str
    src_pipeline_id: str
    target_catalog_name: str
    target_schema_name: str | None
    target_pipeline_name: str | None

    @classmethod
    def from_src_dst(
        cls,
        workspace_name: str,
        src_pipeline_id: str,
        target_catalog_name: str,
        target_schema_name: str | None,
        target_pipeline_name: str | None,
    ) -> "PipelineRule":
        return cls(
            workspace_name=workspace_name,
            src_pipeline_id=src_pipeline_id,
            target_catalog_name=target_catalog_name,
            target_schema_name=target_schema_name,
            target_pipeline_name=target_pipeline_name,
        )

    @classmethod
    def initial(cls, workspace_name: str, catalog_name: str, pipeline: PipelineInfo) -> "PipelineRule":
        return cls(
            workspace_name=workspace_name,
            target_catalog_name=catalog_name,
            src_pipeline_id=pipeline.pipeline_id,
            target_pipeline_name=pipeline.pipeline_name,
            target_schema_name=None,
        )


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

    def __init__(
        self,
        installation: Installation,
        ws: WorkspaceClient,
        sql_backend: SqlBackend,
    ):
        self._installation = installation
        self._ws = ws
        self._sql_backend = sql_backend

    def current_pipelines(self, pipelines: PipelinesCrawler, workspace_name: str, catalog_name: str):
        pipeline_snapshot = list(pipelines.snapshot())
        if not pipeline_snapshot:
            msg = "No pipelines found."
            raise ValueError(msg)
        for pipeline in pipeline_snapshot:
            yield PipelineRule.initial(workspace_name, catalog_name, pipeline)

        return self._pc.snapshot()

    def load(self) -> list[PipelineRule]:
        try:
            return self._installation.load(list[PipelineRule], filename=self.FILENAME)
        except NotFound:
            # TODO: add pipeline mapping creation command
            msg = "Please create pipeline mapping file"
            raise ValueError(msg) from None

    def get_pipelines_to_migrate(self, _pc):
        rules = self.load()
        # Getting all the source tables from the rules
        pipelines = list(_pc.snapshot())
        pipelines_to_migrate = []

        for rule in rules:
            for pipeline in pipelines:
                if pipeline.pipeline_id == rule.src_pipeline_id:
                    pipelines_to_migrate.append(PipelineToMigrate(pipeline, rule))

        return pipelines_to_migrate


class PipelinesMigrator:
    def __init__(self, ws: WorkspaceClient, pipeline_crawler: PipelinesCrawler, pipeline_mapping: PipelineMapping):
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
            logger.info("No pipelines found to migrate")
        return tasks

    def _migrate_pipeline(self, pipeline: PipelineToMigrate):
        try:
            self._clone_pipeline(pipeline)
        except Exception as e:
            logger.error(f"Failed to migrate pipeline {pipeline.src.pipeline_id}: {e}")
        pass

    def _clone_pipeline(self, pipeline: PipelineToMigrate):
        # TODO: implement this in sdk
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        body = {
            'catalog': pipeline.rule.target_catalog_name,
            'clone_mode': 'MIGRATE_TO_UC',
            'configuration': {'pipelines.migration.ignoreExplicitPath': 'true'},
        }
        # if pipeline.rule.target_schema_name is not None: body['target'] = pipeline.rule.target_schema_name
        # if pipeline.rule.target_pipeline_name is not None: body['name'] = pipeline.rule.target_pipeline_name
        res = self._ws.api_client.do(
            'POST', f'/api/2.0/pipelines/{pipeline.src.pipeline_id}/clone', body=body, headers=headers
        )
        return res

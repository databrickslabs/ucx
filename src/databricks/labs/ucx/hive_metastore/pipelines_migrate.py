import logging
from collections.abc import Generator
from dataclasses import dataclass, field
from functools import partial
from typing import BinaryIO

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.parallel import Threads
from databricks.labs.lsql.backends import SqlBackend
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError, NotFound

from databricks.labs.ucx.assessment.pipelines import PipelineInfo, PipelinesCrawler

logger = logging.getLogger(__name__)


@dataclass
class PipelineRule:
    workspace_name: str
    src_pipeline_id: str
    target_catalog_name: str | None = None
    target_schema_name: str | None = None
    target_pipeline_name: str | None = None

    @classmethod
    def from_src_dst(
        cls,
        workspace_name: str,
        src_pipeline_id: str,
        target_catalog_name: str | None = None,
        target_schema_name: str | None = None,
        target_pipeline_name: str | None = None,
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
    rule: PipelineRule = field(compare=False)


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

    @staticmethod
    def current_pipelines(pipelines: PipelinesCrawler, workspace_name: str, catalog_name: str) -> Generator:
        pipeline_snapshot = list(pipelines.snapshot(force_refresh=True))
        if not pipeline_snapshot:
            msg = "No pipelines found."
            raise ValueError(msg)
        for pipeline in pipeline_snapshot:
            yield PipelineRule.initial(workspace_name, catalog_name, pipeline)

    def load(self) -> list[PipelineRule]:
        try:
            return self._installation.load(list[PipelineRule], filename=self.FILENAME)
        except NotFound:
            msg = "Please create pipeline mapping file"
            raise ValueError(msg) from None

    def get_pipelines_to_migrate(self, _pc) -> list[PipelineToMigrate]:
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
    def __init__(self, ws: WorkspaceClient, pipelines_crawler: PipelinesCrawler, catalog_name: str,
                 skip_pipelines=None):
        if skip_pipelines is None:
            skip_pipelines = []
        self._ws = ws
        self._pipeline_crawler = pipelines_crawler
        self._catalog_name = catalog_name
        self._skip_pipelines = skip_pipelines

    def get_pipelines_to_migrate(self) -> list[PipelineInfo]:
        # TODO:
        # add skip logic and return only the pipelines that need to be migrated
        return self._pipeline_crawler.snapshot()

    def migrate_pipelines(self) -> None:
        self._migrate_pipelines()

    def _migrate_pipelines(self) -> list[partial[dict | bool | list | BinaryIO]]:
        # get pipelines to migrate
        pipelines_to_migrate = self.get_pipelines_to_migrate()
        logger.info(f"Found {len(pipelines_to_migrate)} pipelines to migrate")

        tasks = []
        for pipeline in pipelines_to_migrate:
            if pipeline.pipeline_id in self._skip_pipelines:
                logger.info(f"Skipping pipeline {pipeline.pipeline_id}")
                continue
            tasks.append(partial(self._migrate_pipeline, pipeline))
        if not tasks:
            return []
        Threads.strict("migrate pipelines", tasks)
        return tasks

    def _migrate_pipeline(self, pipeline: PipelineInfo) -> dict | list | BinaryIO | bool:
        try:
            return self._clone_pipeline(pipeline)
        except DatabricksError as e:
            logger.error(f"Failed to migrate pipeline {pipeline.pipeline_id}: {e}")
            return False

    def _clone_pipeline(self, pipeline: PipelineInfo) -> dict | list | BinaryIO:
        # Stop HMS pipeline
        self._ws.pipelines.stop(pipeline.pipeline_id)
        # Rename old pipeline first

        # Need to get the pipeline again to get the libraries
        # else updating name fails with libraries not provided error
        get_pipeline = self._ws.pipelines.get(pipeline.pipeline_id)
        self._ws.pipelines.update(pipeline.pipeline_id, name=f"{pipeline.pipeline_name} [OLD]",
                                  clusters=get_pipeline.spec.clusters,
                                  storage=get_pipeline.spec.storage,
                                  continuous=get_pipeline.spec.continuous,
                                  deployment=get_pipeline.spec.deployment,
                                  target=get_pipeline.spec.target,
                                  libraries=get_pipeline.spec.libraries)

        # Clone pipeline
        headers = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
        }
        body = {
            'catalog': self._catalog_name,
            'clone_mode': 'MIGRATE_TO_UC',
            'configuration': {'pipelines.migration.ignoreExplicitPath': 'true'},
            'name': f"{pipeline.pipeline_name}",
        }
        res = self._ws.api_client.do(
            'POST', f'/api/2.0/pipelines/{pipeline.pipeline_id}/clone', body=body, headers=headers
        )

        # After successful clone, update jobs

        # TODO:
        # Check the error from UI
        # BAD_REQUEST: Standard_D4pds_v6 is a Graviton instance and is not compatible with runtime dlt:14.1.21-delta-pipelines-dlt-release-2024.41-rc0-commit-f32d838-image-894c190.
        return res

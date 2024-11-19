import logging
from functools import partial
from typing import BinaryIO

from databricks.labs.blueprint.parallel import Threads
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError
from databricks.sdk.service.jobs import PipelineTask, Task, JobSettings

from databricks.labs.ucx.assessment.pipelines import PipelineInfo, PipelinesCrawler

logger = logging.getLogger(__name__)


class PipelinesMigrator:
    def __init__(
        self, ws: WorkspaceClient, pipelines_crawler: PipelinesCrawler, catalog_name: str, skip_pipelines=None
    ):
        if skip_pipelines is None:
            skip_pipelines = []
        self._ws = ws
        self._pipeline_crawler = pipelines_crawler
        self._catalog_name = catalog_name
        self._skip_pipelines = skip_pipelines
        self._pipeline_job_tasks_mapping: dict[str, list[dict]] = {}

    def _populate_pipeline_job_tasks_mapping(self) -> None:
        jobs = self._ws.jobs.list()

        for job in jobs:
            if not job.job_id:
                continue

            job_details = self._ws.jobs.get(job.job_id)
            if not job_details.settings or not job_details.settings.tasks:
                continue

            for task in job_details.settings.tasks:
                if task.pipeline_task:
                    pipeline_id = task.pipeline_task.pipeline_id
                    job_info = {"job_id": job.job_id, "task_key": task.task_key}
                    if pipeline_id not in self._pipeline_job_tasks_mapping:
                        self._pipeline_job_tasks_mapping[pipeline_id] = [job_info]
                    else:
                        self._pipeline_job_tasks_mapping[pipeline_id].append(job_info)
            logger.info(f"Processing job {job.job_id} to find associated pipeline")

    def get_pipelines_to_migrate(self) -> list[PipelineInfo]:
        # TODO:
        # add skip logic and return only the pipelines that need to be migrated
        return list(self._pipeline_crawler.snapshot())

    def migrate_pipelines(self) -> None:
        self._populate_pipeline_job_tasks_mapping()
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
            if "Cloning from Hive Metastore to Unity Catalog is currently not supported" in str(e):
                logger.error(f"{e}: Please contact Databricks to enable DLT HMS to UC migration API on this workspace")
                return False
            logger.error(f"Failed to migrate pipeline {pipeline.pipeline_id}: {e}")
            return False

    def _clone_pipeline(self, pipeline: PipelineInfo) -> dict | list | BinaryIO:
        # Need to get the pipeline again to get the libraries
        # else updating name fails with libraries not provided error
        get_pipeline = self._ws.pipelines.get(pipeline.pipeline_id)
        if get_pipeline.spec:
            if get_pipeline.spec.catalog:
                # Skip if the pipeline is already migrated to UC
                logger.info(f"Pipeline {pipeline.pipeline_id} is already migrated to UC")
                return []

            # Stop HMS pipeline
            self._ws.pipelines.stop(pipeline.pipeline_id)
            # Rename old pipeline first
            self._ws.pipelines.update(
                pipeline.pipeline_id,
                name=f"{pipeline.pipeline_name} [OLD]",
                clusters=get_pipeline.spec.clusters if get_pipeline.spec.clusters else None,
                storage=get_pipeline.spec.storage if get_pipeline.spec.storage else None,
                continuous=get_pipeline.spec.continuous if get_pipeline.spec.continuous else None,
                deployment=get_pipeline.spec.deployment if get_pipeline.spec.deployment else None,
                target=get_pipeline.spec.target if get_pipeline.spec.target else None,
                libraries=get_pipeline.spec.libraries if get_pipeline.spec.libraries else None,
            )

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
        assert isinstance(res, dict)
        if 'pipeline_id' not in res:
            logger.warning(f"Failed to clone pipeline {pipeline.pipeline_id}")
            return res

        # After successful clone, update jobs
        if pipeline.pipeline_id in self._pipeline_job_tasks_mapping:
            for pipeline_job_task_mapping in self._pipeline_job_tasks_mapping[pipeline.pipeline_id]:
                self._ws.jobs.update(
                    pipeline_job_task_mapping['job_id'],
                    new_settings=JobSettings(
                        tasks=[
                            Task(
                                pipeline_task=PipelineTask(pipeline_id=str(res.get('pipeline_id'))),
                                task_key=pipeline_job_task_mapping['task_key'],
                            )
                        ]
                    ),
                )
                logger.info(f"Updated job {pipeline_job_task_mapping['job_id']} with new pipeline {res['pipeline_id']}")

        # TODO:
        # Check the error from UI
        # BAD_REQUEST: Standard_D4pds_v6 is a Graviton instance and is not compatible with runtime dlt:14.1.21-delta-pipelines-dlt-release-2024.41-rc0-commit-f32d838-image-894c190.
        return res

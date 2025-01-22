import logging
from functools import partial
from typing import BinaryIO

from databricks.labs.blueprint.parallel import Threads
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import DatabricksError, NotFound
from databricks.sdk.service.jobs import PipelineTask, Task, JobSettings

from databricks.labs.ucx.assessment.jobs import JobsCrawler
from databricks.labs.ucx.assessment.pipelines import PipelinesCrawler

logger = logging.getLogger(__name__)


class PipelinesMigrator:
    """
    PipelinesMigrator is responsible for migrating pipelines from HMS to UC
    It uses the DLT Migration API to migrate the pipelines and also updates the jobs associated with the pipelines if any
    The class also provides an option to include only some pipelines that are should be migrated

    :param ws: WorkspaceClient
    :param pipelines_crawler: PipelinesCrawler
    :param catalog_name: str
    :param include_pipeline_ids: list[str] | None
    :param exclude_pipeline_ids: list[str] | None
    """

    def __init__(
        self,
        ws: WorkspaceClient,
        pipelines_crawler: PipelinesCrawler,
        jobs_crawler: JobsCrawler,
        catalog_name: str,
        include_pipeline_ids: list[str] | None = None,
        exclude_pipeline_ids: list[str] | None = None,
    ):
        self._ws = ws
        self._pipeline_crawler = pipelines_crawler
        self._jobs_crawler = jobs_crawler
        self._catalog_name = catalog_name
        self._include_pipeline_ids = include_pipeline_ids
        self._exclude_pipeline_ids = exclude_pipeline_ids
        self._pipeline_job_tasks_mapping: dict[str, list[dict]] = {}

    def _populate_pipeline_job_tasks_mapping(self) -> None:
        """
        Populates the pipeline_job_tasks_mapping dictionary with the pipeline_id as key and the list of jobs associated with the pipeline
        """
        jobs = self._jobs_crawler.snapshot()

        for job in jobs:
            if not job.job_id:
                continue

            try:
                job_details = self._ws.jobs.get(int(job.job_id))
            except NotFound:
                logger.warning(f"Skipping non-existing job: {job.job_id}")
                continue
            if not job_details.settings or not job_details.settings.tasks:
                continue

            for task in job_details.settings.tasks:
                if not task.pipeline_task:
                    continue
                pipeline_id = task.pipeline_task.pipeline_id
                job_info = {"job_id": job.job_id, "task_key": task.task_key}
                if pipeline_id not in self._pipeline_job_tasks_mapping:
                    self._pipeline_job_tasks_mapping[pipeline_id] = [job_info]
                else:
                    self._pipeline_job_tasks_mapping[pipeline_id].append(job_info)
                logger.info(f"Found job:{job.job_id} task:{task.task_key} associated with pipeline {pipeline_id}")

    def _get_pipeline_ids_to_migrate(self) -> list[str]:
        """
        Returns the list of pipelines filtered by the include and exclude list
        """
        pipeline_ids_to_migrate = []

        if self._include_pipeline_ids:
            pipeline_ids_to_migrate = self._include_pipeline_ids
        else:
            pipeline_ids_to_migrate = [p.pipeline_id for p in self._pipeline_crawler.snapshot()]

        if self._exclude_pipeline_ids is not None:
            pipeline_ids_to_migrate = list(set(pipeline_ids_to_migrate) - set(self._exclude_pipeline_ids))
        return pipeline_ids_to_migrate

    def migrate_pipelines(self) -> None:
        """
        Migrate the pipelines from HMS to UC. Public method to be called to start the pipeline migration process
        """
        self._populate_pipeline_job_tasks_mapping()
        self._migrate_pipelines()

    def _migrate_pipelines(self) -> list[partial[dict | bool | list | BinaryIO]]:
        """
        Create tasks to parallely migrate the pipelines
        """
        # get pipelines to migrate
        pipelines_to_migrate = self._get_pipeline_ids_to_migrate()
        logger.info(f"Found {len(pipelines_to_migrate)} pipelines to migrate")

        tasks = []
        for pipeline_id in pipelines_to_migrate:
            tasks.append(partial(self._migrate_pipeline, pipeline_id))
        if not tasks:
            return []
        Threads.strict("migrate pipelines", tasks)
        return tasks

    def _migrate_pipeline(self, pipeline_id: str) -> dict | list | BinaryIO | bool:
        """
        Private method to clone the pipeline and handle the exceptions
        """
        try:
            return self._clone_pipeline(pipeline_id)
        except DatabricksError as e:
            if "Cloning from Hive Metastore to Unity Catalog is currently not supported" in str(e):
                logger.error(f"{e}: Please contact Databricks to enable DLT HMS to UC migration API on this workspace")
                return False
            logger.error(f"Failed to migrate pipeline {pipeline_id}: {e}")
            return False

    def _clone_pipeline(self, pipeline_id: str) -> dict | list | BinaryIO:
        """
        This method calls the DLT Migration API to clone the pipeline
        Stop and rename the old pipeline before cloning the new pipeline
        Call the DLT Migration API to clone the pipeline
        Update the jobs associated with the pipeline to point to the new pipeline
        """
        # Need to get the pipeline again to get the libraries
        # else updating name fails with libraries not provided error
        pipeline_info = self._ws.pipelines.get(pipeline_id)
        if pipeline_info.spec and pipeline_info.pipeline_id:
            if pipeline_info.spec.catalog:
                # Skip if the pipeline is already migrated to UC
                logger.info(f"Pipeline {pipeline_info.pipeline_id} is already migrated to UC")
                return []

            # Stop HMS pipeline
            self._ws.pipelines.stop(pipeline_info.pipeline_id)
            # Rename old pipeline first
            self._ws.pipelines.update(
                pipeline_info.pipeline_id,
                name=f"{pipeline_info.name} [OLD]",
                clusters=pipeline_info.spec.clusters if pipeline_info.spec.clusters else None,
                storage=pipeline_info.spec.storage if pipeline_info.spec.storage else None,
                continuous=pipeline_info.spec.continuous if pipeline_info.spec.continuous else None,
                deployment=pipeline_info.spec.deployment if pipeline_info.spec.deployment else None,
                target=pipeline_info.spec.target if pipeline_info.spec.target else None,
                libraries=pipeline_info.spec.libraries if pipeline_info.spec.libraries else None,
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
            'name': f"{pipeline_info.name}",
        }
        res = self._ws.api_client.do(
            'POST', f'/api/2.0/pipelines/{pipeline_info.pipeline_id}/clone', body=body, headers=headers
        )
        assert isinstance(res, dict)
        if 'pipeline_id' not in res:
            logger.warning(f"Failed to clone pipeline {pipeline_info.pipeline_id}")
            return res

        # After successful clone, update jobs
        # Currently there is no SDK method available to migrate the DLT pipelines
        # We are directly using the DLT Migration API in the interim, once the SDK method is available, we can replace this
        if pipeline_info.pipeline_id in self._pipeline_job_tasks_mapping:
            for pipeline_job_task_mapping in self._pipeline_job_tasks_mapping[pipeline_info.pipeline_id]:
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

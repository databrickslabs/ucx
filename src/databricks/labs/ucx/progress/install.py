import datetime as dt
import logging
from dataclasses import dataclass

from databricks.labs.lsql.backends import SqlBackend
from databricks.labs.lsql.deployment import SchemaDeployer

from databricks.labs.ucx.__about__ import __version__
from databricks.labs.ucx.hive_metastore.verification import MetastoreNotFoundError, VerifyHasCatalog, VerifyHasMetastore
from databricks.labs.ucx.installer.workflows import DeployedWorkflows
from databricks.labs.ucx.progress.workflow_runs import WorkflowRun


logger = logging.getLogger(__name__)


@dataclass(frozen=True, kw_only=True)
class Historical:
    workspace_id: int
    """The identifier of the workspace where this historical record was generated."""

    job_run_id: int
    """The identifier of the job run that generated this historical record."""

    object_type: str
    """The inventory table for which this historical record was generated."""

    object_id: list[str]
    """The type-specific identifier for the corresponding inventory record."""

    data: dict[str, str]
    """Type-specific JSON-encoded data of the inventory record."""

    failures: list[str]
    """The list of problems associated with the object that this inventory record covers."""

    owner: str
    """The identity that has ownership of the object."""

    ucx_version: str = __version__
    """The UCX semantic version."""


class ProgressTrackingInstallation:
    """Install resources for UCX's progress tracking."""

    _SCHEMA = "multiworkspace"

    def __init__(self, sql_backend: SqlBackend, ucx_catalog: str) -> None:
        # `mod` is a required parameter, though, it's not used in this context without views.
        self._schema_deployer = SchemaDeployer(sql_backend, self._SCHEMA, mod=None, catalog=ucx_catalog)

    def run(self) -> None:
        self._schema_deployer.deploy_schema()
        self._schema_deployer.deploy_table("workflow_runs", WorkflowRun)
        self._schema_deployer.deploy_table("historical", Historical)
        logger.info("Installation completed successfully!")


class VerifyProgressTracking:
    """Verify the progress tracking is ready to be used."""

    def __init__(
        self,
        verify_has_metastore: VerifyHasMetastore,
        verify_has_ucx_catalog: VerifyHasCatalog,
        deployed_workflows: DeployedWorkflows,
    ) -> None:
        self._verify_has_metastore = verify_has_metastore
        self._verify_has_ucx_catalog = verify_has_ucx_catalog
        self._deployed_workflows = deployed_workflows

    def verify(self, timeout=dt.timedelta(seconds=0)) -> None:
        """Verify the progress tracking installation is ready to be used.

        Prerequisites:
        - UC metastore exists.
        - UCX catalog exists.
        - A job run corresponding to the "assessment" job:
            - Finished successfully.
            - OR if pending or running, we will wait up to the timeout for the assessment run to finish. If it did still
              not finish successfully, we raise an error.

        Otherwise, we consider the prerequisites to be NOT matched.

        Args :
            timeout (datetime.timedelta) : Timeout to wait for pending or running assessment run.

        Raises :
            RuntimeWarning : Signalling the prerequisites are not met.
        """
        metastore_not_attached_message = (
            "Metastore not attached to workspace. Run `databricks labs ucx assign-metastore`"
        )
        try:
            has_metastore = self._verify_has_metastore.verify_metastore()
        except MetastoreNotFoundError as e:
            raise RuntimeWarning(metastore_not_attached_message) from e
        if not has_metastore:
            raise RuntimeWarning(metastore_not_attached_message)
        if not self._verify_has_ucx_catalog.verify():
            raise RuntimeWarning("UCX catalog not configured. Run `databricks labs ucx create-ucx-catalog`")
        if not self._deployed_workflows.validate_step("assessment", timeout=timeout):
            raise RuntimeWarning(
                "Assessment workflow did not complete successfully yet. "
                "Run `databricks labs ucx ensure-assessment-run` command"
            )

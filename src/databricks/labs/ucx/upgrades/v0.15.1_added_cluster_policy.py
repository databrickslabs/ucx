# pylint: disable=invalid-name

import json
import logging
from databricks.labs.ucx.install import WorkspaceInstallation
from dataclasses import replace

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.tui import Prompts
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.installer.policy import ClusterPolicyInstaller
from databricks.sdk.errors import (  # pylint: disable=redefined-builtin
    Aborted,
    AlreadyExists,
    BadRequest,
    Cancelled,
    DataLoss,
    DeadlineExceeded,
    InternalError,
    InvalidParameterValue,
    NotFound,
    NotImplemented,
    OperationFailed,
    PermissionDenied,
    RequestLimitExceeded,
    ResourceAlreadyExists,
    ResourceConflict,
    ResourceDoesNotExist,
    ResourceExhausted,
    TemporarilyUnavailable,
    TooManyRequests,
    Unauthenticated,
    Unknown,
)
logger = logging.getLogger(__name__)


def upgrade(installation: Installation, ws: WorkspaceClient):
    config = installation.load(WorkspaceConfig)
    prompts = Prompts()
    instance_profile = None
    spark_conf_dict = {}
    policy_installer = ClusterPolicyInstaller(installation, ws)
    policies_with_external_hms = list(policy_installer.get_cluster_policies_with_external_hive_metastores())
    if len(policies_with_external_hms) > 0 and prompts.confirm(
        "We have identified one or more cluster policies set up for an external metastore"
        "Would you like to set UCX to connect to the external metastore?"
    ):
        logger.info("Setting up an external metastore")
        cluster_policies = {conf.name: conf.definition for conf in policies_with_external_hms}
        if len(cluster_policies) >= 1:
            cluster_policy = json.loads(prompts.choice_from_dict("Choose a cluster policy", cluster_policies))
            instance_profile, spark_conf_dict = policy_installer.get_ext_hms_conf_from_policy(cluster_policy)

    policy_id = policy_installer.create_cluster_policy(config.inventory_database, spark_conf_dict, instance_profile)
    config.policy_id = policy_id
    installation.load("state.json")
    states = installation.load(json, filename="state.json")
    if not states.jobs:
        logger.error("No jobs present or jobs already deleted")
        return
    for step_name, job_id in states.jobs.items():
        try:
            job = ws.jobs.get(job_id)
            job_setting = job.settings
            for cluster in job_setting.job_clusters:
                cluster.new_cluster.policy_id=policy_id
            ws.jobs.update(job.job_id,new_settings=job_setting)
        except NotFound:
            logger.error(f"Job id {job.job_id} not found.")
            continue

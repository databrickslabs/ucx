# pylint: disable=invalid-name

import logging

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.installer import InstallState
from databricks.labs.blueprint.tui import Prompts
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.installer.policy import ClusterPolicyInstaller

logger = logging.getLogger(__name__)


def upgrade(installation: Installation, ws: WorkspaceClient):
    config = installation.load(WorkspaceConfig)
    policy_installer = ClusterPolicyInstaller(installation, ws, Prompts())
    config.policy_id, _, _, _ = policy_installer.create(config.inventory_database)
    installation.save(config)
    states = InstallState.from_installation(installation)
    assert config.policy_id is not None
    policy_installer.update_job_policy(states, config.policy_id)

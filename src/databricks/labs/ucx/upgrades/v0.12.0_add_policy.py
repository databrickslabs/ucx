from databricks.labs.blueprint.installation import Installation
from databricks.sdk import WorkspaceClient

from databricks.labs.ucx.config import WorkspaceConfig


def upgrade(installation: Installation, ws: WorkspaceClient):
    config = installation.load(WorkspaceConfig)
    policy_id = ws.cluster_policies.create(
        name=f"Unity Catalog Migration ({inventory_database})",
        definition=self._cluster_policy_definition(conf=spark_conf_dict, instance_profile=instance_profile),
        description="Custom cluster policy for Unity Catalog Migration (UCX)",
    ).policy_id
    config.policy_id = policy_id
    installation.save(config)

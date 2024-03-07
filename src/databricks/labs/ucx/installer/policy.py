import json
import logging

from databricks.labs.blueprint.installation import Installation
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import compute

logger = logging.getLogger(__name__)


class ClusterPolicyInstaller:
    def __init__(self, installation: Installation, ws: WorkspaceClient):
        self._ws = ws
        self._installation = installation
        self._policy_id = ""

    @staticmethod
    def _policy_config(value: str):
        return {"type": "fixed", "value": value}

    def create_cluster_policy(
        self, inventory_database: str, spark_conf: dict, instance_profile: str | None
    ) -> str | None:
        policy_name = f"Unity Catalog Migration ({inventory_database}) ({self._ws.current_user.me().user_name})"
        policies = self._ws.cluster_policies.list()
        policy_id = None
        for policy in policies:
            if policy.name == policy_name:
                policy_id = policy.policy_id
                logger.info(f"Cluster policy {policy_name} already present, reusing the same.")
                break
        if not policy_id:
            logger.info("Creating UCX cluster policy.")
            policy_id = self._ws.cluster_policies.create(
                name=policy_name,
                definition=self._cluster_policy_definition(conf=spark_conf, instance_profile=instance_profile),
                description="Custom cluster policy for Unity Catalog Migration (UCX)",
            ).policy_id
        assert policy_id is not None
        self._policy_id = policy_id
        return self._policy_id

    def _cluster_policy_definition(self, conf: dict, instance_profile: str | None) -> str:
        policy_definition = {
            "spark_version": self._policy_config(self._ws.clusters.select_spark_version(latest=True)),
            "node_type_id": self._policy_config(self._ws.clusters.select_node_type(local_disk=True)),
        }
        if conf:
            for key, value in conf.items():
                policy_definition[f"spark_conf.{key}"] = self._policy_config(value)
        if self._ws.config.is_aws:
            policy_definition["aws_attributes.availability"] = self._policy_config(
                compute.AwsAvailability.ON_DEMAND.value
            )
            if instance_profile:
                policy_definition["aws_attributes.instance_profile_arn"] = self._policy_config(instance_profile)
        elif self._ws.config.is_azure:  # pylint: disable=confusing-consecutive-elif
            policy_definition["azure_attributes.availability"] = self._policy_config(
                compute.AzureAvailability.ON_DEMAND_AZURE.value
            )
        else:
            policy_definition["gcp_attributes.availability"] = self._policy_config(
                compute.GcpAvailability.ON_DEMAND_GCP.value
            )
        return json.dumps(policy_definition)

    @staticmethod
    def get_ext_hms_conf_from_policy(cluster_policy):
        spark_conf_dict = {}
        instance_profile = None
        if cluster_policy.get("aws_attributes.instance_profile_arn") is not None:
            instance_profile = cluster_policy.get("aws_attributes.instance_profile_arn").get("value")
            logger.info(f"Instance Profile is Set to {instance_profile}")
        for key in cluster_policy.keys():
            if (
                key.startswith("spark_conf.spark.sql.hive.metastore")
                or key.startswith("spark_conf.spark.hadoop.javax.jdo.option")
                or key.startswith("spark_conf.spark.databricks.hive.metastore")
                or key.startswith("spark_conf.spark.hadoop.hive.metastore.glue")
            ):
                spark_conf_dict[key[11:]] = cluster_policy[key]["value"]
        return instance_profile, spark_conf_dict

    def get_cluster_policies_with_external_hive_metastores(self):
        for policy in self._ws.cluster_policies.list():
            def_json = json.loads(policy.definition)
            glue_node = def_json.get("spark_conf.spark.databricks.hive.metastore.glueCatalog.enabled")
            if glue_node is not None and glue_node.get("value") == "true":
                yield policy
                continue
            for key in def_json.keys():
                if key.startswith("spark_conf.spark.sql.hive.metastore"):
                    yield policy
                    break

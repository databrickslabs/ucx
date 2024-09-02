import json
import logging

from databricks.labs.blueprint.installation import Installation
from databricks.labs.blueprint.installer import InstallState
from databricks.labs.blueprint.tui import Prompts
from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service import compute
from databricks.sdk.service.sql import GetWorkspaceWarehouseConfigResponse


logger = logging.getLogger(__name__)

# Known prefixes for external HMS spark attributes
ext_hms_prefixes = [
    "spark_conf.spark.sql.hive.metastore",
    "spark_conf.spark.hadoop.hive.metastore",
    "spark_conf.spark.hadoop.javax.jdo.option",
    "spark_conf.spark.databricks.hive.metastore",
]


class ClusterPolicyInstaller:
    def __init__(self, installation: Installation, ws: WorkspaceClient, prompts: Prompts):
        self._ws = ws
        self._installation = installation
        self._prompts = prompts

    @staticmethod
    def _policy_config(value: str):
        return {"type": "fixed", "value": value}

    def has_ext_hms(self) -> bool:
        policies_with_external_hms = list(self._get_cluster_policies_with_external_hive_metastores())
        if len(policies_with_external_hms) > 0:
            return True
        warehouse_config = self._get_warehouse_config_with_external_hive_metastore()
        if warehouse_config is not None:
            return True
        return False

    def create(self, inventory_database: str) -> tuple[str, str, dict, str | None]:
        instance_profile = ""
        spark_conf_dict = {}
        # get instance pool id to be put into the cluster policy
        instance_pool_id = self._get_instance_pool_id()
        policies_with_external_hms = list(self._get_cluster_policies_with_external_hive_metastores())
        if len(policies_with_external_hms) > 0 and self._prompts.confirm(
            "We have identified one or more cluster policies set up for an external metastore. "
            "Would you like to set UCX to connect to the external metastore?"
        ):
            logger.info("Setting up an external metastore")
            cluster_policies = {conf.name: conf.definition for conf in policies_with_external_hms}
            if len(cluster_policies) >= 1:
                cluster_policy = json.loads(self._prompts.choice_from_dict("Choose a cluster policy", cluster_policies))
                instance_profile, spark_conf_dict = self._extract_external_hive_metastore_conf(cluster_policy)
        else:
            warehouse_config = self._get_warehouse_config_with_external_hive_metastore()
            if warehouse_config and self._prompts.confirm(
                "We have identified the workspace warehouse is set up for an external metastore. "
                "Would you like to set UCX to connect to the external metastore?"
            ):
                logger.info("Setting up an external metastore")
                instance_profile, spark_conf_dict = self._extract_external_hive_metastore_sql_conf(warehouse_config)
        policy_name = f"Unity Catalog Migration ({inventory_database}) ({self._ws.current_user.me().user_name})"
        policies = self._ws.cluster_policies.list()
        for policy in policies:
            if policy.name == policy_name:
                logger.info(f"Cluster policy {policy_name} already present, reusing the same.")
                policy_id = policy.policy_id
                assert policy_id is not None
                return policy_id, instance_profile, spark_conf_dict, instance_pool_id
        logger.info("Creating UCX cluster policy.")
        policy_id = self._ws.cluster_policies.create(
            name=policy_name,
            definition=self._definition(spark_conf_dict, instance_profile, instance_pool_id),
            description="Custom cluster policy for Unity Catalog Migration (UCX)",
        ).policy_id
        assert policy_id is not None
        return (
            policy_id,
            instance_profile,
            spark_conf_dict,
            instance_pool_id,
        )

    def _get_instance_pool_id(self) -> str | None:
        try:
            instance_pool_id = self._prompts.question(
                "Instance pool id to be set in cluster policy for all workflow clusters", default="None"
            )
        except OSError:
            # when unit test v0.15.0_added_cluster_policy.py MockPrompts cannot be injected to ClusterPolicyInstaller
            # return None to pass the test
            return None
        if instance_pool_id.lower() == "none":
            return None
        try:
            self._ws.instance_pools.get(instance_pool_id)
            return instance_pool_id
        except NotFound:
            logger.warning(
                f"Instance pool id {instance_pool_id} does not exist. Will not set instance pool in the cluster policy. You can manually edit the cluster policy after installation."
            )
            return None

    def _definition(self, conf: dict, instance_profile: str | None, instance_pool_id: str | None) -> str:
        latest_lts_dbr = self._ws.clusters.select_spark_version(latest=True)
        node_type_id = self._ws.clusters.select_node_type(local_disk=True, min_memory_gb=32)
        policy_definition = {
            "spark_version": self._policy_config(latest_lts_dbr),
            "node_type_id": self._policy_config(node_type_id),
        }
        for key, value in conf.items():
            policy_definition[f"spark_conf.{key}"] = self._policy_config(value)
        # set the availability to on demand
        if self._ws.config.is_aws:
            if instance_profile:
                policy_definition["aws_attributes.instance_profile_arn"] = self._policy_config(instance_profile)
            policy_definition["aws_attributes.availability"] = self._policy_config(
                compute.AwsAvailability.ON_DEMAND.value
            )
            policy_definition["aws_attributes.zone_id"] = self._policy_config("auto")
        elif self._ws.config.is_azure:
            policy_definition["azure_attributes.availability"] = self._policy_config(
                compute.AzureAvailability.ON_DEMAND_AZURE.value
            )
        else:
            policy_definition["gcp_attributes.availability"] = self._policy_config(
                compute.GcpAvailability.ON_DEMAND_GCP.value
            )
        if instance_pool_id:
            policy_definition["instance_pool_id"] = self._policy_config(instance_pool_id)
            # 'node_type_id' cannot be supplied when an instance pool ID is provided
            policy_definition.pop("node_type_id")
            # 'availability' cannot be supplied when an instance pool ID is provided
            policy_definition.pop("aws_attributes.availability", "")
            # 'zone_id' cannot be supplied when an instance pool ID is provided
            policy_definition.pop("aws_attributes.zone_id", "")
            policy_definition.pop("azure_attributes.availability", "")
            policy_definition.pop("gcp_attributes.availability", "")
        return json.dumps(policy_definition)

    @staticmethod
    def _extract_external_hive_metastore_conf(cluster_policy):
        spark_conf_dict = {}
        instance_profile = None
        if cluster_policy.get("aws_attributes.instance_profile_arn") is not None:
            instance_profile = cluster_policy.get("aws_attributes.instance_profile_arn").get("value")
            logger.info(f"Instance Profile is Set to {instance_profile}")
        for key in cluster_policy.keys():
            for known_prefix in ext_hms_prefixes:
                if key.startswith(known_prefix):
                    spark_conf_dict[key[11:]] = cluster_policy[key]["value"]
        return instance_profile, spark_conf_dict

    def _get_cluster_policies_with_external_hive_metastores(self):
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

    @staticmethod
    def _extract_external_hive_metastore_sql_conf(sql_config: GetWorkspaceWarehouseConfigResponse):
        spark_conf_dict: dict[str, str] = {}
        instance_profile = None
        if sql_config.instance_profile_arn is not None:
            instance_profile = sql_config.instance_profile_arn
            logger.info(f"Instance Profile is Set to {instance_profile}")
        if sql_config.data_access_config is None:
            return instance_profile, spark_conf_dict
        for conf in sql_config.data_access_config:
            if conf.key is None:
                continue
            if conf.value is None:
                continue
            if (
                conf.key.startswith("spark.sql.hive.metastore")
                or conf.key.startswith("spark.hadoop.javax.jdo.option")
                or conf.key.startswith("spark.databricks.hive.metastore")
                or conf.key.startswith("spark.hadoop.hive.metastore.glue")
            ):
                spark_conf_dict[conf.key] = conf.value
        return instance_profile, spark_conf_dict

    def _get_warehouse_config_with_external_hive_metastore(self) -> GetWorkspaceWarehouseConfigResponse | None:
        sql_config = self._ws.warehouses.get_workspace_warehouse_config()
        if sql_config.data_access_config is None:
            return None
        for conf in sql_config.data_access_config:
            if conf.key is None:
                continue
            is_glue = conf.key.startswith("spark.databricks.hive.metastore.glueCatalog.enabled")
            if conf.key.startswith("spark.sql.hive.metastore") or is_glue:
                return sql_config
        return None

    def update_job_policy(self, state: InstallState, policy_id: str):
        if not state.jobs:
            logger.error("No jobs found in states")
            return
        for _, job_id in state.jobs.items():
            try:
                job = self._ws.jobs.get(job_id)
                job_settings = job.settings
                assert job.job_id is not None
                assert job_settings is not None
                if job_settings.job_clusters is None:
                    # if job_clusters is None, it means override cluster is being set
                    # and hence policy should not be applied
                    return
            except NotFound:
                logger.error(f"Job id {job_id} not found. Please check if the job is present in the workspace")
                continue
            try:
                job_clusters = []
                for cluster in job_settings.job_clusters:
                    assert cluster.new_cluster is not None
                    cluster.new_cluster.policy_id = policy_id
                    job_clusters.append(cluster)
                job_settings.job_clusters = job_clusters
                self._ws.jobs.update(job.job_id, new_settings=job_settings)
            except NotFound:
                logger.error(f"Job id {job_id} not found.")
                continue

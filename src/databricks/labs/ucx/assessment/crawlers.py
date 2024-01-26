import base64
import json
import logging
import re

from databricks.sdk import WorkspaceClient
from databricks.sdk.errors import NotFound
from databricks.sdk.service import compute
from databricks.sdk.service.compute import ClusterDetails, Policy

logger = logging.getLogger(__name__)

INCOMPATIBLE_SPARK_CONFIG_KEYS = [
    "spark.databricks.passthrough.enabled",
    "spark.hadoop.javax.jdo.option.ConnectionURL",
    "spark.databricks.hive.metastore.glueCatalog.enabled",
]

_AZURE_SP_CONF = [
    "fs.azure.account.auth.type",
    "fs.azure.account.oauth.provider.type",
    "fs.azure.account.oauth2.client.id",
    "fs.azure.account.oauth2.client.secret",
    "fs.azure.account.oauth2.client.endpoint",
]
_SECRET_PATTERN = r"{{(secrets.*?)}}"
_STORAGE_ACCOUNT_EXTRACT_PATTERN = r"(?:id|endpoint)(.*?)dfs"
_AZURE_SP_CONF_FAILURE_MSG = "Uses azure service principal credentials config in"
_SECRET_LIST_LENGTH = 3
_CLIENT_ENDPOINT_LENGTH = 6
_INIT_SCRIPT_DBFS_PATH = 2


def _get_init_script_data(w, init_script_info):
    if init_script_info.dbfs:
        if len(init_script_info.dbfs.destination.split(":")) == _INIT_SCRIPT_DBFS_PATH:
            file_api_format_destination = init_script_info.dbfs.destination.split(":")[1]
            if file_api_format_destination:
                try:
                    data = w.dbfs.read(file_api_format_destination).data
                    return base64.b64decode(data).decode("utf-8")
                except NotFound:
                    return None
    if init_script_info.workspace:
        workspace_file_destination = init_script_info.workspace.destination
        if workspace_file_destination:
            try:
                data = w.workspace.export(workspace_file_destination).content
                return base64.b64decode(data).decode("utf-8")
            except NotFound:
                return None
    return None


def _azure_sp_conf_in_init_scripts(init_script_data: str) -> bool:
    for conf in _AZURE_SP_CONF:
        if re.search(conf, init_script_data):
            return True
    return False


def _azure_sp_conf_present_check(config: dict) -> bool:
    for key in config.keys():
        for conf in _AZURE_SP_CONF:
            if re.search(conf, key):
                return True
    return False


def spark_version_compatibility(spark_version: str | None) -> str:
    if not spark_version:
        return "unreported version"
    first_comp_custom_rt = 3
    first_comp_custom_x = 2
    dbr_version_components = spark_version.split("-")
    first_components = dbr_version_components[0].split(".")
    if "custom" in spark_version:
        # custom runtime
        return "unsupported"
    if "dlt" in spark_version:
        # shouldn't hit this? Does show up in cluster list
        return "dlt"
    if len(first_components) != first_comp_custom_rt:
        # custom runtime
        return "unsupported"
    if first_components[first_comp_custom_x] != "x":
        # custom runtime
        return "unsupported"

    try:
        version = int(first_components[0]), int(first_components[1])
    except ValueError:
        version = 0, 0
    if version < (10, 0):
        return "unsupported"
    if (10, 0) <= version < (11, 3):
        return "kinda works"
    return "supported"


def _check_spark_conf(conf: dict[str, str], source) -> list[str]:
    failures = []
    for k in INCOMPATIBLE_SPARK_CONFIG_KEYS:
        if k in conf:
            failures.append(f"unsupported config: {k}")
    for value in conf.values():
        if "dbfs:/mnt" in value or "/dbfs/mnt" in value:
            failures.append(f"using DBFS mount in configuration: {value}")
    # Checking if Azure cluster config is present in spark config
    if _azure_sp_conf_present_check(conf):
        failures.append(f"{_AZURE_SP_CONF_FAILURE_MSG} {source}.")
    return failures


def _safe_get_cluster_policy(ws: WorkspaceClient, policy_id: str) -> Policy | None:
    try:
        return ws.cluster_policies.get(policy_id)
    except NotFound:
        logger.warning(f"The cluster policy was deleted: {policy_id}")
        return None


def _check_cluster_policy(ws: WorkspaceClient, cluster, source):
    failures = []
    policy = _safe_get_cluster_policy(ws, cluster.policy_id)
    if policy:
        if policy.definition:
            if _azure_sp_conf_present_check(json.loads(policy.definition)):
                failures.append(f"{_AZURE_SP_CONF_FAILURE_MSG} {source}.")
        if policy.policy_family_definition_overrides:
            if _azure_sp_conf_present_check(json.loads(policy.policy_family_definition_overrides)):
                failures.append(f"{_AZURE_SP_CONF_FAILURE_MSG} {source}.")
    return failures


def _check_cluster_init_script(ws: WorkspaceClient, init_scripts, source):
    failures = []
    for init_script_info in init_scripts:
        init_script_data = _get_init_script_data(ws, init_script_info)
        failures.extend(_check_init_script(init_script_data, source))
    return failures


def _check_init_script(init_script_data, source):
    failures = []
    if not init_script_data:
        return failures
    if _azure_sp_conf_in_init_scripts(init_script_data):
        failures.append(f"{_AZURE_SP_CONF_FAILURE_MSG} {source}.")
    return failures


def _check_cluster_failures(ws: WorkspaceClient, cluster: ClusterDetails | compute.ClusterSpec, source):
    failures = []

    support_status = spark_version_compatibility(cluster.spark_version)
    if support_status != "supported":
        failures.append(f"not supported DBR: {cluster.spark_version}")
    if cluster.spark_conf is not None:
        failures.extend(_check_spark_conf(cluster.spark_conf, source))
    # Checking if Azure cluster config is present in cluster policies
    if cluster.policy_id:
        failures.extend(_check_cluster_policy(ws, cluster, source))
    if cluster.init_scripts:
        failures.extend(_check_cluster_init_script(ws, cluster.init_scripts, source))

    return failures

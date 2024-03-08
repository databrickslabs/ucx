import logging
import re

logger = logging.getLogger(__name__)

INCOMPATIBLE_SPARK_CONFIG_KEYS = [
    "spark.databricks.passthrough.enabled",
    "spark.hadoop.javax.jdo.option.ConnectionURL",
    "spark.databricks.hive.metastore.glueCatalog.enabled",
]

AZURE_SP_CONF = [
    "fs.azure.account.auth.type",
    "fs.azure.account.oauth.provider.type",
    "fs.azure.account.oauth2.client.id",
    "fs.azure.account.oauth2.client.secret",
    "fs.azure.account.oauth2.client.endpoint",
]
AZURE_SP_CONF_FAILURE_MSG = "Uses azure service principal credentials config in"
INIT_SCRIPT_DBFS_PATH = 2
INIT_SCRIPT_LOCAL_PATH = 2


def azure_sp_conf_in_init_scripts(init_script_data: str) -> bool:
    for conf in AZURE_SP_CONF:
        if re.search(conf, init_script_data):
            return True
    return False


def azure_sp_conf_present_check(config: dict) -> bool:
    for key in config.keys():
        for conf in AZURE_SP_CONF:
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

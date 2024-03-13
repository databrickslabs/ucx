# pylint: disable=invalid-name

import logging
from dataclasses import dataclass

from databricks.labs.blueprint.installation import Installation
from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

from databricks.labs.ucx.config import WorkspaceConfig
from databricks.labs.ucx.install import load_cluster_specs

upgrade_logger = logging.getLogger(__name__)


@dataclass
class V16WorkspaceConfig:  # pylint: disable=too-many-instance-attributes, duplicate-code
    __file__ = "config.yml"
    __version__ = 2
    inventory_database: str
    workspace_group_regex: str | None = None
    workspace_group_replace: str | None = None
    account_group_regex: str | None = None
    group_match_by_external_id: bool = False
    include_group_names: list[str] | None = None
    renamed_group_prefix: str | None = "ucx-renamed-"
    instance_pool_id: str | None = None
    warehouse_id: str | None = None
    connect: Config | None = None
    num_threads: int | None = 10
    database_to_catalog_mapping: dict[str, str] | None = None
    default_catalog: str | None = "ucx_default"
    log_level: str | None = "INFO"
    workspace_start_path: str = "/"
    instance_profile: str | None = None
    spark_conf: dict[str, str] | None = None
    override_clusters: dict[str, str] | None = None
    policy_id: str | None = None
    num_days_submit_runs_history: int = 30
    uber_spn_id: str | None = None
    is_terraform_used: bool = False
    include_databases: list[str] | None = None


def upgrade(installation: Installation, ws: WorkspaceClient):  # pylint: disable=unused-argument
    upgrade_logger.info("loading v0.16.0 WorkspaceConfig.")
    v16_config = installation.load(V16WorkspaceConfig)
    old_spark_conf = v16_config.spark_conf
    policy_id = v16_config.policy_id

    cluster_specs = load_cluster_specs()
    for _, cluster_spec in cluster_specs.items():
        cluster_spec.policy_id = policy_id
        if not old_spark_conf:
            continue
        spark_conf = cluster_spec.spark_conf
        if spark_conf:
            spark_conf.update(old_spark_conf)
            continue
        cluster_spec.spark_conf = old_spark_conf

    upgrade_logger.info("updating v0.16.0 WorkspaceConfig with spark_conf removed and cluster_specs added.")
    new_config = WorkspaceConfig(
        v16_config.inventory_database,
        workspace_group_regex=v16_config.workspace_group_regex,
        workspace_group_replace=v16_config.workspace_group_replace,
        account_group_regex=v16_config.account_group_regex,
        group_match_by_external_id=v16_config.group_match_by_external_id,
        include_group_names=v16_config.include_group_names,
        renamed_group_prefix=v16_config.renamed_group_prefix,
        instance_pool_id=v16_config.instance_pool_id,
        warehouse_id=v16_config.warehouse_id,
        connect=v16_config.connect,
        num_threads=v16_config.num_threads,
        database_to_catalog_mapping=v16_config.database_to_catalog_mapping,
        default_catalog=v16_config.default_catalog,
        log_level=v16_config.log_level,
        workspace_start_path=v16_config.workspace_start_path,
        instance_profile=v16_config.instance_profile,
        cluster_specs=cluster_specs,
        override_clusters=v16_config.override_clusters,
        policy_id=v16_config.policy_id,
        num_days_submit_runs_history=v16_config.num_days_submit_runs_history,
        uber_spn_id=v16_config.uber_spn_id,
        is_terraform_used=v16_config.is_terraform_used,
        include_databases=v16_config.include_databases,
    )
    installation.save(new_config)

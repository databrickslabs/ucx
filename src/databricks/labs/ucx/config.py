from dataclasses import dataclass

from databricks.sdk.core import Config

__all__ = ["WorkspaceConfig"]


@dataclass
class WorkspaceConfig:  # pylint: disable=too-many-instance-attributes
    __file__ = "config.yml"
    __version__ = 2

    inventory_database: str
    ucx_catalog: str = "ucx"  # Catalog to store UCX artifact tables (shared across workspaces)
    # Group name conversion parameters.
    workspace_group_regex: str | None = None
    workspace_group_replace: str | None = None
    account_group_regex: str | None = None
    group_match_by_external_id: bool = False
    # Includes group names for migration. If not specified, all matching groups will be picked up
    include_group_names: list[str] | None = None
    renamed_group_prefix: str | None = "db-temp-"
    instance_pool_id: str | None = None
    # in v3, warehouse_id should be part of connect
    warehouse_id: str | None = None
    connect: Config | None = None
    num_threads: int | None = 10
    database_to_catalog_mapping: dict[str, str] | None = None
    default_catalog: str | None = "ucx_default"  # DEPRECATED: Keeping to avoid errors when loading old configurations
    log_level: str | None = "INFO"

    # Starting path for notebooks and directories crawler
    workspace_start_path: str = "/"
    instance_profile: str | None = None
    spark_conf: dict[str, str] | None = None
    min_workers: int | None = 1
    max_workers: int | None = 10

    override_clusters: dict[str, str] | None = None
    policy_id: str | None = None
    num_days_submit_runs_history: int = 30
    uber_spn_id: str | None = None
    uber_instance_profile: str | None = None

    is_terraform_used: bool = False  # Not used, keep for backwards compatibility

    # Whether the assessment should capture a specific list of databases, if not specified, it will list all databases.
    include_databases: list[str] | None = None

    # Whether the tables in mounts crawler should crawl a specific list of mounts.
    # If not specified, it will list all mounts.
    include_mounts: list[str] | None = None
    exclude_paths_in_mount: list[str] | None = None
    include_paths_in_mount: list[str] | None = None

    # Used for limiting the number of jobs to be analysed
    include_job_ids: list[int] | None = None

    # Whether to trigger assessment job after installation
    trigger_job: bool = False

    # List of workspace ids ucx is installed on, only applied to account-level installation
    installed_workspace_ids: list[int] | None = None

    # Threshold for row count comparison during data reconciliation, in percentage
    recon_tolerance_percent: int = 5

    # Whether to upload dependent libraries to the workspace
    upload_dependencies: bool = False

    # [INTERNAL ONLY] Whether the assessment should capture only specific object permissions.
    include_object_permissions: list[str] | None = None

    # [INTERNAL ONLY] Whether the assessment should lint only specific dashboards.
    include_dashboard_ids: list[str] | None = None

    def replace_inventory_variable(self, text: str) -> str:
        return text.replace("$inventory", f"hive_metastore.{self.inventory_database}")

    @classmethod
    def v1_migrate(cls, raw: dict) -> dict:
        # See https://github.com/databrickslabs/ucx/blob/v0.5.0/src/databricks/labs/ucx/config.py#L16-L32
        groups = raw.pop("groups", {})
        selected_groups = groups.get("selected", [])
        if selected_groups:
            raw["include_group_names"] = selected_groups
        raw["renamed_group_prefix"] = groups.get("backup_group_prefix", "db-temp-")
        raw["version"] = 2
        return raw

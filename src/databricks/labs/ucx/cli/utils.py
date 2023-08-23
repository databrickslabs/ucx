from pathlib import Path

from yaml import safe_load

from databricks.labs.ucx.config import MigrationConfig


def get_migration_config(config_file: Path) -> MigrationConfig:
    _raw_config = safe_load(config_file.read_text())
    _raw_config = {} if not _raw_config else _raw_config
    return MigrationConfig(**_raw_config)

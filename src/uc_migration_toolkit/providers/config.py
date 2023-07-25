from uc_migration_toolkit.config import MigrationConfig


class ConfigProvider:
    def __init__(self):
        self._config: MigrationConfig | None = None

    def set_config(self, config: MigrationConfig):
        self._config = config

    @property
    def config(self) -> MigrationConfig:
        assert self._config, "Config is not set"
        return self._config


provider = ConfigProvider()

import os
from contextlib import contextmanager
from functools import partial
from pathlib import Path

import yaml
from pydantic import RootModel

from databricks.labs.ucx.cli.utils import get_migration_config
from databricks.labs.ucx.config import (
    GroupsConfig,
    InventoryConfig,
    InventoryTable,
    MigrationConfig,
)


def test_initialization():
    mc = partial(
        MigrationConfig,
        inventory=InventoryConfig(table=InventoryTable(catalog="catalog", database="database", name="name")),
        groups=GroupsConfig(auto=True),
    )
    mc()


# path context manager
# changes current directory to given path, then changes back to previous directory
@contextmanager
def set_directory(path: Path):
    """Sets the cwd within the context

    Args:
        path (Path): The path to the cwd

    Yields:
        None
    """

    origin = Path().absolute()
    try:
        os.chdir(path)
        yield
    finally:
        os.chdir(origin)


def test_reader(tmp_path: Path):
    with set_directory(tmp_path):
        mc = partial(
            MigrationConfig,
            inventory=InventoryConfig(table=InventoryTable(catalog="catalog", database="database", name="name")),
            groups=GroupsConfig(auto=True),
        )

        config: MigrationConfig = mc()
        config_file = tmp_path / "config.yml"

        with config_file.open("w") as writable:
            yaml.safe_dump(RootModel[MigrationConfig](config).model_dump(), writable)

        loaded = get_migration_config(config_file)
        assert loaded == config

import os
from contextlib import contextmanager
from functools import partial
from pathlib import Path

import yaml

from databricks.labs.ucx.config import GroupsConfig, WorkspaceConfig


def test_initialization():
    mc = partial(
        WorkspaceConfig,
        inventory_database="abc",
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
            WorkspaceConfig,
            inventory_database="abc",
            groups=GroupsConfig(auto=True),
            database_to_catalog_mapping={"db1": "cat1", "db2": "cat2"},
            spark_conf={"key1": "val1", "key2": "val2"},
        )

        config: WorkspaceConfig = mc()
        config_file = tmp_path / "config.yml"

        as_dict = config.as_dict()
        with config_file.open("w") as writable:
            yaml.safe_dump(as_dict, writable)

        loaded = WorkspaceConfig.from_file(config_file)
        assert loaded == config

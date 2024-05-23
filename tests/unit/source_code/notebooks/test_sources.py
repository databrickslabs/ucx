from pathlib import Path

import pytest

from databricks.labs.ucx.source_code.languages import Languages
from databricks.labs.ucx.source_code.notebooks.sources import FileLinter


@pytest.mark.parametrize("path, content", [("xyz.py", "a = 3"), ("xyz.sql", "select * from dual")])
def test_file_linter_lints_supported_language(path, content, migration_index):
    linter = FileLinter(Languages(migration_index), Path(path), content)
    advices = list(linter.lint())
    assert not advices


@pytest.mark.parametrize("path", ["xyz.scala", "xyz.r", "xyz.sh"])
def test_file_linter_lints_not_yet_supported_language(path, migration_index):
    linter = FileLinter(Languages(migration_index), Path(path), "")
    advices = list(linter.lint())
    assert [advice.code for advice in advices] == ["unsupported-language"]


@pytest.mark.parametrize(
    "path",
    [
        "xyz.json",
        "xyz.xml",
        "xyz.yml",
        "xyz.cfg",
        "xyz.md",
        "xyz.txt",
        "xyz.gif",
        "xyz.png",
        "xyz.jpg",
        "xyz.jpeg",
        "xyz.tif",
        "xyz.bmp",
        "xyz.toml",
        ".DS_Store",  # on MacOS
    ],
)
def test_file_linter_lints_ignorable_language(path, migration_index):
    linter = FileLinter(Languages(migration_index), Path(path), "")
    advices = list(linter.lint())
    assert not advices

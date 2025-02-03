import pytest

from databricks.labs.ucx.source_code.graph import Dependency
from databricks.labs.ucx.source_code.files import FileLoader
from databricks.labs.ucx.source_code.linters.context import LinterContext
from databricks.labs.ucx.source_code.linters.files import FileLinter


def test_file_linter_lints_python(tmp_path, migration_index, mock_path_lookup) -> None:
    path = tmp_path / "xyz.py"
    path.write_text("a = 3")
    dependency = Dependency(FileLoader(), path)
    linter = FileLinter(dependency, mock_path_lookup, LinterContext(migration_index))
    advices = list(linter.lint())
    assert not advices


def test_file_linter_lints_sql(tmp_path, migration_index, mock_path_lookup) -> None:
    path = tmp_path / "xyz.sql"
    path.write_text("SELECT * FROM dual")
    dependency = Dependency(FileLoader(), path)
    linter = FileLinter(dependency, mock_path_lookup, LinterContext(migration_index))
    advices = list(linter.lint())
    assert not advices


@pytest.mark.parametrize("path", ["xyz.scala", "xyz.r", "xyz.sh"])
def test_file_linter_lints_not_yet_supported_language(tmp_path, path, migration_index, mock_path_lookup) -> None:
    path = tmp_path / path
    path.touch()
    dependency = Dependency(FileLoader(), path)
    linter = FileLinter(dependency, mock_path_lookup, LinterContext(migration_index))
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
def test_file_linter_lints_ignorable_language(tmp_path, path, migration_index, mock_path_lookup) -> None:
    path = tmp_path / path
    path.touch()
    dependency = Dependency(FileLoader(), path)
    linter = FileLinter(dependency, mock_path_lookup, LinterContext(migration_index))
    advices = list(linter.lint())
    assert not advices

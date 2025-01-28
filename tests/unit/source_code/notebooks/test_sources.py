import codecs
from pathlib import Path

import pytest
from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationIndex
from databricks.labs.ucx.source_code.base import Deprecation, Failure
from databricks.labs.ucx.source_code.graph import Dependency
from databricks.labs.ucx.source_code.linters.context import LinterContext
from databricks.labs.ucx.source_code.files import FileLoader
from databricks.labs.ucx.source_code.notebooks.loaders import NotebookLoader
from databricks.labs.ucx.source_code.notebooks.sources import FileLinter, Notebook, NotebookLinter
from databricks.labs.ucx.source_code.path_lookup import PathLookup


@pytest.mark.parametrize(
    "source, language",
    [
        ("a = 3", Language.PYTHON),
        ("select * from dual", Language.SQL),
    ],
)
def test_file_linter_lints_supported_language(
    tmp_path, migration_index, mock_path_lookup, source: str, language: Language
) -> None:
    extension = ".sql" if language == Language.SQL else ".py"
    path = tmp_path / f"file{extension}"
    path.write_text(source)
    dependency = Dependency(FileLoader(), path)
    linter = FileLinter(dependency, mock_path_lookup, LinterContext(migration_index))
    advices = list(linter.lint())
    assert not advices


@pytest.mark.parametrize(
    "bom, encoding",
    [
        (codecs.BOM_UTF8, "utf-8"),
        (codecs.BOM_UTF16_LE, "utf-16-le"),
        (codecs.BOM_UTF16_BE, "utf-16-be"),
        (codecs.BOM_UTF32_LE, "utf-32-le"),
        (codecs.BOM_UTF32_BE, "utf-32-be"),
    ],
)
def test_file_linter_lints_supported_language_encoded_file_with_bom(
    tmp_path, migration_index, mock_path_lookup, bom, encoding
) -> None:
    path = tmp_path / "file.py"
    path.write_bytes(bom + "a = 12".encode(encoding))
    dependency = Dependency(FileLoader(), path)
    linter = FileLinter(dependency, mock_path_lookup, LinterContext(migration_index))

    advices = list(linter.lint())

    assert not advices


@pytest.mark.parametrize("path", ["xyz.scala", "xyz.r", "xyz.sh"])
def test_file_linter_lints_not_yet_supported_language(tmp_path, path, migration_index, mock_path_lookup) -> None:
    path = tmp_path / path
    path.touch()
    dependency = Dependency(FileLoader(), path)
    linter = FileLinter(dependency, mock_path_lookup, LinterContext(migration_index), None)
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
    linter = FileLinter(dependency, mock_path_lookup, LinterContext(migration_index), None)
    advices = list(linter.lint())
    assert not advices


def test_file_linter_lints_non_ascii_encoded_file(migration_index, mock_path_lookup) -> None:
    non_ascii_encoded_file = Path(__file__).parent.parent / "samples" / "nonascii.py"
    dependency = Dependency(FileLoader(), non_ascii_encoded_file)
    linter = FileLinter(dependency, mock_path_lookup, LinterContext(migration_index))
    advices = list(linter.lint())
    assert advices == [Failure("unsupported-file", f"Unsupported file: {non_ascii_encoded_file}", 0, 0, 1, 1)]


def test_file_linter_lints_file_with_missing_file(tmp_path, migration_index, mock_path_lookup) -> None:
    path = tmp_path / "non_existing_file.py"
    dependency = Dependency(FileLoader(), path)
    linter = FileLinter(dependency, mock_path_lookup, LinterContext(migration_index))
    advices = list(linter.lint())
    assert advices == [Failure("unsupported-file", f"Unsupported file: {path}", 0, 0, 1, 1)]


def test_file_linter_lints_leaf1_from_samples(migration_index, mock_path_lookup) -> None:
    dependency = Dependency(NotebookLoader(), Path("leaf1.py"))
    linter = FileLinter(dependency, mock_path_lookup, LinterContext(migration_index))
    advices = list(linter.lint())
    assert not advices
    assert Path("leaf1.py") in mock_path_lookup.successfully_resolved_paths


class _NotebookLinter(NotebookLinter):
    """A helper class to construct the notebook linter from source code for testing simplification."""

    @classmethod
    def from_source_code(
        cls, index: TableMigrationIndex, path_lookup: PathLookup, source: str, default_language: Language
    ) -> NotebookLinter:
        context = LinterContext(index)
        notebook = Notebook.parse(Path(""), source, default_language)
        assert notebook is not None
        return cls(notebook, path_lookup, context)


def test_notebook_linter_lints_source_yielding_no_advices(migration_index, mock_path_lookup) -> None:
    linter = _NotebookLinter.from_source_code(
        migration_index, mock_path_lookup, "# Databricks notebook source\nprint(1)", Language.PYTHON
    )

    advices = list(linter.lint())

    assert not advices, "Expected no advices"


def test_notebook_linter_lints_source_yielding_parse_failure(migration_index, mock_path_lookup) -> None:
    linter = _NotebookLinter.from_source_code(
        migration_index, mock_path_lookup, "# Databricks notebook source\nprint(1", Language.PYTHON
    )

    advices = list(linter.lint())

    assert advices == [
        Failure(
            code='python-parse-error',
            message='Failed to parse code due to invalid syntax: print(1',
            start_line=1,
            start_col=5,
            end_line=1,
            end_col=1,
        )
    ]


@pytest.mark.xfail(reason="https://github.com/databrickslabs/ucx/issues/3556")
def test_notebook_linter_lints_source_yielding_parse_failures(migration_index, mock_path_lookup) -> None:
    source = """
# Databricks notebook source

print(1

# COMMAND ----------

print(2
""".lstrip()  # Missing parentheses is on purpose
    linter = _NotebookLinter.from_source_code(migration_index, mock_path_lookup, source, Language.PYTHON)

    advices = list(linter.lint())

    assert advices == [
        Failure(
            code='python-parse-error',
            message='Failed to parse code due to invalid syntax: print(1',
            start_line=2,
            start_col=5,
            end_line=2,
            end_col=1,
        ),
        Failure(
            code='python-parse-error',
            message='Failed to parse code due to invalid syntax: print(2',
            start_line=6,
            start_col=5,
            end_line=6,
            end_col=1,
        ),
    ]


def test_notebook_linter_lints_parent_child_context_from_grand_parent(migration_index, mock_path_lookup) -> None:
    """Verify the NotebookLinter can resolve %run"""
    path = Path(__file__).parent.parent / "samples" / "parent-child-context" / "grand_parent.py"
    notebook = Notebook.parse(path, path.read_text(), Language.PYTHON)
    linter = NotebookLinter(notebook, mock_path_lookup.change_directory(path.parent), LinterContext(migration_index))

    advices = list(linter.lint())

    assert not advices, "Expected no advices"


def test_notebook_linter_lints_migrated_table(migration_index, mock_path_lookup) -> None:
    """Regression test with the tests below."""
    source = """
# Databricks notebook source

table_name = "old.things"  # Migrated table according to the migration index

# COMMAND ----------

spark.table(table_name)
""".lstrip()
    linter = _NotebookLinter.from_source_code(migration_index, mock_path_lookup, source, Language.PYTHON)

    advices = list(linter.lint())

    assert advices
    assert advices[0] == Deprecation(
        code='table-migrated-to-uc-python',
        message='Table old.things is migrated to brand.new.stuff in Unity Catalog',
        start_line=6,
        start_col=0,
        end_line=6,
        end_col=23,
    )


def test_notebook_linter_lints_not_migrated_table(migration_index, mock_path_lookup) -> None:
    """Regression test with the tests above and below."""
    source = """
# Databricks notebook source

table_name = "not_migrated.table"  # NOT a migrated table according to the migration index

# COMMAND ----------

spark.table(table_name)
""".lstrip()
    linter = _NotebookLinter.from_source_code(migration_index, mock_path_lookup, source, Language.PYTHON)

    advices = list(linter.lint())

    assert not [advice for advice in advices if advice.code == "table-migrated-to-uc"]


def test_notebook_linter_lints_migrated_table_with_rename(migration_index, mock_path_lookup) -> None:
    """The spark.table should read the table defined above the call not below.

    This is a regression test with the tests above and below.
    """
    source = """
# Databricks notebook source

table_name = "old.things"  # Migrated table according to the migration index

# COMMAND ----------

spark.table(table_name)

# COMMAND ----------

table_name = "not_migrated.table"  # NOT a migrated table according to the migration index
""".lstrip()
    linter = _NotebookLinter.from_source_code(migration_index, mock_path_lookup, source, Language.PYTHON)

    advices = list(linter.lint())

    assert advices
    assert advices[0] == Deprecation(
        code='table-migrated-to-uc-python',
        message='Table old.things is migrated to brand.new.stuff in Unity Catalog',
        start_line=6,
        start_col=0,
        end_line=6,
        end_col=23,
    )


def test_notebook_linter_lints_not_migrated_table_with_rename(migration_index, mock_path_lookup) -> None:
    """The spark.table should read the table defined above the call not below.

    This is a regression test with the tests above.
    """
    source = """
# Databricks notebook source

table_name = "not_migrated.table"  # NOT a migrated table according to the migration index

# COMMAND ----------

spark.table(table_name)

# COMMAND ----------

table_name = "old.things"  # Migrated table according to the migration index
""".lstrip()
    linter = _NotebookLinter.from_source_code(migration_index, mock_path_lookup, source, Language.PYTHON)

    advices = list(linter.lint())

    assert not [advice for advice in advices if advice.code == "table-migrated-to-uc"]

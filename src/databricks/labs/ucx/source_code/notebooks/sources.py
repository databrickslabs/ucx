from __future__ import annotations

from collections.abc import Iterable
from functools import cached_property
from pathlib import Path

from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.hive_metastore.migration_status import MigrationIndex
from databricks.labs.ucx.source_code.base import Advice, Failure

from databricks.labs.ucx.source_code.graph import SourceContainer, DependencyGraph, DependencyProblem
from databricks.labs.ucx.source_code.languages import Languages
from databricks.labs.ucx.source_code.notebooks.cells import CellLanguage, Cell, CELL_SEPARATOR, NOTEBOOK_HEADER


class Notebook(SourceContainer):

    @staticmethod
    def parse(path: Path, source: str, default_language: Language) -> Notebook:
        default_cell_language = CellLanguage.of_language(default_language)
        cells = default_cell_language.extract_cells(source)
        if cells is None:
            raise ValueError(f"Could not parse Notebook: {path}")
        return Notebook(path, source, default_language, cells, source.endswith('\n'))

    def __init__(self, path: Path, source: str, language: Language, cells: list[Cell], ends_with_lf):
        self._path = path
        self._source = source
        self._language = language
        self._cells = cells
        self._ends_with_lf = ends_with_lf

    @property
    def path(self) -> Path:
        return self._path

    @property
    def cells(self) -> list[Cell]:
        return self._cells

    @property
    def original_code(self) -> str:
        return self._source

    def to_migrated_code(self):
        default_language = CellLanguage.of_language(self._language)
        header = f"{default_language.comment_prefix} {NOTEBOOK_HEADER}"
        sources = [header]
        for i, cell in enumerate(self._cells):
            migrated_code = cell.migrated_code
            if cell.language is not default_language:
                migrated_code = default_language.wrap_with_magic(migrated_code, cell.language)
            sources.append(migrated_code)
            if i < len(self._cells) - 1:
                sources.append('')
                sources.append(f'{default_language.comment_prefix} {CELL_SEPARATOR}')
                sources.append('')
        if self._ends_with_lf:
            sources.append('')  # following join will append lf
        return '\n'.join(sources)

    def build_dependency_graph(self, parent: DependencyGraph) -> list[DependencyProblem]:
        problems: list[DependencyProblem] = []
        for cell in self._cells:
            cell_problems = cell.build_dependency_graph(parent)
            problems.extend(cell_problems)
        return problems

    def __repr__(self):
        return f"<Notebook {self._path}>"


class NotebookLinter:
    """
    Parses a Databricks notebook and then applies available linters
    to the code cells according to the language of the cell.
    """

    def __init__(self, langs: Languages, notebook: Notebook):
        self._languages: Languages = langs
        self._notebook: Notebook = notebook

    @classmethod
    def from_source(cls, index: MigrationIndex, source: str, default_language: Language) -> 'NotebookLinter':
        langs = Languages(index)
        notebook = Notebook.parse(Path(""), source, default_language)
        assert notebook is not None
        return cls(langs, notebook)

    def lint(self) -> Iterable[Advice]:
        for cell in self._notebook.cells:
            if not self._languages.is_supported(cell.language.language):
                continue
            linter = self._languages.linter(cell.language.language)
            for advice in linter.lint(cell.original_code):
                yield advice.replace(
                    start_line=advice.start_line + cell.original_offset,
                    end_line=advice.end_line + cell.original_offset,
                )

    @staticmethod
    def name() -> str:
        return "notebook-linter"


SUPPORTED_EXTENSION_LANGUAGES = {
    '.py': Language.PYTHON,
    '.sql': Language.SQL,
}


class FileLinter:
    _NOT_YET_SUPPORTED_SUFFIXES = {
        '.scala',
        '.sh',
        '.r',
    }
    _IGNORED_SUFFIXES = {
        '.json',
        '.md',
        '.txt',
        '.xml',
        '.yml',
        '.toml',
        '.cfg',
        '.bmp',
        '.gif',
        '.png',
        '.tif',
        '.tiff',
        '.svg',
        '.jpg',
        '.jpeg',
        '.pyc',
        '.whl',
        '.egg',
        '.class',
        '.iml',
        '.gz',
    }
    _IGNORED_NAMES = {
        '.ds_store',
        '.gitignore',
        '.coverage',
        'license',
        'codeowners',
        'makefile',
        'pkg-info',
        'metadata',
        'wheel',
        'record',
        'notice',
        'zip-safe',
    }

    def __init__(self, langs: Languages, path: Path, content: str | None = None):
        self._languages: Languages = langs
        self._path: Path = path
        self._content = content

    @cached_property
    def _source_code(self) -> str:
        return self._path.read_text() if self._content is None else self._content

    def _file_language(self):
        return SUPPORTED_EXTENSION_LANGUAGES.get(self._path.suffix.lower())

    def _is_notebook(self):
        language = self._file_language()
        if not language:
            return False
        return self._source_code.startswith(CellLanguage.of_language(language).file_magic_header)

    def lint(self) -> Iterable[Advice]:
        if self._is_notebook():
            yield from self._lint_notebook()
        else:
            yield from self._lint_file()

    def _lint_file(self):
        language = self._file_language()
        if not language:
            suffix = self._path.suffix.lower()
            if suffix in self._IGNORED_SUFFIXES or self._path.name.lower() in self._IGNORED_NAMES:
                yield from []
            elif suffix in self._NOT_YET_SUPPORTED_SUFFIXES:
                yield Failure("unsupported-language", f"Language not supported yet for {self._path}", 0, 0, 1, 1)
            else:
                yield Failure("unknown-language", f"Cannot detect language for {self._path}", 0, 0, 1, 1)
        else:
            try:
                linter = self._languages.linter(language)
                yield from linter.lint(self._source_code)
            except ValueError as err:
                yield Failure(
                    "unsupported-content", f"Error while parsing content of {self._path.as_posix()}: {err}", 0, 0, 1, 1
                )

    def _lint_notebook(self):
        notebook = Notebook.parse(self._path, self._source_code, self._file_language())
        notebook_linter = NotebookLinter(self._languages, notebook)
        yield from notebook_linter.lint()

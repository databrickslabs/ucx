from __future__ import annotations

import codecs
import locale
from collections.abc import Iterable
from functools import cached_property
from pathlib import Path
from typing import cast

from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.hive_metastore.migration_status import MigrationIndex
from databricks.labs.ucx.source_code.base import Advice, Failure, Linter, PythonSequentialLinter

from databricks.labs.ucx.source_code.graph import SourceContainer, DependencyGraph, DependencyProblem
from databricks.labs.ucx.source_code.linters.context import LinterContext
from databricks.labs.ucx.source_code.notebooks.cells import CellLanguage, Cell, CELL_SEPARATOR, NOTEBOOK_HEADER, \
    RunCell, PythonCell
from databricks.labs.ucx.source_code.path_lookup import PathLookup


class Notebook(SourceContainer):

    @staticmethod
    def parse(path: Path, source: str, default_language: Language) -> Notebook:
        default_cell_language = CellLanguage.of_language(default_language)
        cells = default_cell_language.extract_cells(source)
        if cells is None:
            raise ValueError(f"Could not parse Notebook: {path}")
        return Notebook(path, source, default_language, cells, source.endswith('\n'))

    def __init__(self, path: Path, source: str, language: Language, cells: list[Cell], ends_with_lf: bool):
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
        """Check for any problems with dependencies of the cells in this notebook.

        Returns:
            A list of found dependency problems; position information for problems is relative to the notebook source.
        """
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

    def __init__(self, context: LinterContext, path_lookup: PathLookup, notebook: Notebook):
        self._context: LinterContext = context
        self._path_lookup = path_lookup
        self._notebook: Notebook = notebook
        # reuse Python linter, which accumulates statements for improved inference
        self._python_linter: PythonSequentialLinter = cast(PythonSequentialLinter, context.linter(Language.PYTHON))

    @classmethod
    def from_source(cls, index: MigrationIndex, source: str, default_language: Language, path_lookup: PathLookup) -> NotebookLinter:
        ctx = LinterContext(index)
        notebook = Notebook.parse(Path(""), source, default_language)
        assert notebook is not None
        return cls(ctx, path_lookup, notebook)

    def lint(self) -> Iterable[Advice]:
        for cell in self._notebook.cells:
            if isinstance(cell, RunCell):
                self._process_run_cell(cell)
            if not self._context.is_supported(cell.language.language):
                continue
            linter = self._linter(cell.language.language)
            for advice in linter.lint(cell.original_code):
                yield advice.replace(
                    start_line=advice.start_line + cell.original_offset,
                    end_line=advice.end_line + cell.original_offset,
                )

    def _process_run_cell(self, cell: RunCell):
        path, _ = cell.read_notebook_path()
        if path is None:
            return  # malformed run cell already reported
        resolved = self._path_lookup.resolve(path)
        if resolved is None:
            return  # already reported during dependency building
        # TODO deal with workspace notebooks
        language = SUPPORTED_EXTENSION_LANGUAGES.get(resolved.suffix.lower(), None)
        # we only support Python for now
        if language is not Language.PYTHON:
            return
        source = resolved.read_text(_guess_encoding(resolved))
        notebook = Notebook.parse(path, source, language)
        for cell in notebook.cells:
            if isinstance(cell, RunCell):
                self._process_run_cell(cell)
                continue
            if not isinstance(cell, PythonCell):
                continue
            self._python_linter.process_child_cell(cell.original_code)

    def _linter(self, language: Language) -> Linter:
        if language is Language.PYTHON:
            return self._python_linter
        return self._context.linter(language)

    @staticmethod
    def name() -> str:
        return "notebook-linter"


SUPPORTED_EXTENSION_LANGUAGES = {
    '.py': Language.PYTHON,
    '.sql': Language.SQL,
}


def _guess_encoding(path:Path):
    # some files encode a unicode BOM (byte-order-mark), so let's use that if available
    with path.open('rb') as _file:
        raw = _file.read(4)
        if raw.startswith(codecs.BOM_UTF32_LE) or raw.startswith(codecs.BOM_UTF32_BE):
            return 'utf-32'
        if raw.startswith(codecs.BOM_UTF16_LE) or raw.startswith(codecs.BOM_UTF16_BE):
            return 'utf-16'
        if raw.startswith(codecs.BOM_UTF8):
            return 'utf-8-sig'
        # no BOM, let's use default encoding
        return locale.getpreferredencoding(False)


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

    def __init__(self, ctx: LinterContext, path: Path, content: str | None = None):
        self._ctx: LinterContext = ctx
        self._path: Path = path
        self._content = content

    @cached_property
    def _source_code(self) -> str:
        if self._content is None:
            self._content = self._path.read_text(_guess_encoding(self._path))
        return self._content

    def _file_language(self):
        return SUPPORTED_EXTENSION_LANGUAGES.get(self._path.suffix.lower())

    def _is_notebook(self):
        language = self._file_language()
        if not language:
            return False
        return self._source_code.startswith(CellLanguage.of_language(language).file_magic_header)

    def lint(self) -> Iterable[Advice]:
        encoding = locale.getpreferredencoding(False)
        try:
            is_notebook = self._is_notebook()
        except FileNotFoundError:
            failure_message = f"File not found: {self._path}"
            yield Failure("file-not-found", failure_message, 0, 0, 1, 1)
            return
        except UnicodeDecodeError:
            failure_message = f"File without {encoding} encoding is not supported {self._path}"
            yield Failure("unsupported-file-encoding", failure_message, 0, 0, 1, 1)
            return
        except PermissionError:
            failure_message = f"Missing read permission for {self._path}"
            yield Failure("file-permission", failure_message, 0, 0, 1, 1)
            return

        if is_notebook:
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
                linter = self._ctx.linter(language)
                yield from linter.lint(self._source_code)
            except ValueError as err:
                failure_message = f"Error while parsing content of {self._path.as_posix()}: {err}"
                yield Failure("unsupported-content", failure_message, 0, 0, 1, 1)

    def _lint_notebook(self):
        notebook = Notebook.parse(self._path, self._source_code, self._file_language())
        notebook_linter = NotebookLinter(self._ctx, notebook)
        yield from notebook_linter.lint()

from __future__ import annotations

import dataclasses
import locale
import logging
from collections.abc import Iterable
from pathlib import Path
from typing import cast


from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationIndex
from databricks.labs.ucx.source_code.base import (
    file_language,
    is_a_notebook,
    safe_read_text,
    read_text,
    Advice,
    CurrentSessionState,
    Failure,
)

from databricks.labs.ucx.source_code.graph import (
    SourceContainer,
    DependencyGraph,
    DependencyProblem,
    InheritedContext,
)
from databricks.labs.ucx.source_code.linters.context import LinterContext
from databricks.labs.ucx.source_code.linters.imports import (
    SysPathChange,
    UnresolvedPath,
)
from databricks.labs.ucx.source_code.notebooks.magic import MagicLine
from databricks.labs.ucx.source_code.python.python_ast import PythonLinter, Tree
from databricks.labs.ucx.source_code.notebooks.cells import (
    CellLanguage,
    Cell,
    CELL_SEPARATOR,
    NOTEBOOK_HEADER,
    RunCell,
    PythonCell,
    RunCommand,
)
from databricks.labs.ucx.source_code.path_lookup import PathLookup

logger = logging.getLogger(__name__)


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

    def to_migrated_code(self) -> str:
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

    def build_inherited_context(self, graph: DependencyGraph, child_path: Path) -> InheritedContext:
        problems: list[DependencyProblem] = []
        context = InheritedContext(None, False, problems)
        for cell in self._cells:
            child_context = cell.build_inherited_context(graph, child_path)
            context = context.append(child_context, True)
            if context.found:
                return context
        return context

    def __repr__(self):
        return f"<Notebook {self._path}>"


class NotebookLinter:
    """
    Parses a Databricks notebook and then applies available linters
    to the code cells according to the language of the cell.
    """

    @classmethod
    def from_source(
        cls,
        index: TableMigrationIndex,
        path_lookup: PathLookup,
        session_state: CurrentSessionState,
        source: str,
        default_language: Language,
    ) -> NotebookLinter:
        ctx = LinterContext(index)
        notebook = Notebook.parse(Path(""), source, default_language)
        assert notebook is not None
        return cls(ctx, path_lookup, session_state, notebook)

    def __init__(
        self,
        context: LinterContext,
        path_lookup: PathLookup,
        session_state: CurrentSessionState,
        notebook: Notebook,
        inherited_tree: Tree | None = None,
    ):
        self._context: LinterContext = context
        self._path_lookup = path_lookup
        self._session_state = session_state
        self._notebook: Notebook = notebook
        self._inherited_tree = inherited_tree

        self._python_trees: dict[PythonCell, Tree] = {}  # the original trees to be linted

    def lint(self) -> Iterable[Advice]:
        failure = self._parse_trees(self._notebook, True, parent_tree=self._inherited_tree)
        if failure:
            yield failure
            return
        for cell in self._notebook.cells:
            try:
                linter = self._context.linter(cell.language.language)
            except ValueError:  # Language is not supported (yet)
                continue
            if isinstance(cell, PythonCell):
                linter = cast(PythonLinter, linter)
                tree = self._python_trees[cell]
                advices = linter.lint_tree(tree)
            else:
                advices = linter.lint(cell.original_code)
            for advice in advices:
                yield dataclasses.replace(
                    advice,
                    start_line=advice.start_line + cell.original_offset,
                    end_line=advice.end_line + cell.original_offset,
                )
        return

    def _parse_trees(self, notebook: Notebook, register_trees: bool, *, parent_tree: Tree | None) -> Failure | None:
        for cell in notebook.cells:
            failure = None
            if isinstance(cell, RunCell):
                failure = self._load_tree_from_run_cell(cell, parent_tree=parent_tree)
            elif isinstance(cell, PythonCell):
                failure = self._load_tree_from_python_cell(cell, register_trees, parent_tree=parent_tree)
                parent_tree = self._python_trees.get(cell)
            if failure:
                return failure
        return None

    def _load_tree_from_run_cell(self, run_cell: RunCell, *, parent_tree: Tree | None = None) -> Failure | None:
        path = run_cell.maybe_notebook_path()
        if path is None:
            return None  # malformed run cell already reported
        notebook = self._load_source_from_path(path)
        if notebook is not None:
            return self._parse_trees(notebook, False, parent_tree=parent_tree)
        return None

    def _load_tree_from_python_cell(
        self, python_cell: PythonCell, register_trees: bool, *, parent_tree: Tree | None
    ) -> Failure | None:
        maybe_tree = Tree.maybe_normalized_parse(python_cell.original_code)
        if maybe_tree.failure:
            return maybe_tree.failure
        assert maybe_tree.tree is not None
        if parent_tree:
            parent_tree.attach_child_tree(maybe_tree.tree)
        if register_trees:
            # A cell with only comments will not produce a tree
            self._python_trees[python_cell] = maybe_tree.tree or Tree.new_module()
        return self._load_children_from_tree(maybe_tree.tree)

    def _load_children_from_tree(self, tree: Tree) -> Failure | None:
        """Load children from tree by looking for child notebooks and path changes that might affect their loading."""
        code_path_nodes = self._list_magic_lines_with_run_command(tree) + SysPathChange.extract_from_tree(
            self._session_state, tree
        )
        # Sys path changes require to load children in order of reading
        for base_node in sorted(code_path_nodes, key=lambda node: (node.node.lineno, node.node.col_offset)):
            failure = self._load_children_from_base_node(base_node, parent_tree=tree)
            if failure:
                return failure
        return None

    @staticmethod
    def _list_magic_lines_with_run_command(tree: Tree) -> list[MagicLine]:
        """List the magic lines with a run command"""
        run_commands = []
        magic_lines, _ = MagicLine.extract_from_tree(tree, lambda code, message, node: None)
        for magic_line in magic_lines:
            if isinstance(magic_line.as_magic(), RunCommand):
                run_commands.append(magic_line)
        return run_commands

    def _load_children_from_base_node(
        self, base_node: SysPathChange | MagicLine, *, parent_tree: Tree | None
    ) -> Failure | None:
        if isinstance(base_node, SysPathChange):
            failure = self._mutate_path_lookup(base_node)
            if failure:
                return failure
        if isinstance(base_node, MagicLine):
            magic = base_node.as_magic()
            assert isinstance(magic, RunCommand)
            notebook = self._load_source_from_path(magic.notebook_path)
            if notebook is None:
                failure = Failure.from_node(
                    code='dependency-not-found',
                    message=f"Can't locate dependency: {magic.notebook_path}",
                    node=base_node.node,
                )
                return failure
            return self._parse_trees(notebook, False, parent_tree=parent_tree)
        return None

    def _mutate_path_lookup(self, change: SysPathChange) -> Failure | None:
        if isinstance(change, UnresolvedPath):
            return Failure.from_node(
                code='sys-path-cannot-compute-value',
                message=f"Can't update sys.path from {change.node.as_string()} because the expression cannot be computed",
                node=change.node,
            )
        change.apply_to(self._path_lookup)
        return None

    def _load_source_from_path(self, path: Path | None) -> Notebook | None:
        if path is None:
            return None  # already reported during dependency building
        resolved = self._path_lookup.resolve(path)
        if resolved is None:
            return None  # already reported during dependency building
        # TODO deal with workspace notebooks
        language = file_language(resolved)
        # we only support Python notebooks for now
        if language is not Language.PYTHON:
            logger.warning(f"Unsupported notebook language: {language}")
            return None
        source = safe_read_text(resolved)
        if not source:
            return None
        return Notebook.parse(path, source, language)


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

    def __init__(
        self,
        ctx: LinterContext,
        path_lookup: PathLookup,
        session_state: CurrentSessionState,
        path: Path,
        inherited_tree: Tree | None = None,
        content: str | None = None,
    ):
        self._ctx: LinterContext = ctx
        self._path_lookup = path_lookup
        self._session_state = session_state
        self._path = path
        self._inherited_tree = inherited_tree
        self._content = content

    def lint(self) -> Iterable[Advice]:
        encoding = locale.getpreferredencoding(False)
        try:
            # Not using `safe_read_text` here to surface read errors
            self._content = self._content or read_text(self._path)
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

        if self._is_notebook():
            yield from self._lint_notebook()
        else:
            yield from self._lint_file()

    def _is_notebook(self) -> bool:
        assert self._content is not None, "Content should be read from path before calling this method"
        # pre-check to avoid loading unsupported content
        language = file_language(self._path)
        if not language:
            return False
        return is_a_notebook(self._path, self._content)

    def _lint_file(self) -> Iterable[Advice]:
        assert self._content is not None, "Content should be read from path before calling this method"
        language = file_language(self._path)
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
                yield from linter.lint(self._content)
            except ValueError as err:
                failure_message = f"Error while parsing content of {self._path.as_posix()}: {err}"
                yield Failure("unsupported-content", failure_message, 0, 0, 1, 1)

    def _lint_notebook(self) -> Iterable[Advice]:
        assert self._content is not None, "Content should be read from path before calling this method"
        language = file_language(self._path)
        if not language:
            yield Failure("unknown-language", f"Cannot detect language for {self._path}", 0, 0, 1, 1)
            return
        notebook = Notebook.parse(self._path, self._content, language)
        notebook_linter = NotebookLinter(
            self._ctx, self._path_lookup, self._session_state, notebook, self._inherited_tree
        )
        yield from notebook_linter.lint()

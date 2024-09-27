from __future__ import annotations

import dataclasses
import locale
import logging
from collections.abc import Iterable
from functools import cached_property
from pathlib import Path
from typing import cast

from astroid import AstroidSyntaxError, Module, NodeNG  # type: ignore

from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationIndex
from databricks.labs.ucx.source_code.base import (
    Advice,
    Failure,
    Linter,
    PythonSequentialLinter,
    CurrentSessionState,
    Advisory,
    guess_encoding,
    file_language,
    is_a_notebook,
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
from databricks.labs.ucx.source_code.python.python_ast import Tree, NodeBase
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

    def build_inherited_context(self, graph: DependencyGraph, child_path: Path) -> InheritedContext:
        context = InheritedContext(None, False)
        for cell in self._cells:
            child = cell.build_inherited_context(graph, child_path)
            context = context.append(child, True)
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
        # reuse Python linter across related files and notebook cells
        # this is required in order to accumulate statements for improved inference
        self._python_linter: PythonSequentialLinter = cast(PythonSequentialLinter, context.linter(Language.PYTHON))
        if inherited_tree is not None:
            self._python_linter.append_tree(inherited_tree)
        self._python_trees: dict[PythonCell, Tree] = {}  # the original trees to be linted

    def lint(self) -> Iterable[Advice]:
        has_failure = False
        for advice in self._load_tree_from_notebook(self._notebook, True):
            if isinstance(advice, Failure):  # happens when a cell is unparseable
                has_failure = True
            yield advice
        if has_failure:
            return
        for cell in self._notebook.cells:
            if not self._context.is_supported(cell.language.language):
                continue
            if isinstance(cell, PythonCell):
                tree = self._python_trees[cell]
                advices = self._python_linter.lint_tree(tree)
            else:
                linter = self._linter(cell.language.language)
                advices = linter.lint(cell.original_code)
            for advice in advices:
                yield dataclasses.replace(
                    advice,
                    start_line=advice.start_line + cell.original_offset,
                    end_line=advice.end_line + cell.original_offset,
                )

    def _load_tree_from_notebook(self, notebook: Notebook, register_trees: bool) -> Iterable[Advice]:
        for cell in notebook.cells:
            if isinstance(cell, RunCell):
                yield from self._load_tree_from_run_cell(cell)
                continue
            if isinstance(cell, PythonCell):
                yield from self._load_tree_from_python_cell(cell, register_trees)
                continue

    def _load_tree_from_python_cell(self, python_cell: PythonCell, register_trees: bool) -> Iterable[Advice]:
        try:
            tree = Tree.normalize_and_parse(python_cell.original_code)
            if register_trees:
                self._python_trees[python_cell] = tree
            yield from self._load_children_from_tree(tree)
        except AstroidSyntaxError as e:
            yield Failure('syntax-error', str(e), 0, 0, 0, 0)

    def _load_children_from_tree(self, tree: Tree) -> Iterable[Advice]:
        assert isinstance(tree.node, Module)
        # look for child notebooks (and sys.path changes that might affect their loading)
        base_nodes: list[NodeBase] = []
        base_nodes.extend(self._list_run_magic_lines(tree))
        base_nodes.extend(SysPathChange.extract_from_tree(self._session_state, tree))
        if len(base_nodes) == 0:
            self._python_linter.append_tree(tree)
            return
        # append globals
        globs = cast(Module, tree.node).globals
        self._python_linter.append_globals(globs)
        # need to execute things in intertwined sequence so concat and sort them
        nodes = list(cast(Module, tree.node).body)
        base_nodes = sorted(base_nodes, key=lambda node: (node.node.lineno, node.node.col_offset))
        yield from self._load_children_with_base_nodes(nodes, base_nodes)
        # append remaining nodes
        self._python_linter.append_nodes(nodes)

    @staticmethod
    def _list_run_magic_lines(tree: Tree) -> Iterable[MagicLine]:

        def _ignore_problem(_code: str, _message: str, _node: NodeNG) -> None:
            return None

        commands, _ = MagicLine.extract_from_tree(tree, _ignore_problem)
        for command in commands:
            if isinstance(command.as_magic(), RunCommand):
                yield command

    def _load_children_with_base_nodes(self, nodes: list[NodeNG], base_nodes: list[NodeBase]):
        for base_node in base_nodes:
            yield from self._load_children_with_base_node(nodes, base_node)

    def _load_children_with_base_node(self, nodes: list[NodeNG], base_node: NodeBase):
        while len(nodes) > 0:
            node = nodes.pop(0)
            self._python_linter.append_nodes([node])
            if node.lineno < base_node.node.lineno:
                continue
            yield from self._load_children_from_base_node(base_node)

    def _load_children_from_base_node(self, base_node: NodeBase):
        if isinstance(base_node, SysPathChange):
            yield from self._mutate_path_lookup(base_node)
            return
        if isinstance(base_node, MagicLine):
            magic = base_node.as_magic()
            assert isinstance(magic, RunCommand)
            notebook = self._load_source_from_path(magic.notebook_path)
            if notebook is None:
                yield Advisory.from_node(
                    code='dependency-not-found',
                    message=f"Can't locate dependency: {magic.notebook_path}",
                    node=base_node.node,
                )
                return
            yield from self._load_tree_from_notebook(notebook, False)
            return

    def _mutate_path_lookup(self, change: SysPathChange):
        if isinstance(change, UnresolvedPath):
            yield Advisory.from_node(
                code='sys-path-cannot-compute-value',
                message=f"Can't update sys.path from {change.node.as_string()} because the expression cannot be computed",
                node=change.node,
            )
            return
        change.apply_to(self._path_lookup)

    def _load_tree_from_run_cell(self, run_cell: RunCell) -> Iterable[Advice]:
        path = run_cell.maybe_notebook_path()
        if path is None:
            return  # malformed run cell already reported
        notebook = self._load_source_from_path(path)
        if notebook is not None:
            yield from self._load_tree_from_notebook(notebook, False)

    def _load_source_from_path(self, path: Path | None):
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
        source = resolved.read_text(guess_encoding(resolved))
        return Notebook.parse(path, source, language)

    def _linter(self, language: Language) -> Linter:
        if language is Language.PYTHON:
            return self._python_linter
        return self._context.linter(language)

    def _load_source_from_run_cell(self, run_cell: RunCell):
        path = run_cell.maybe_notebook_path()
        if path is None:
            return  # malformed run cell already reported
        resolved = self._path_lookup.resolve(path)
        if resolved is None:
            return  # already reported during dependency building
        language = file_language(resolved)
        if language is not Language.PYTHON:
            return
        source = resolved.read_text(guess_encoding(resolved))
        notebook = Notebook.parse(path, source, language)
        for cell in notebook.cells:
            if isinstance(cell, RunCell):
                self._load_source_from_run_cell(cell)
                continue
            if not isinstance(cell, PythonCell):
                continue
            self._python_linter.process_child_cell(cell.original_code)

    @staticmethod
    def name() -> str:
        return "notebook-linter"


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

    @cached_property
    def _source_code(self) -> str:
        if self._content is None:
            self._content = self._path.read_text(guess_encoding(self._path))
        return self._content

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

    def _is_notebook(self):
        # pre-check to avoid loading unsupported content
        language = file_language(self._path)
        if not language:
            return False
        return is_a_notebook(self._path, self._source_code)

    def _lint_file(self):
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
                if self._inherited_tree is not None and isinstance(linter, PythonSequentialLinter):
                    linter.append_tree(self._inherited_tree)
                yield from linter.lint(self._source_code)
            except ValueError as err:
                failure_message = f"Error while parsing content of {self._path.as_posix()}: {err}"
                yield Failure("unsupported-content", failure_message, 0, 0, 1, 1)

    def _lint_notebook(self):
        notebook = Notebook.parse(self._path, self._source_code, file_language(self._path))
        notebook_linter = NotebookLinter(
            self._ctx, self._path_lookup, self._session_state, notebook, self._inherited_tree
        )
        yield from notebook_linter.lint()

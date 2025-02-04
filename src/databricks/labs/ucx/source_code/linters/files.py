from __future__ import annotations  # for type hints

import dataclasses
import locale
import logging
from collections.abc import Iterable
from pathlib import Path
from typing import cast

from astroid import Module, NodeNG  # type: ignore
from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationIndex
from databricks.labs.ucx.source_code.base import (
    CurrentSessionState,
    file_language,
    is_a_notebook,
    Advice,
    Failure,
    Advisory,
    safe_read_text,
    read_text,
)
from databricks.labs.ucx.source_code.linters.base import Linter
from databricks.labs.ucx.source_code.graph import Dependency
from databricks.labs.ucx.source_code.linters.context import LinterContext
from databricks.labs.ucx.source_code.linters.imports import SysPathChange, UnresolvedPath
from databricks.labs.ucx.source_code.notebooks.cells import (
    PythonCell,
    RunCell,
    RunCommand,
)
from databricks.labs.ucx.source_code.notebooks.loaders import NotebookLoader
from databricks.labs.ucx.source_code.notebooks.magic import MagicLine
from databricks.labs.ucx.source_code.notebooks.sources import Notebook
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.python.python_ast import Tree, MaybeTree, NodeBase
from databricks.labs.ucx.source_code.linters.python import PythonSequentialLinter

logger = logging.getLogger(__name__)


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
        maybe_tree = MaybeTree.from_source_code(python_cell.original_code)
        if maybe_tree.failure:
            yield maybe_tree.failure
        tree = maybe_tree.tree
        # a cell with only comments will not produce a tree
        if register_trees:
            self._python_trees[python_cell] = tree or Tree.new_module()
        if not tree:
            return
        yield from self._load_children_from_tree(tree)

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

    def _load_children_with_base_nodes(self, nodes: list[NodeNG], base_nodes: list[NodeBase]) -> Iterable[Advice]:
        for base_node in base_nodes:
            yield from self._load_children_with_base_node(nodes, base_node)

    def _load_children_with_base_node(self, nodes: list[NodeNG], base_node: NodeBase) -> Iterable[Advice]:
        while len(nodes) > 0:
            node = nodes.pop(0)
            self._python_linter.append_nodes([node])
            if node.lineno < base_node.node.lineno:
                continue
            yield from self._load_children_from_base_node(base_node)

    def _load_children_from_base_node(self, base_node: NodeBase) -> Iterable[Advice]:
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

    def _mutate_path_lookup(self, change: SysPathChange) -> Iterable[Advice]:
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

    def _linter(self, language: Language) -> Linter:
        if language is Language.PYTHON:
            return self._python_linter
        return self._context.linter(language)

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
                if self._inherited_tree is not None and isinstance(linter, PythonSequentialLinter):
                    linter.append_tree(self._inherited_tree)
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
            self._ctx,
            self._path_lookup,
            self._session_state,
            notebook,
            self._inherited_tree,
        )
        yield from notebook_linter.lint()


class NotebookMigrator:
    def __init__(self, languages: LinterContext):
        # TODO: move languages to `apply`
        self._languages = languages

    def revert(self, path: Path) -> bool:
        backup_path = path.with_suffix(".bak")
        if not backup_path.exists():
            return False
        return path.write_text(backup_path.read_text()) > 0

    def apply(self, path: Path) -> bool:
        if not path.exists():
            return False
        dependency = Dependency(NotebookLoader(), path)
        # TODO: the interface for this method has to be changed
        lookup = PathLookup.from_sys_path(Path.cwd())
        container = dependency.load(lookup)
        assert isinstance(container, Notebook)
        return self._apply(container)

    def _apply(self, notebook: Notebook) -> bool:
        changed = False
        for cell in notebook.cells:
            # %run is not a supported language, so this needs to come first
            if isinstance(cell, RunCell):
                # TODO migration data, see https://github.com/databrickslabs/ucx/issues/1327
                continue
            if not self._languages.is_supported(cell.language.language):
                continue
            migrated_code = self._languages.apply_fixes(cell.language.language, cell.original_code)
            if migrated_code != cell.original_code:
                cell.migrated_code = migrated_code
                changed = True
        if changed:
            # TODO https://github.com/databrickslabs/ucx/issues/1327 store 'migrated' status
            notebook.path.replace(notebook.path.with_suffix(".bak"))
            notebook.path.write_text(notebook.to_migrated_code())
        return changed

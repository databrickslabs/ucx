from __future__ import annotations  # for type hints

import dataclasses
import locale
import logging
from collections.abc import Iterable
from pathlib import Path
from typing import cast

from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.source_code.base import (
    file_language,
    is_a_notebook,
    Advice,
    Failure,
    safe_read_text,
    read_text,
)
from databricks.labs.ucx.source_code.graph import Dependency
from databricks.labs.ucx.source_code.linters.base import PythonLinter
from databricks.labs.ucx.source_code.linters.context import LinterContext
from databricks.labs.ucx.source_code.linters.imports import SysPathChange, UnresolvedPath
from databricks.labs.ucx.source_code.notebooks.cells import (
    Cell,
    PythonCell,
    RunCell,
    RunCommand,
)
from databricks.labs.ucx.source_code.notebooks.loaders import NotebookLoader
from databricks.labs.ucx.source_code.notebooks.magic import MagicLine
from databricks.labs.ucx.source_code.notebooks.sources import Notebook
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.python.python_ast import Tree, MaybeTree

logger = logging.getLogger(__name__)


class NotebookLinter:
    """
    Parses a Databricks notebook and then applies available linters
    to the code cells according to the language of the cell.
    """

    def __init__(
        self, context: LinterContext, path_lookup: PathLookup, notebook: Notebook, parent_tree: Tree | None = None
    ):
        self._context: LinterContext = context
        self._path_lookup = path_lookup
        self._notebook: Notebook = notebook
        self._parent_tree = parent_tree or Tree.new_module()

        # Python trees are constructed during notebook parsing and cached for later usage
        self._python_tree_cache: dict[tuple[Path, Cell], Tree] = {}  # Path in key is the notebook's path

    def lint(self) -> Iterable[Advice]:
        maybe_tree = self._parse_notebook(self._notebook, parent_tree=self._parent_tree)
        if maybe_tree and maybe_tree.failure:
            yield maybe_tree.failure
            return
        for cell in self._notebook.cells:
            try:
                linter = self._context.linter(cell.language.language)
            except ValueError:  # Language is not supported (yet)
                continue
            if isinstance(cell, PythonCell):
                linter = cast(PythonLinter, linter)
                tree = self._python_tree_cache[(self._notebook.path, cell)]
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

    def _parse_notebook(self, notebook: Notebook, *, parent_tree: Tree) -> MaybeTree | None:
        """Parse a notebook by parsing its cells.

        The notebook linter is designed to parse a valid tree for its notebook **only**. Possible child notebooks
        referenced by run cells are brought into the scope of this notebook, however, their trees are not valid complete
        for linting. The child notebooks are linted with another call to the notebook linter that includes the
        context(s) from which these notebooks are ran.

        Returns :
            MaybeTree | None : The tree or failure belonging to the **last** cell. If None, it signals that none of the
                cells contain Python code.
        """
        maybe_tree = None
        for cell in notebook.cells:
            if isinstance(cell, RunCell):
                maybe_tree = self._resolve_and_parse_run_cell(cell, parent_tree=parent_tree)
            elif isinstance(cell, PythonCell):
                maybe_tree = self._parse_python_cell(cell, parent_tree=parent_tree)
            if maybe_tree and maybe_tree.failure:
                return maybe_tree
            if maybe_tree and maybe_tree.tree:
                self._python_tree_cache[(notebook.path, cell)] = maybe_tree.tree
                parent_tree = maybe_tree.tree  # The subsequent cell gets the globals from the previous cell
        return maybe_tree

    def _resolve_and_parse_run_cell(self, run_cell: RunCell, *, parent_tree: Tree) -> MaybeTree | None:
        """Resolve the path in the run cell and parse the notebook it refers."""
        path = run_cell.maybe_notebook_path()
        if path is None:
            return None  # malformed run cell already reported
        notebook = self._resolve_and_parse_notebook_path(path)
        if not notebook:
            return None
        maybe_tree = self._parse_notebook(notebook, parent_tree=parent_tree)
        if maybe_tree and maybe_tree.tree:
            # From the perspective of this cell, a run cell pulls the globals from the child notebook in
            parent_tree.extend_globals(maybe_tree.tree.node.globals)
        return maybe_tree

    def _parse_python_cell(self, python_cell: PythonCell, *, parent_tree: Tree) -> MaybeTree:
        """Parse the Python cell."""
        failure: Failure | None
        maybe_tree = MaybeTree.from_source_code(python_cell.original_code)
        if maybe_tree.failure:
            failure = dataclasses.replace(
                maybe_tree.failure,
                start_line=maybe_tree.failure.start_line + python_cell.original_offset,
                end_line=maybe_tree.failure.end_line + python_cell.original_offset,
            )
            return MaybeTree(None, failure)
        assert maybe_tree.tree is not None
        maybe_tree.tree.extend_globals(parent_tree.node.globals)
        failure = self._parse_tree(maybe_tree.tree)
        if failure:
            return MaybeTree(None, failure)
        return maybe_tree

    def _parse_tree(self, tree: Tree) -> Failure | None:
        """Parse tree by looking for referred notebooks and path changes that might affect loading notebooks."""
        code_path_nodes = self._list_magic_lines_with_run_command(tree) + SysPathChange.extract_from_tree(
            self._context.session_state, tree
        )
        # Sys path changes require to load children in order of reading
        for base_node in sorted(code_path_nodes, key=lambda node: (node.node.lineno, node.node.col_offset)):
            failure = self._process_code_node(base_node, parent_tree=tree)
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

    def _process_code_node(self, node: SysPathChange | MagicLine, *, parent_tree: Tree) -> Failure | None:
        """Process a code node.

        1. `SysPathChange` mutate the path lookup.
        2. `MagicLine` containing a `RunCommand` run other notebooks that should be parsed.
        """
        if isinstance(node, SysPathChange):
            return self._mutate_path_lookup(node)
        if isinstance(node, MagicLine):
            magic = node.as_magic()
            assert isinstance(magic, RunCommand)
            notebook = self._resolve_and_parse_notebook_path(magic.notebook_path)
            if notebook is None:
                failure = Failure.from_node(
                    code='dependency-not-found',
                    message=f"Can't locate dependency: {magic.notebook_path}",
                    node=node.node,
                )
                return failure
            maybe_tree = self._parse_notebook(notebook, parent_tree=parent_tree)
            if not maybe_tree:
                return None
            if maybe_tree.tree:
                # From the perspective of this node, a run node pulls the globals from the child notebook in
                parent_tree.extend_globals(maybe_tree.tree.node.globals)
            return maybe_tree.failure
        return None

    def _mutate_path_lookup(self, change: SysPathChange) -> Failure | None:
        """Mutate the path lookup."""
        if isinstance(change, UnresolvedPath):
            return Failure.from_node(
                code='sys-path-cannot-compute-value',
                message=f"Can't update sys.path from {change.node.as_string()} because the expression cannot be computed",
                node=change.node,
            )
        change.apply_to(self._path_lookup)
        return None

    def _resolve_and_parse_notebook_path(self, path: Path | None) -> Notebook | None:
        """Resolve and parse notebook path."""
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
        context: LinterContext,
        path_lookup: PathLookup,
        path: Path,
        inherited_tree: Tree | None = None,
        content: str | None = None,
    ):
        self._context = context
        self._path_lookup = path_lookup
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
                linter = self._context.linter(language)
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
        notebook_linter = NotebookLinter(self._context, self._path_lookup, notebook, self._inherited_tree)
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

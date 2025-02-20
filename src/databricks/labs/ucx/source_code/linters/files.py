from __future__ import annotations  # for type hints

import dataclasses
import logging
from collections.abc import Iterable
from pathlib import Path
from typing import cast

from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.source_code.base import (
    Advice,
    Failure,
    infer_file_language_if_supported,
    safe_read_text,
)
from databricks.labs.ucx.source_code.files import LocalFile
from databricks.labs.ucx.source_code.graph import Dependency
from databricks.labs.ucx.source_code.known import KnownDependency
from databricks.labs.ucx.source_code.linters.base import PythonFixer, PythonLinter
from databricks.labs.ucx.source_code.linters.context import LinterContext
from databricks.labs.ucx.source_code.linters.imports import SysPathChange, UnresolvedPath
from databricks.labs.ucx.source_code.notebooks.cells import (
    Cell,
    PythonCell,
    RunCell,
    RunCommand,
)
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
        self,
        notebook: Notebook,
        path_lookup: PathLookup,
        context: LinterContext,
        parent_tree: Tree | None = None,
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

    def apply(self) -> None:
        """Apply changes to the notebook."""
        maybe_tree = self._parse_notebook(self._notebook, parent_tree=self._parent_tree)
        if maybe_tree and maybe_tree.failure:
            logger.warning("Failed to parse the notebook, run linter for more details.")
            return
        for cell in self._notebook.cells:
            try:
                linter = self._context.linter(cell.language.language)
            except ValueError:  # Language is not supported (yet)
                continue
            fixed_code = cell.original_code  # For default fixing
            tree = self._python_tree_cache.get((self._notebook.path, cell))  # For Python fixing
            is_python_cell = isinstance(cell, PythonCell)
            if is_python_cell and tree:
                advices = cast(PythonLinter, linter).lint_tree(tree)
            else:
                advices = linter.lint(cell.original_code)
            for advice in advices:
                fixer = self._context.fixer(cell.language.language, advice.code)
                if not fixer:
                    continue
                if is_python_cell and tree:
                    # By calling `apply_tree` instead of `apply`, we chain fixes on the same tree
                    tree = cast(PythonFixer, fixer).apply_tree(tree)
                else:
                    fixed_code = fixer.apply(fixed_code)
            cell.migrated_code = tree.node.as_string() if tree else fixed_code
        self._notebook.back_up_original_and_flush_migrated_code()
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
        language = infer_file_language_if_supported(resolved)
        # we only support Python notebooks for now
        if language != Language.PYTHON:
            logger.warning(f"Unsupported notebook language: {language}")
            return None
        source = safe_read_text(resolved)
        if not source:
            return None
        return Notebook.parse(path, source, language)


class FileLinter:

    def __init__(
        self,
        dependency: Dependency,
        path_lookup: PathLookup,
        context: LinterContext,
        inherited_tree: Tree | None = None,
    ):
        self._dependency = dependency
        self._path_lookup = path_lookup
        self._context = context
        self._inherited_tree = inherited_tree

    def lint(self) -> Iterable[Advice]:
        """Lint the file."""
        if isinstance(self._dependency, KnownDependency):
            # TODO: Pass on the right advice type (https://github.com/databrickslabs/ucx/issues/3625)
            advices = [problem.as_advice().as_advisory() for problem in self._dependency.problems]
            yield from advices
            return
        source_container = self._dependency.load(self._path_lookup)
        if not source_container:
            # The linter only reports **linting** errors, not loading errors
            return
        if isinstance(source_container, Notebook):
            yield from self._lint_notebook(source_container)
        elif isinstance(source_container, LocalFile):
            yield from self._lint_file(source_container)
        else:
            yield Failure("unsupported-file", "Unsupported file", -1, -1, -1, -1)

    def _lint_file(self, local_file: LocalFile) -> Iterable[Advice]:
        """Lint a local file."""
        try:
            linter = self._context.linter(local_file.language)
            yield from linter.lint(local_file.original_code)
        except ValueError:
            # TODO: Remove when implementing: https://github.com/databrickslabs/ucx/issues/3544
            yield Failure("unsupported-language", f"Unsupported language: {local_file.language}", -1, -1, -1, -1)

    def _lint_notebook(self, notebook: Notebook) -> Iterable[Advice]:
        """Lint a notebook."""
        notebook_linter = NotebookLinter(notebook, self._path_lookup, self._context, self._inherited_tree)
        yield from notebook_linter.lint()

    def apply(self) -> None:
        """Apply changes to the file."""
        source_container = self._dependency.load(self._path_lookup)
        if isinstance(source_container, LocalFile):
            self._apply_file(source_container)
        elif isinstance(source_container, Notebook):
            self._apply_notebook(source_container)

    def _apply_file(self, local_file: LocalFile) -> None:
        """Apply changes to a local file."""
        fixed_code = self._context.apply_fixes(local_file.language, local_file.original_code)
        local_file.migrated_code = fixed_code
        local_file.back_up_original_and_flush_migrated_code()

    def _apply_notebook(self, notebook: Notebook) -> None:
        """Apply changes to a notebook."""
        notebook_linter = NotebookLinter(notebook, self._path_lookup, self._context, self._inherited_tree)
        notebook_linter.apply()

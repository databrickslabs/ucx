from __future__ import annotations  # for type hints

import dataclasses
import logging
from collections.abc import Iterable, Callable
from pathlib import Path
import sys
from typing import TextIO, cast

from astroid import Module, NodeNG  # type: ignore

from databricks.labs.ucx.source_code.base import (
    LocatedAdvice,
    CurrentSessionState,
    is_a_notebook,
    Advice,
    Failure,
    Advisory,
    file_language,
    safe_read_text,
    Linter,
)
from databricks.labs.ucx.source_code.files import FileLoader, LocalFile
from databricks.labs.ucx.source_code.linters.imports import SysPathChange, UnresolvedPath
from databricks.labs.ucx.source_code.notebooks.cells import PythonCell, RunCell, RunCommand
from databricks.labs.ucx.source_code.notebooks.magic import MagicLine
from databricks.labs.ucx.source_code.python.python_ast import Tree, PythonSequentialLinter, MaybeTree, NodeBase
from databricks.labs.ucx.source_code.notebooks.loaders import NotebookLoader
from databricks.labs.ucx.source_code.notebooks.sources import Notebook
from databricks.labs.ucx.source_code.path_lookup import PathLookup
from databricks.labs.ucx.source_code.known import KnownList
from databricks.sdk.service.workspace import Language
from databricks.labs.blueprint.tui import Prompts

from databricks.labs.ucx.source_code.linters.context import LinterContext
from databricks.labs.ucx.source_code.graph import (
    BaseImportResolver,
    BaseFileResolver,
    Dependency,
    DependencyGraph,
    DependencyLoader,
    DependencyProblem,
    MaybeDependency,
    SourceContainer,
    DependencyResolver,
    DependencyGraphWalker,
)

logger = logging.getLogger(__name__)


class Folder(SourceContainer):

    def __init__(
        self,
        path: Path,
        notebook_loader: NotebookLoader,
        file_loader: FileLoader,
        folder_loader: FolderLoader,
    ):
        self._path = path
        self._notebook_loader = notebook_loader
        self._file_loader = file_loader
        self._folder_loader = folder_loader

    @property
    def path(self) -> Path:
        return self._path

    def build_dependency_graph(self, parent: DependencyGraph) -> list[DependencyProblem]:
        # don't directly scan non-source directories, let it be done for relevant imports only
        if self._path.name in {"__pycache__", ".git", ".github", ".venv", ".mypy_cache", "site-packages"}:
            return []
        return list(self._build_dependency_graph(parent))

    def _build_dependency_graph(self, parent: DependencyGraph) -> Iterable[DependencyProblem]:
        for child_path in self._path.iterdir():
            is_file = child_path.is_file()
            is_notebook = is_a_notebook(child_path)
            loader = self._notebook_loader if is_notebook else self._file_loader if is_file else self._folder_loader
            dependency = Dependency(loader, child_path, inherits_context=is_notebook)
            yield from parent.register_dependency(dependency).problems

    def __repr__(self):
        return f"<Folder {self._path}>"


class LocalCodeLinter:

    def __init__(
        self,
        notebook_loader: NotebookLoader,
        file_loader: FileLoader,
        folder_loader: FolderLoader,
        path_lookup: PathLookup,
        session_state: CurrentSessionState,
        dependency_resolver: DependencyResolver,
        context_factory: Callable[[], LinterContext],
    ) -> None:
        self._notebook_loader = notebook_loader
        self._file_loader = file_loader
        self._folder_loader = folder_loader
        self._path_lookup = path_lookup
        self._session_state = session_state
        self._dependency_resolver = dependency_resolver
        self._extensions = {".py": Language.PYTHON, ".sql": Language.SQL}
        self._context_factory = context_factory

    def lint(
        self,
        prompts: Prompts,
        path: Path | None,
        stdout: TextIO = sys.stdout,
    ) -> list[LocatedAdvice]:
        """Lint local code files looking for problems in notebooks and python files."""
        if path is None:
            response = prompts.question(
                "Which file or directory do you want to lint?",
                default=Path.cwd().as_posix(),
                validate=lambda p_: Path(p_).exists(),
            )
            path = Path(response)
        located_advices = list(self.lint_path(path))
        for located_advice in located_advices:
            stdout.write(f"{located_advice}\n")
        return located_advices

    def lint_path(self, path: Path, linted_paths: set[Path] | None = None) -> Iterable[LocatedAdvice]:
        is_dir = path.is_dir()
        loader: DependencyLoader
        if is_a_notebook(path):
            loader = self._notebook_loader
        elif path.is_dir():
            loader = self._folder_loader
        else:
            loader = self._file_loader
        path_lookup = self._path_lookup.change_directory(path if is_dir else path.parent)
        root_dependency = Dependency(loader, path, not is_dir)  # don't inherit context when traversing folders
        graph = DependencyGraph(root_dependency, None, self._dependency_resolver, path_lookup, self._session_state)
        container = root_dependency.load(path_lookup)
        assert container is not None  # because we just created it
        problems = container.build_dependency_graph(graph)
        for problem in problems:
            yield problem.as_located_advice()
            return  # If dependency graph is not complete, we will not lint it's contents
        context_factory = self._context_factory

        class LintingWalker(DependencyGraphWalker[LocatedAdvice]):

            def _process_dependency(
                self, dependency: Dependency, path_lookup: PathLookup, inherited_tree: Tree | None
            ) -> Iterable[LocatedAdvice]:
                ctx = context_factory()
                # FileLinter will determine which file/notebook linter to use
                linter = FileLinter(dependency, path_lookup, ctx, inherited_tree=inherited_tree)
                for advice in linter.lint():
                    yield LocatedAdvice(advice, dependency.path)

        if linted_paths is None:
            linted_paths = set()
        walker = LintingWalker(graph, linted_paths, self._path_lookup)
        yield from walker


class LocalFileMigrator:
    """The LocalFileMigrator class is responsible for fixing code files based on their language."""

    def __init__(self, context_factory: Callable[[], LinterContext]):
        self._extensions = {".py": Language.PYTHON, ".sql": Language.SQL}
        self._context_factory = context_factory

    def apply(self, path: Path) -> bool:
        if path.is_dir():
            for child_path in path.iterdir():
                self.apply(child_path)
            return True
        return self._apply_file_fix(path)

    def _apply_file_fix(self, path: Path):
        """
        The fix method reads a file, lints it, applies fixes, and writes the fixed code back to the file.
        """
        # Check if the file extension is in the list of supported extensions
        if path.suffix not in self._extensions:
            return False
        # Get the language corresponding to the file extension
        language = self._extensions[path.suffix]
        # If the language is not supported, return
        if not language:
            return False
        logger.info(f"Analysing {path}")
        # Get the linter for the language
        context = self._context_factory()
        linter = context.linter(language)
        # Open the file and read the code
        with path.open("r") as f:
            try:
                code = f.read()
            except UnicodeDecodeError as e:
                logger.warning(f"Could not decode file {path}: {e}")
                return False
            applied = False
            # Lint the code and apply fixes
            for advice in linter.lint(code):
                logger.info(f"Found: {advice}")
                fixer = context.fixer(language, advice.code)
                if not fixer:
                    continue
                logger.info(f"Applying fix for {advice}")
                code = fixer.apply(code)
                applied = True
            if not applied:
                return False
            # Write the fixed code back to the file
            with path.open("w") as f:
                logger.info(f"Overwriting {path}")
                f.write(code)
                return True


class FolderLoader(DependencyLoader):

    def __init__(self, notebook_loader: NotebookLoader, file_loader: FileLoader):
        self._notebook_loader = notebook_loader
        self._file_loader = file_loader

    def load_dependency(self, path_lookup: PathLookup, dependency: Dependency) -> Folder | None:
        absolute_path = path_lookup.resolve(dependency.path)
        if not absolute_path:
            return None
        return Folder(absolute_path, self._notebook_loader, self._file_loader, self)


class ModuleDependency(Dependency):
    """A dependency representing a (Python) module.

    Next to the default parameters, this dependency has a module name.
    """

    _MISSING_SOURCE_PATH = Path("<MISSING_SOURCE_PATH>")

    def __init__(self, loader: DependencyLoader, *, path: Path | None = None, module_name: str, known: bool) -> None:
        super().__init__(loader, path or self._MISSING_SOURCE_PATH, inherits_context=False)
        self._module_name = module_name
        self.known = known

        if self._path == self._MISSING_SOURCE_PATH and not self.known:
            assert ValueError("Only known modules can have a missing source path.")

    def __repr__(self) -> str:
        return f"ModuleDependency<{self._module_name} ({self._path})>"


class ImportFileResolver(BaseImportResolver, BaseFileResolver):
    """Resolve module import

    TODO: Split into two classes
    """

    def __init__(self, file_loader: FileLoader, *, allow_list: KnownList | None = None):
        super().__init__()
        self._file_loader = file_loader

        self._allow_list = allow_list or KnownList()

    def resolve_file(self, path_lookup, path: Path) -> MaybeDependency:
        """Locates a file."""
        absolute_path = path_lookup.resolve(path)
        if absolute_path:
            return MaybeDependency(Dependency(self._file_loader, absolute_path), [])
        problem = DependencyProblem("file-not-found", f"File not found: {path.as_posix()}")
        return MaybeDependency(None, [problem])

    def resolve_import(self, path_lookup: PathLookup, name: str) -> MaybeDependency:
        """Resolve a simple or composite import name."""
        if not name:
            return MaybeDependency(None, [DependencyProblem("ucx-bug", "Import name is empty")])
        known = self._allow_list.module_compatibility(name).known
        parts = []
        # Relative imports use leading dots. A single leading dot indicates a relative import, starting with
        # the current package. Two or more leading dots indicate a relative import to the parent(s) of the current
        # package, one level per dot after the first.
        # see https://docs.python.org/3/reference/import.html#package-relative-imports
        for i, rune in enumerate(name):
            if not i and rune == '.':  # leading single dot
                parts.append(path_lookup.cwd.as_posix())
                continue
            if rune != '.':
                parts.append(name[i:].replace('.', '/'))
                break
            parts.append("..")
        for candidate in (f'{"/".join(parts)}.py', f'{"/".join(parts)}/__init__.py'):
            relative_path = Path(candidate)
            absolute_path = path_lookup.resolve(relative_path)
            if not absolute_path:
                continue
            dependency = ModuleDependency(self._file_loader, path=absolute_path, module_name=name, known=known)
            return MaybeDependency(dependency, [])
        if known:
            # For known modules, we allow the source path to be missing, however, we do not return early as we prefer
            # to find the source path for reporting reasons
            dependency = ModuleDependency(self._file_loader, module_name=name, known=known)
            return MaybeDependency(dependency, [])
        return MaybeDependency(None, [DependencyProblem("import-not-found", f"Could not locate import: {name}")])

    def __repr__(self):
        return "ImportFileResolver()"


class NotebookLinter:
    """
    Parses a Databricks notebook and then applies available linters
    to the code cells according to the language of the cell.
    """

    def __init__(
        self, notebook: Notebook, path_lookup: PathLookup, context: LinterContext, inherited_tree: Tree | None = None
    ):
        self._context: LinterContext = context
        self._path_lookup = path_lookup
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
        base_nodes.extend(SysPathChange.extract_from_tree(self._context.session_state, tree))
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
        dependency: Dependency,
        path_lookup: PathLookup,
        context: LinterContext,
        *,
        allow_list: KnownList | None = None,
        inherited_tree: Tree | None = None,
    ):
        self._dependency = dependency
        self._path_lookup = path_lookup
        self._context = context
        self._allow_list = allow_list or KnownList()
        self._inherited_tree = inherited_tree

    def lint(self) -> Iterable[Advice]:
        """Lint the file."""
        if self._dependency.path.suffix.lower() in self._IGNORED_SUFFIXES:
            return
        if self._dependency.path.name.lower() in self._IGNORED_NAMES:
            return
        if self._dependency.path.suffix.lower() in self._NOT_YET_SUPPORTED_SUFFIXES:
            message = f"Unsupported language for suffix: {self._dependency.path.suffix}"
            yield Failure("unsupported-language", message, -1, -1, -1, -1)
            return
        source_container = self._dependency.load(self._path_lookup)
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
            if self._inherited_tree is not None and isinstance(linter, PythonSequentialLinter):
                linter.append_tree(self._inherited_tree)
            yield from linter.lint(local_file.content)
        except ValueError:
            # TODO: Remove when implementing: https://github.com/databrickslabs/ucx/issues/3544
            yield Failure("unsupported-language", f"Unsupported language: {local_file.language}", -1, -1, -1, -1)

    def _lint_notebook(self, notebook: Notebook) -> Iterable[Advice]:
        """Lint a notebook."""
        notebook_linter = NotebookLinter(notebook, self._path_lookup, self._context, self._inherited_tree)
        yield from notebook_linter.lint()

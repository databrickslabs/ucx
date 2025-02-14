from __future__ import annotations

import logging
import sys
from collections.abc import Callable, Iterable
from pathlib import Path
from typing import TextIO

from databricks.labs.blueprint.tui import Prompts
from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.source_code.base import CurrentSessionState, LocatedAdvice, is_a_notebook
from databricks.labs.ucx.source_code.graph import DependencyResolver, DependencyLoader, Dependency, DependencyGraph
from databricks.labs.ucx.source_code.linters.context import LinterContext
from databricks.labs.ucx.source_code.folders import FolderLoader
from databricks.labs.ucx.source_code.files import FileLoader
from databricks.labs.ucx.source_code.linters.graph_walkers import LinterWalker
from databricks.labs.ucx.source_code.notebooks.loaders import NotebookLoader
from databricks.labs.ucx.source_code.path_lookup import PathLookup


logger = logging.getLogger(__name__)


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

    def lint_path(self, path: Path) -> Iterable[LocatedAdvice]:
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
            return
        walker = LinterWalker(graph, self._path_lookup, self._context_factory)
        yield from walker

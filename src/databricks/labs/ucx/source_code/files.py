from __future__ import annotations  # for type hints

import logging
from pathlib import Path

from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.source_code.dependencies import SourceContainer, DependencyGraph, Dependency, DependencyType
from databricks.labs.ucx.source_code.languages import Languages
from databricks.labs.ucx.source_code.notebook import CellLanguage
from databricks.labs.ucx.source_code.python_linter import PythonLinter, ASTLinter


logger = logging.getLogger(__name__)


class SourceFile(SourceContainer):

    def __init__(self, path: str, source: str, language: Language):
        self._path = path
        self._original_code = source
        # using CellLanguage so we can reuse the facilities it provides
        self._language = CellLanguage.of_language(language)

    @property
    def dependency_type(self) -> DependencyType:
        return DependencyType.FILE

    def build_dependency_graph(self, graph: DependencyGraph) -> None:
        if self._language is not CellLanguage.PYTHON:
            logger.warning(f"Unsupported language: {self._language.language}")
        linter = ASTLinter.parse(self._original_code)
        run_notebook_calls = PythonLinter.list_dbutils_notebook_run_calls(linter)
        notebook_paths = {PythonLinter.get_dbutils_notebook_run_path_arg(call) for call in run_notebook_calls}
        for path in notebook_paths:
            graph.register_dependency(Dependency(DependencyType.NOTEBOOK, path))
        # TODO https://github.com/databrickslabs/ucx/issues/1287
        import_names = PythonLinter.list_import_sources(linter)
        for import_name in import_names:
            # we don't know yet the type of the dependency
            graph.register_dependency(Dependency(None, import_name))


class WorkspaceFile(SourceFile):
    pass


class LocalFile(SourceFile):
    pass


class LocalFileMigrator:
    """The LocalFileMigrator class is responsible for fixing code files based on their language."""

    def __init__(self, languages: Languages):
        self._languages = languages
        self._extensions = {".py": Language.PYTHON, ".sql": Language.SQL}

    def apply(self, path: Path) -> bool:
        if path.is_dir():
            for child_path in path.iterdir():
                self.apply(child_path)
            return True
        return self._apply_file_fix(path)

    def _apply_file_fix(self, path):
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
        linter = self._languages.linter(language)
        # Open the file and read the code
        with path.open("r") as f:
            code = f.read()
            applied = False
            # Lint the code and apply fixes
            for advice in linter.lint(code):
                logger.info(f"Found: {advice}")
                fixer = self._languages.fixer(language, advice.code)
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

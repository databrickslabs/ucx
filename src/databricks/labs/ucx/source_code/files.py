from __future__ import annotations  # for type hints

import ast
import logging
import sys
from pathlib import Path

from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.source_code.dependencies import (
    DependencyGraph,
    DependencyProblem,
)
from databricks.labs.ucx.source_code.dependency_loaders import SourceContainer
from databricks.labs.ucx.source_code.languages import Languages
from databricks.labs.ucx.source_code.notebook import CellLanguage
from databricks.labs.ucx.source_code.python_linter import PythonLinter, ASTLinter


logger = logging.getLogger(__name__)


class LocalFile(SourceContainer):

    def __init__(self, path: Path, source: str, language: Language):
        self._path = path
        self._original_code = source
        # using CellLanguage so we can reuse the facilities it provides
        self._language = CellLanguage.of_language(language)

    def build_dependency_graph(self, parent: DependencyGraph) -> None:
        if self._language is not CellLanguage.PYTHON:
            logger.warning(f"Unsupported language: {self._language.language}")
            return
        # TODO replace the below with parent.build_graph_from_python_source
        # can only be done after https://github.com/databrickslabs/ucx/issues/1287
        linter = ASTLinter.parse(self._original_code)
        run_notebook_calls = PythonLinter.list_dbutils_notebook_run_calls(linter)
        for call in run_notebook_calls:
            call_problems: list[DependencyProblem] = []
            notebook_path_arg = PythonLinter.get_dbutils_notebook_run_path_arg(call)
            if isinstance(notebook_path_arg, ast.Constant):
                notebook_path = notebook_path_arg.value
                parent.register_notebook(Path(notebook_path), call_problems.append)
                call_problems = [
                    problem.replace(
                        source_path=self._path,
                        start_line=call.lineno,
                        start_col=call.col_offset,
                        end_line=call.end_lineno or 0,
                        end_col=call.end_col_offset or 0,
                    )
                    for problem in call_problems
                ]
                parent.add_problems(call_problems)
                continue
            problem = DependencyProblem(
                code='dependency-check',
                message="Can't check dependency not provided as a constant",
                source_path=self._path,
                start_line=call.lineno,
                start_col=call.col_offset,
                end_line=call.end_lineno or 0,
                end_col=call.end_col_offset or 0,
            )
            parent.add_problems([problem])
        # TODO https://github.com/databrickslabs/ucx/issues/1287
        in_site_packages = "site-packages" in parent.dependency.path.as_posix()
        sys_module_keys = sys.modules.keys()
        for pair in PythonLinter.list_import_sources(linter):
            import_name = pair[0]
            # TODO remove HORRIBLE hack until we implement https://github.com/databrickslabs/ucx/issues/1421
            # if it's a site-package, provide full path until we implement 1421
            if in_site_packages and import_name not in sys_module_keys:
                import_name = Path(parent.dependency.path.parent, import_name + ".py").as_posix()
            import_problems: list[DependencyProblem] = []
            parent.register_import(import_name, import_problems.append)
            node = pair[1]
            import_problems = [
                problem.replace(
                    source_path=self._path,
                    start_line=node.lineno,
                    start_col=node.col_offset,
                    end_line=node.end_lineno or 0,
                    end_col=node.end_col_offset or 0,
                )
                for problem in import_problems
            ]
            parent.add_problems(import_problems)


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

import logging
from pathlib import Path

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.hive_metastore.table_migrate import TablesMigrate
from databricks.labs.ucx.source_code.languages import Languages

logger = logging.getLogger(__name__)


class Files:
    """The Files class is responsible for fixing code files based on their language."""

    def __init__(self, languages: Languages):
        self._languages = languages
        self._extensions = {".py": Language.PYTHON, ".sql": Language.SQL}

    @classmethod
    def for_cli(cls, ws: WorkspaceClient):
        tables_migrate = TablesMigrate.for_cli(ws)
        index = tables_migrate.index()
        languages = Languages(index)
        return cls(languages)

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

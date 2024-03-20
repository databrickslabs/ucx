from pathlib import Path

from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.code.languages import Languages


class Files:
    """The Files class is responsible for fixing code files based on their language."""

    def __init__(self, languages: Languages):
        self._languages = languages
        self._extensions = {"py": Language.PYTHON, "sql": Language.SQL}

    def fix(self, path: Path) -> bool:
        """
        The fix method reads a file, lints it, applies fixes, and writes the fixed code back to the file.
        """
        # Check if the file extension is in the list of supported extensions
        if path.suffix[1:] not in self._extensions:
            return False
        # Get the language corresponding to the file extension
        language = self._extensions[path.suffix[1:]]
        # If the language is not supported, return
        if not language:
            return False
        # Get the linter for the language
        linter = self._languages.linter(language)
        # Open the file and read the code
        with path.open("r") as f:
            code = f.read()
            applied = False
            # Lint the code and apply fixes
            for diagnostic in linter.lint(code):
                fixer = self._languages.fixer(language, diagnostic.code)
                if not fixer:
                    continue
                code = fixer.apply(code)
                applied = True
            if not applied:
                return False
            # Write the fixed code back to the file
            with path.open("w") as f:
                f.write(code)
                return True

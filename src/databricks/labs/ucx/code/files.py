from pathlib import Path

from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.code.languages import Languages


class Files:
    def __init__(self, languages: Languages):
        self._languages = languages
        self._extensions = {"py": Language.PYTHON, "sql": Language.SQL}

    def fix(self, path: Path):
        if path.suffix[1:] not in self._extensions:
            return
        language = self._extensions[path.suffix[1:]]
        if not language:
            return
        fixer = self._languages.fixer(language)
        with open(path, "r") as f:
            code = f.read()
        if fixer.match(code):
            code = fixer.apply(code)
            with open(path, "w") as f:
                f.write(code)

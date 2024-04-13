from collections.abc import Iterable

from databricks.labs.ucx.source_code.base import Advice
from databricks.labs.ucx.source_code.notebook import Notebook
from databricks.labs.ucx.source_code.languages import Languages


class NotebookLinter:
    """
    Parses a Databricks notebook and then applies available linters
    to the code cells according to the language of the cell.
    """

    def __init__(self, languages: Languages):
        self._languages = languages

    def lint(self, notebook: Notebook) -> Iterable[Advice]:
        for cell in notebook.cells:
            if not self._languages.is_supported(cell.language.language):
                continue
            linter = self._languages.linter(cell.language.language)
            yield from linter.lint(cell.original_code)

    def name(self) -> str:
        return "notebook-linter"

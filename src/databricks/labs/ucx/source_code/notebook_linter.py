from collections.abc import Iterable

from databricks.labs.ucx.source_code.base import Advice
from databricks.labs.ucx.source_code.notebook import Notebook
from databricks.labs.ucx.source_code import languages
from databricks.labs.ucx.source_code.languages import Languages, Language


class NotebookLinter:
    """
    Parses a Databricks notebook and then applies available linters
    to the code cells according to the language of the cell.
    """

    def __init__(self, langs: Languages, notebook: Notebook):
        self._languages = langs
        self._notebook = notebook

    @classmethod
    def from_notebook(cls, langs: Languages, notebook: Notebook) -> 'NotebookLinter':
        return cls(langs, notebook)

    @classmethod
    def from_source(
        cls, langs: Languages, source: str, default_language: Language = languages.Language.SQL
    ) -> 'NotebookLinter':
        notebook = Notebook.parse("", source, default_language)
        assert notebook is not None
        return cls(langs, notebook)

    @classmethod
    def from_path(cls, langs: Languages, path: str) -> 'NotebookLinter':
        with open(path, "r", encoding="utf-8") as f:
            source = f.read()
        return cls.from_source(langs, source)

    def lint(self) -> Iterable[Advice]:
        for cell in self._notebook.cells:
            if not self._languages.is_supported(cell.language.language):
                continue
            linter = self._languages.linter(cell.language.language)
            yield from linter.lint(cell.original_code)

    @staticmethod
    def name() -> str:
        return "notebook-linter"

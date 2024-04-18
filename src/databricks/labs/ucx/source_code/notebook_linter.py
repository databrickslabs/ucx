from collections.abc import Iterable

from databricks.labs.ucx.source_code.base import Advice, Linter
from databricks.labs.ucx.source_code.notebook import Notebook
from databricks.labs.ucx.source_code.languages import Languages, Language


class NotebookLinter(Linter):
    """
    Parses a Databricks notebook and then applies available linters
    to the code cells according to the language of the cell.
    """

    def __init__(self, langs: Languages, notebook: Notebook, schema: str | None = None):
        self._languages: Languages = langs
        self._notebook: Notebook = notebook
        self._schema: str | None = schema

    @classmethod
    def from_source(cls, langs: Languages, source: str, default_language: Language) -> 'NotebookLinter':
        notebook = Notebook.parse("", source, default_language)
        assert notebook is not None
        return cls(langs, notebook)


    def lint(self, code:str|None = None, schema: str| None = None) -> Iterable[Advice]:
        for cell in self._notebook.cells:
            if not self._languages.is_supported(cell.language.language):
                continue
            linter = self._languages.linter(cell.language.language)
            for advice in linter.lint(cell.original_code, self._schema):
                yield advice.replace(
                    start_line=advice.start_line + cell.original_offset, end_line=advice.end_line + cell.original_offset
                )
            _schema = linter.schema

    @staticmethod
    def name() -> str:
        return "notebook-linter"

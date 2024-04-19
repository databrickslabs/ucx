from collections.abc import Iterable

from databricks.labs.ucx.source_code.base import Advice, Linter, DEFAULT_SCHEMA
from databricks.labs.ucx.source_code.notebook import Notebook
from databricks.labs.ucx.source_code.languages import Languages, Language


class NotebookLinter(Linter):
    """
    Parses a Databricks notebook and then applies available linters
    to the code cells according to the language of the cell.
    """

    def __init__(self, langs: Languages, notebook: Notebook):
        self._languages: Languages = langs
        self._notebook: Notebook = notebook
        self._schema: str = DEFAULT_SCHEMA

    @property
    def schema(self):
        # This linter is a controller of a number of linters for each cell
        # language in the notebook. It is not generally useful to know the schema
        # of the last cell that was linted, except for testing purposes.
        return self._schema

    @classmethod
    def from_source(cls, langs: Languages, source: str, default_language: Language) -> 'NotebookLinter':
        notebook = Notebook.parse("", source, default_language)
        assert notebook is not None
        return cls(langs, notebook)

    def lint(self, _: str, schema: str) -> Iterable[Advice]:
        self._schema = schema
        for cell in self._notebook.cells:
            if not self._languages.is_supported(cell.language.language):
                continue
            linter = self._languages.linter(cell.language.language)
            # We are tracking the schema of the notebook as it changes cell by cell
            for advice in linter.lint(cell.original_code, self._schema):
                yield advice.replace(
                    start_line=advice.start_line + cell.original_offset, end_line=advice.end_line + cell.original_offset
                )
            # pass on the resulting schema to the next linter as this cell may
            # have changed the schema
            _schema = linter.schema

    @staticmethod
    def name() -> str:
        return "notebook-linter"

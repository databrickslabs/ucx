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
        self._languages: Languages = langs
        self._notebook: Notebook = notebook
        self._cell_offsets: list[int] = []

    @classmethod
    def from_source(
        cls, langs: Languages, source: str, default_language: Language = languages.Language.SQL
    ) -> 'NotebookLinter':
        notebook = Notebook.parse("", source, default_language)
        assert notebook is not None
        return cls(langs, notebook)

    def lint(self) -> Iterable[Advice]:
        for cell in self._notebook.cells:
            if not self._languages.is_supported(cell.language.language):
                continue
            linter = self._languages.linter(cell.language.language)
            for advice in linter.lint(cell.original_code):
                yield advice
                self._cell_offsets.append(cell.original_offset)

    def adjust_advices(self, advices: list[Advice]) -> Iterable[Advice]:
        for advice, offset in zip(advices, self._cell_offsets):
            advice = advice.replace(start_line=advice.start_line + offset, end_line=advice.end_line + offset)
            yield advice

    @staticmethod
    def name() -> str:
        return "notebook-linter"

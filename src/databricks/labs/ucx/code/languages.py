from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.code.base import Fixer, Linter, SequentialLinter
from databricks.labs.ucx.code.pyspark import SparkSql
from databricks.labs.ucx.code.queries import FromTable
from databricks.labs.ucx.hive_metastore.table_migrate import Index


class Languages:
    def __init__(self, index: Index):
        self._index = index
        from_table = FromTable(index)
        self._analysers = {
            Language.PYTHON: SequentialLinter([SparkSql(from_table)]),
            Language.SQL: SequentialLinter([from_table]),
        }
        self._fixers: dict[Language, list[Fixer]] = {
            Language.PYTHON: [SparkSql(from_table)],
            Language.SQL: [from_table],
        }

    def is_supported(self, language: Language) -> bool:
        return language in self._analysers and language in self._fixers

    def linter(self, language: Language) -> Linter:
        if language not in self._analysers:
            raise ValueError(f"Unsupported language: {language}")
        return self._analysers[language]

    def fixer(self, language: Language, diagnostic_code: str) -> Fixer | None:
        if language not in self._fixers:
            return None
        for fixer in self._fixers[language]:
            if fixer.name() == diagnostic_code:
                return fixer
        return None

    def apply_fixes(self, language: Language, code: str) -> str:
        linter = self.linter(language)
        for advice in linter.lint(code):
            fixer = self.fixer(language, advice.code)
            if fixer:
                code = fixer.apply(code)
        return code

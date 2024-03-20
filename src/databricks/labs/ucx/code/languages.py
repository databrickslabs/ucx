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
        self._fixers = {
            Language.PYTHON: [SparkSql(from_table)],
            Language.SQL: [from_table],
        }

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

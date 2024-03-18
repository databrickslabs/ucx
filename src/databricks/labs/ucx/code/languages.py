from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.code.base import Fixer, SequentialFixer, Analyser, SequentialAnalyser
from databricks.labs.ucx.code.pyspark import SparkSql
from databricks.labs.ucx.code.queries import FromTable
from databricks.labs.ucx.hive_metastore.table_migrate import Index

_EXT = {
    "py": Language.PYTHON,
    "sql": Language.SQL,
}


class Languages:
    def __init__(self, index: Index):
        self._index = index
        from_table = FromTable(index)
        self._analysers = {
            Language.PYTHON: SequentialAnalyser([SparkSql(from_table)]),
            Language.SQL: SequentialAnalyser([from_table]),
        }
        self._fixers = {
            Language.PYTHON: SequentialFixer([SparkSql(from_table)]),
            Language.SQL: SequentialFixer([from_table]),
        }

    def analyser(self, language: Language) -> Analyser:
        if language not in self._analysers:
            raise ValueError(f"Unsupported language: {language}")
        return self._analysers[language]

    def fixer(self, language: Language) -> Fixer:
        if language not in self._fixers:
            raise ValueError(f"Unsupported language: {language}")
        return self._fixers[language]

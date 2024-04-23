from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.hive_metastore.migration_status import MigrationIndex
from databricks.labs.ucx.source_code.base import Fixer, Linter, SequentialLinter, CurrentSessionState
from databricks.labs.ucx.source_code.pyspark import SparkSql
from databricks.labs.ucx.source_code.queries import FromTable
from databricks.labs.ucx.source_code.dbfs import DBFSUsageLinter, FromDbfsFolder
from databricks.labs.ucx.source_code.table_creation import DBRv8d0Linter


class Languages:
    def __init__(self, index: MigrationIndex):
        self._index = index
        session_state = CurrentSessionState()
        from_table = FromTable(index, session_state=session_state)
        dbfs_from_folder = FromDbfsFolder()
        self._linters = {
            Language.PYTHON: SequentialLinter(
                [
                    SparkSql(from_table, index),
                    DBFSUsageLinter(),
                    DBRv8d0Linter(dbr_version=None),
                ]
            ),
            Language.SQL: SequentialLinter([from_table, dbfs_from_folder]),
        }
        self._fixers: dict[Language, list[Fixer]] = {
            Language.PYTHON: [SparkSql(from_table, index)],
            Language.SQL: [from_table],
        }

    def is_supported(self, language: Language) -> bool:
        return language in self._linters and language in self._fixers

    def linter(self, language: Language) -> Linter:
        if language not in self._linters:
            raise ValueError(f"Unsupported language: {language}")
        return self._linters[language]

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

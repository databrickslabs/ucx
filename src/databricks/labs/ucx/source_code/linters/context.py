from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.hive_metastore.migration_status import MigrationIndex
from databricks.labs.ucx.source_code.base import Fixer, Linter, SequentialLinter, CurrentSessionState
from databricks.labs.ucx.source_code.linters.dbfs import FromDbfsFolder, DBFSUsageLinter
from databricks.labs.ucx.source_code.linters.imports import DbutilsLinter

from databricks.labs.ucx.source_code.linters.pyspark import SparkSql
from databricks.labs.ucx.source_code.linters.spark_connect import SparkConnectLinter
from databricks.labs.ucx.source_code.linters.table_creation import DBRv8d0Linter
from databricks.labs.ucx.source_code.queries import FromTable


class LinterContext:
    def __init__(self, index: MigrationIndex, session_state: CurrentSessionState | None = None):
        self._index = index
        session_state = CurrentSessionState() if not session_state else session_state
        from_table = FromTable(index, session_state=session_state)
        dbfs_from_folder = FromDbfsFolder()
        self._linters = {
            Language.PYTHON: SequentialLinter(
                [
                    SparkSql(from_table, index, session_state),
                    DBFSUsageLinter(session_state),
                    DBRv8d0Linter(dbr_version=None),
                    SparkConnectLinter(is_serverless=False),
                    DbutilsLinter(session_state),
                ]
            ),
            Language.SQL: SequentialLinter([from_table, dbfs_from_folder]),
        }
        self._fixers: dict[Language, list[Fixer]] = {
            Language.PYTHON: [SparkSql(from_table, index, session_state)],
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

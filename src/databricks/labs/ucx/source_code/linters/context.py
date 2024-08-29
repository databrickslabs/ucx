from typing import cast

from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.hive_metastore.migration_status import MigrationIndex
from databricks.labs.ucx.source_code.base import (
    Fixer,
    Linter,
    SQLSequentialLinter,
    CurrentSessionState,
    PythonSequentialLinter,
    PythonLinter,
    SQLLinter,
)
from databricks.labs.ucx.source_code.linters.dfsa import DFSAPyLinter, DFSASQLLinter
from databricks.labs.ucx.source_code.linters.imports import DbutilsPyLinter

from databricks.labs.ucx.source_code.linters.pyspark import SparkSqlPyLinter
from databricks.labs.ucx.source_code.linters.spark_connect import SparkConnectPyLinter
from databricks.labs.ucx.source_code.linters.table_creation import DBRv8d0PyLinter
from databricks.labs.ucx.source_code.queries import FromTableSQLLinter


class LinterContext:
    def __init__(self, index: MigrationIndex | None = None, session_state: CurrentSessionState | None = None):
        self._index = index
        session_state = CurrentSessionState() if not session_state else session_state

        python_linters: list[PythonLinter] = []
        python_fixers: list[Fixer] = []

        sql_linters: list[SQLLinter] = []
        sql_fixers: list[Fixer] = []

        if index is not None:
            from_table = FromTableSQLLinter(index, session_state=session_state)
            sql_linters.append(from_table)
            sql_fixers.append(from_table)
            python_linters.append(SparkSqlPyLinter(from_table, index, session_state))
            python_fixers.append(SparkSqlPyLinter(from_table, index, session_state))

        python_linters += [
            DFSAPyLinter(session_state),
            DBRv8d0PyLinter(dbr_version=session_state.dbr_version),
            SparkConnectPyLinter(session_state),
            DbutilsPyLinter(session_state),
        ]
        sql_linters.append(DFSASQLLinter())

        self._linters: dict[Language, list[SQLLinter] | list[PythonLinter]] = {
            Language.PYTHON: python_linters,
            Language.SQL: sql_linters,
        }
        self._fixers: dict[Language, list[Fixer]] = {
            Language.PYTHON: python_fixers,
            Language.SQL: sql_fixers,
        }

    def is_supported(self, language: Language) -> bool:
        return language in self._linters and language in self._fixers

    def linter(self, language: Language) -> Linter:
        if language not in self._linters:
            raise ValueError(f"Unsupported language: {language}")
        if language is Language.PYTHON:
            return PythonSequentialLinter(cast(list[PythonLinter], self._linters[language]))
        if language is Language.SQL:
            return SQLSequentialLinter(cast(list[SQLLinter], self._linters[language]))
        raise ValueError(f"Unsupported language: {language}")

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

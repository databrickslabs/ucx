from __future__ import annotations

from abc import abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass

# Code mapping between LSP, PyLint, and our own diagnostics:
# | LSP                       | PyLint     | Our            |
# |---------------------------|------------|----------------|
# | Severity.ERROR            | Error      | Failure()      |
# | Severity.WARN             | Warning    | Advisory()     |
# | DiagnosticTag.DEPRECATED  | Warning    | Deprecation()  |
# | Severity.INFO             | Info       | Advice()       |
# | Severity.HINT             | Convention | Convention()   |
# | DiagnosticTag.UNNECESSARY | Refactor   | Convention()   |


@dataclass
class Advice:
    code: str
    message: str
    start_line: int
    start_col: int
    end_line: int
    end_col: int

    def replace(
        self,
        code: str | None = None,
        message: str | None = None,
        start_line: int | None = None,
        start_col: int | None = None,
        end_line: int | None = None,
        end_col: int | None = None,
    ) -> 'Advice':
        return self.__class__(
            code=code if code is not None else self.code,
            message=message if message is not None else self.message,
            start_line=start_line if start_line is not None else self.start_line,
            start_col=start_col if start_col is not None else self.start_col,
            end_line=end_line if end_line is not None else self.end_line,
            end_col=end_col if end_col is not None else self.end_col,
        )

    def as_advisory(self) -> 'Advisory':
        return Advisory(**self.__dict__)

    def as_failure(self) -> 'Failure':
        return Failure(**self.__dict__)

    def as_deprecation(self) -> 'Deprecation':
        return Deprecation(**self.__dict__)

    def as_convention(self) -> 'Convention':
        return Convention(**self.__dict__)


class Advisory(Advice):
    """A warning that does not prevent the code from running."""


class Failure(Advisory):
    """An error that prevents the code from running."""


class Deprecation(Advisory):
    """An advisory that suggests to replace the code with a newer version."""


class Convention(Advice):
    """A suggestion for a better way to write the code."""


class Linter:
    @abstractmethod
    def lint(self, code: str) -> Iterable[Advice]: ...


class Fixer:
    @abstractmethod
    def name(self) -> str: ...

    @abstractmethod
    def apply(self, code: str) -> str: ...


# The default schema to use when the schema is not specified in a table reference
# See: https://spark.apache.org/docs/3.0.0-preview/sql-ref-syntax-qry-select-usedb.html
DEFAULT_CATALOG = 'hive_metastore'
DEFAULT_SCHEMA = 'default'


@dataclass
class CurrentSessionState:
    """
    A data class that represents the current state of a session.

    This class can be used to track various aspects of a session, such as the current schema.

    Attributes:
        catalog (str): The current schema of the session. If not provided, it defaults to 'DEFAULT_CATALOG'.
        schema (str): The current schema of the session. If not provided, it defaults to 'DEFAULT_SCHEMA'.
    """

    schema: str = DEFAULT_SCHEMA
    catalog: str = DEFAULT_CATALOG


class SequentialLinter(Linter):
    def __init__(self, linters: list[Linter]):
        self._linters = linters

    def lint(self, code: str) -> Iterable[Advice]:
        for linter in self._linters:
            yield from linter.lint(code)

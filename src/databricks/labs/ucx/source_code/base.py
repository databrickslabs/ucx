from __future__ import annotations

import logging
from abc import abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from astroid import NodeNG  # type: ignore

from databricks.sdk.service import compute


# Code mapping between LSP, PyLint, and our own diagnostics:
# | LSP                       | PyLint     | Our            |
# |---------------------------|------------|----------------|
# | Severity.ERROR            | Error      | Failure()      |
# | Severity.WARN             | Warning    | Advisory()     |
# | DiagnosticTag.DEPRECATED  | Warning    | Deprecation()  |
# | Severity.INFO             | Info       | Advice()       |
# | Severity.HINT             | Convention | Convention()   |
# | DiagnosticTag.UNNECESSARY | Refactor   | Convention()   |

logger = logging.getLogger(__name__)


@dataclass
class Advice:
    code: str
    message: str
    # Lines and columns are both 0-based: the first line is line 0.
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
    ) -> Advice:
        return type(self)(
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

    def for_path(self, path: Path) -> LocatedAdvice:
        return LocatedAdvice(self, path)

    @classmethod
    def from_node(cls, code: str, message: str, node: NodeNG) -> Advice:
        # Astroid lines are 1-based.
        return cls(
            code=code,
            message=message,
            start_line=(node.lineno or 1) - 1,
            start_col=node.col_offset or 0,
            end_line=(node.end_lineno or 1) - 1,
            end_col=node.end_col_offset,
        )

    def replace_from_node(self, node: NodeNG) -> Advice:
        # Astroid lines are 1-based.
        return self.replace(
            start_line=(node.lineno or 1) - 1,
            start_col=node.col_offset,
            end_line=(node.end_lineno or 1) - 1,
            end_col=node.end_col_offset,
        )


@dataclass
class LocatedAdvice:
    advice: Advice
    path: Path

    @property
    def is_unknown(self):
        return self.path == Path('UNKNOWN')

    def message_relative_to(self, base: Path, *, default: Path | None = None) -> str:
        advice = self.advice
        path = self.path
        if self.is_unknown:
            logger.debug(f'THIS IS A BUG! {advice.code}:{advice.message} has unknown path')
        if default is not None:
            path = default
        path = path.relative_to(base)
        return f"{path.as_posix()}:{advice.start_line}:{advice.start_col}: [{advice.code}] {advice.message}"


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
    spark_conf: dict[str, str] | None = None
    named_parameters: dict[str, str] | None = None
    data_security_mode: compute.DataSecurityMode | None = None


class SequentialLinter(Linter):
    def __init__(self, linters: list[Linter]):
        self._linters = linters

    def lint(self, code: str) -> Iterable[Advice]:
        for linter in self._linters:
            yield from linter.lint(code)

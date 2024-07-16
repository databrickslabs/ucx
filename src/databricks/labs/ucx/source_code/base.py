from __future__ import annotations

import logging
from abc import abstractmethod
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from astroid import AstroidSyntaxError, Module, NodeNG  # type: ignore

from databricks.sdk.service import compute

from databricks.labs.ucx.source_code.linters.python_ast import Tree

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
        *,
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
    def from_node(cls, *, code: str, message: str, node: NodeNG) -> Advice:
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


class Failure(Advice):
    """An error that prevents the code from running."""


class Deprecation(Advice):
    """An advisory that suggests to replace the code with a newer version."""


class Convention(Advice):
    """A suggestion for a better way to write the code."""


class Linter:
    @abstractmethod
    def lint(self, code: str) -> Iterable[Advice]: ...


class PythonLinter(Linter):

    def lint(self, code: str) -> Iterable[Advice]:
        tree = Tree.normalize_and_parse(code)
        yield from self.lint_tree(tree)

    @abstractmethod
    def lint_tree(self, tree: Tree) -> Iterable[Advice]: ...


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
    is_serverless: bool = False
    dbr_version: tuple[int, int] | None = None

    @classmethod
    def from_json(cls, json: dict) -> CurrentSessionState:
        return cls(
            schema=json.get('schema', DEFAULT_SCHEMA),
            catalog=json.get('catalog', DEFAULT_CATALOG),
            spark_conf=json.get('spark_conf', None),
            named_parameters=json.get('named_parameters', None),
            data_security_mode=json.get('data_security_mode', None),
            is_serverless=json.get('is_serverless', False),
            dbr_version=tuple(json['dbr_version']) if 'dbr_version' in json else None,
        )


class SequentialLinter(Linter):
    def __init__(self, linters: list[Linter]):
        self._linters = linters

    def lint(self, code: str) -> Iterable[Advice]:
        for linter in self._linters:
            yield from linter.lint(code)


class PythonSequentialLinter(Linter):

    def __init__(self, linters: list[PythonLinter]):
        self._linters = linters
        self._tree: Tree | None = None

    def lint(self, code: str) -> Iterable[Advice]:
        try:
            tree = self._parse_and_append(code)
            yield from self.lint_tree(tree)
        except AstroidSyntaxError as e:
            yield Failure('syntax-error', str(e), 0, 0, 0, 0)

    def lint_tree(self, tree: Tree) -> Iterable[Advice]:
        for linter in self._linters:
            yield from linter.lint_tree(tree)

    def _parse_and_append(self, code: str) -> Tree:
        tree = Tree.normalize_and_parse(code)
        self.append_tree(tree)
        return tree

    def append_tree(self, tree: Tree):
        if self._tree is None:
            self._tree = Tree(Module("root"))
        self._tree.append_tree(tree)

    def append_nodes(self, nodes: list[NodeNG]):
        if self._tree is None:
            self._tree = Tree(Module("root"))
        self._tree.append_nodes(nodes)

    def append_globals(self, globs: dict):
        if self._tree is None:
            self._tree = Tree(Module("root"))
        self._tree.append_globals(globs)

    def process_child_cell(self, code: str):
        try:
            tree = Tree.normalize_and_parse(code)
            if self._tree is None:
                self._tree = tree
            else:
                self._tree.append_tree(tree)
        except AstroidSyntaxError as e:
            # error already reported when linting enclosing notebook
            logger.warning(f"Failed to parse Python cell: {code}", exc_info=e)

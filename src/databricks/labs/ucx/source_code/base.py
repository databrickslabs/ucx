from __future__ import annotations

import codecs
import locale
import logging
from abc import abstractmethod, ABC
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from astroid import AstroidSyntaxError, NodeNG  # type: ignore
from sqlglot import Expression, parse as parse_sql, ParseError as SqlParseError

from databricks.sdk.service import compute
from databricks.sdk.service.workspace import Language

from databricks.labs.blueprint.paths import WorkspacePath
from databricks.labs.ucx.source_code.python.python_ast import Tree

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
        return f"./{path.as_posix()}:{advice.start_line}:{advice.start_col}: [{advice.code}] {advice.message}"


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


class SqlLinter(Linter):

    def lint(self, code: str) -> Iterable[Advice]:
        try:
            expressions = parse_sql(code, read='databricks')
            for expression in expressions:
                if not expression:
                    continue
                yield from self.lint_expression(expression)
        except SqlParseError as e:
            logger.debug(f"Failed to parse SQL: {code}", exc_info=e)
            yield self.sql_parse_failure(code)

    @staticmethod
    def sql_parse_failure(code: str):
        return Failure(
            code='sql-parse-error',
            message=f"SQL expression is not supported yet: {code}",
            # SQLGlot does not propagate tokens yet. See https://github.com/tobymao/sqlglot/issues/3159
            start_line=0,
            start_col=0,
            end_line=0,
            end_col=1024,
        )

    @abstractmethod
    def lint_expression(self, expression: Expression) -> Iterable[Advice]: ...


class PythonLinter(Linter):

    def lint(self, code: str) -> Iterable[Advice]:
        tree = Tree.normalize_and_parse(code)
        yield from self.lint_tree(tree)

    @abstractmethod
    def lint_tree(self, tree: Tree) -> Iterable[Advice]: ...


class Fixer(ABC):

    @property
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
            data_security_mode=cls.parse_security_mode(json.get('data_security_mode', None)),
            is_serverless=json.get('is_serverless', False),
            dbr_version=tuple(json['dbr_version']) if 'dbr_version' in json else None,
        )

    @staticmethod
    def parse_security_mode(mode_str: str | None) -> compute.DataSecurityMode | None:
        try:
            return compute.DataSecurityMode(mode_str) if mode_str else None
        except ValueError:
            logger.warning(f'Unknown data_security_mode {mode_str}')
            return None


class SqlSequentialLinter(SqlLinter):

    def __init__(self, linters: list[SqlLinter]):
        self._linters = linters

    def lint_expression(self, expression: Expression) -> Iterable[Advice]:
        for linter in self._linters:
            yield from linter.lint_expression(expression)


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
        self._make_tree().append_tree(tree)

    def append_nodes(self, nodes: list[NodeNG]):
        self._make_tree().append_nodes(nodes)

    def append_globals(self, globs: dict):
        self._make_tree().append_globals(globs)

    def process_child_cell(self, code: str):
        try:
            this_tree = self._make_tree()
            tree = Tree.normalize_and_parse(code)
            this_tree.append_tree(tree)
        except AstroidSyntaxError as e:
            # error already reported when linting enclosing notebook
            logger.warning(f"Failed to parse Python cell: {code}", exc_info=e)

    def _make_tree(self) -> Tree:
        if self._tree is None:
            self._tree = Tree.new_module()
        return self._tree


SUPPORTED_EXTENSION_LANGUAGES = {
    '.py': Language.PYTHON,
    '.sql': Language.SQL,
}


def file_language(path: Path) -> Language | None:
    return SUPPORTED_EXTENSION_LANGUAGES.get(path.suffix.lower())


def guess_encoding(path: Path):
    # some files encode a unicode BOM (byte-order-mark), so let's use that if available
    with path.open('rb') as _file:
        raw = _file.read(4)
        if raw.startswith(codecs.BOM_UTF32_LE) or raw.startswith(codecs.BOM_UTF32_BE):
            return 'utf-32'
        if raw.startswith(codecs.BOM_UTF16_LE) or raw.startswith(codecs.BOM_UTF16_BE):
            return 'utf-16'
        if raw.startswith(codecs.BOM_UTF8):
            return 'utf-8-sig'
        # no BOM, let's use default encoding
        return locale.getpreferredencoding(False)


# duplicated from CellLanguage to prevent cyclic import
LANGUAGE_COMMENT_PREFIXES = {Language.PYTHON: '#', Language.SCALA: '//', Language.SQL: '--'}
NOTEBOOK_HEADER = "Databricks notebook source"


def is_a_notebook(path: Path, content: str | None = None) -> bool:
    if isinstance(path, WorkspacePath):
        return path.is_notebook()
    if not path.is_file():
        return False
    language = file_language(path)
    if not language:
        return False
    magic_header = f"{LANGUAGE_COMMENT_PREFIXES.get(language)} {NOTEBOOK_HEADER}"
    if content is not None:
        return content.startswith(magic_header)
    try:
        with path.open('rt', encoding=guess_encoding(path)) as f:
            file_header = f.read(len(magic_header))
    except (FileNotFoundError, UnicodeDecodeError, PermissionError):
        logger.warning(f"Could not read file {path}")
        return False
    return file_header == magic_header

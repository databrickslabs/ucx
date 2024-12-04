from __future__ import annotations

import codecs
import dataclasses
import io
import logging
import sys
from abc import abstractmethod, ABC
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, BinaryIO, TextIO

from astroid import NodeNG  # type: ignore
from sqlglot import Expression, parse as parse_sql
from sqlglot.errors import SqlglotError

from databricks.sdk.service import compute
from databricks.sdk.service.workspace import Language

from databricks.labs.blueprint.paths import WorkspacePath


if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

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
        return dataclasses.replace(
            self,
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
    def is_unknown(self) -> bool:
        return self.path == Path('UNKNOWN')

    def message_relative_to(self, base: Path, *, default: Path | None = None) -> str:
        advice = self.advice
        path = self.path
        if self.is_unknown:
            logger.debug(f'THIS IS A BUG! {advice.code}:{advice.message} has unknown path')
        if default is not None:
            path = default
        try:
            path = path.relative_to(base)
        except ValueError:
            logger.debug(f'Not a relative path: {path} to base: {base}')
        # increment start_line because it is 0-based whereas IDEs are usually 1-based
        return f"./{path.as_posix()}:{advice.start_line+1}:{advice.start_col}: [{advice.code}] {advice.message}"


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
            # TODO: unify with SqlParser.walk_expressions(...)
            expressions = parse_sql(code, read='databricks')
            for expression in expressions:
                if not expression:
                    continue
                yield from self.lint_expression(expression)
        except SqlglotError as e:
            logger.debug(f"Failed to parse SQL: {code}", exc_info=e)
            yield self.sql_parse_failure(code)

    @staticmethod
    def sql_parse_failure(code: str) -> Failure:
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


class Fixer(ABC):

    @property
    @abstractmethod
    def name(self) -> str: ...

    @abstractmethod
    def apply(self, code: str) -> str: ...


@dataclass
class LineageAtom:

    object_type: str
    object_id: str
    other: dict[str, str] | None = None


@dataclass
class SourceInfo:

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Self:
        source_lineage = data.get("source_lineage", None)
        if isinstance(source_lineage, list) and len(source_lineage) > 0 and isinstance(source_lineage[0], dict):
            lineage_atoms = [LineageAtom(**lineage) for lineage in source_lineage]
            data["source_lineage"] = lineage_atoms
        return cls(**data)

    UNKNOWN = "unknown"

    source_id: str = UNKNOWN
    source_timestamp: datetime = datetime.fromtimestamp(0)
    source_lineage: list[LineageAtom] = field(default_factory=list)
    assessment_start_timestamp: datetime = datetime.fromtimestamp(0)
    assessment_end_timestamp: datetime = datetime.fromtimestamp(0)

    def replace_source(
        self,
        source_id: str | None = None,
        source_lineage: list[LineageAtom] | None = None,
        source_timestamp: datetime | None = None,
    ) -> Self:
        return dataclasses.replace(
            self,
            source_id=source_id or self.source_id,
            source_timestamp=source_timestamp or self.source_timestamp,
            source_lineage=source_lineage or self.source_lineage,
        )

    def replace_assessment_infos(
        self,
        assessment_start: datetime | None = None,
        assessment_end: datetime | None = None,
    ) -> Self:
        return dataclasses.replace(
            self,
            assessment_start_timestamp=assessment_start or self.assessment_start_timestamp,
            assessment_end_timestamp=assessment_end or self.assessment_end_timestamp,
        )

    @property
    def source_type(self) -> str | None:
        if not self.source_lineage:
            return None
        last = self.source_lineage[-1]
        return last.object_type

    @property
    def query_id(self) -> str | None:
        if self.source_type != 'QUERY':
            return None
        last = self.source_lineage[-1]
        parts = last.object_id.split('/')
        if len(parts) < 2:
            return None
        return parts[1]


@dataclass
class UsedTable(SourceInfo):

    @classmethod
    def parse(cls, value: str, default_schema: str, is_read=True, is_write=False) -> UsedTable:
        parts = value.split(".")
        if len(parts) >= 3:
            catalog_name = parts.pop(0)
        else:
            catalog_name = "hive_metastore"
        if len(parts) >= 2:
            schema_name = parts.pop(0)
        else:
            schema_name = default_schema
        return UsedTable(
            catalog_name=catalog_name, schema_name=schema_name, table_name=parts[0], is_read=is_read, is_write=is_write
        )

    @property
    def full_name(self) -> str:
        return ".".join([self.catalog_name, self.schema_name, self.table_name])

    catalog_name: str = SourceInfo.UNKNOWN
    schema_name: str = SourceInfo.UNKNOWN
    table_name: str = SourceInfo.UNKNOWN
    is_read: bool = True
    is_write: bool = False


class TableCollector(ABC):

    @abstractmethod
    def collect_tables(self, source_code: str) -> Iterable[UsedTable]: ...


@dataclass
class UsedTableNode:
    table: UsedTable
    node: NodeNG


class TableSqlCollector(TableCollector, ABC): ...


@dataclass
class DirectFsAccess(SourceInfo):
    """A record describing a Direct File System Access"""

    path: str = SourceInfo.UNKNOWN
    is_read: bool = False
    is_write: bool = False


@dataclass
class DirectFsAccessNode:
    dfsa: DirectFsAccess
    node: NodeNG


class DfsaCollector(ABC):

    @abstractmethod
    def collect_dfsas(self, source_code: str) -> Iterable[DirectFsAccess]: ...


class DfsaSqlCollector(DfsaCollector, ABC): ...


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


class SqlSequentialLinter(SqlLinter, DfsaCollector, TableCollector):

    def __init__(
        self,
        linters: list[SqlLinter],
        dfsa_collectors: list[DfsaSqlCollector],
        table_collectors: list[TableSqlCollector],
    ):
        self._linters = linters
        self._dfsa_collectors = dfsa_collectors
        self._table_collectors = table_collectors

    def lint_expression(self, expression: Expression) -> Iterable[Advice]:
        for linter in self._linters:
            yield from linter.lint_expression(expression)

    def collect_dfsas(self, source_code: str) -> Iterable[DirectFsAccess]:
        for collector in self._dfsa_collectors:
            yield from collector.collect_dfsas(source_code)

    def collect_tables(self, source_code: str) -> Iterable[UsedTable]:
        for collector in self._table_collectors:
            yield from collector.collect_tables(source_code)


SUPPORTED_EXTENSION_LANGUAGES = {
    '.py': Language.PYTHON,
    '.sql': Language.SQL,
}


def file_language(path: Path) -> Language | None:
    return SUPPORTED_EXTENSION_LANGUAGES.get(path.suffix.lower())


def _detect_encoding_bom(binary_io: BinaryIO, *, preserve_position: bool) -> str | None:
    # Peek at the first (up to) 4 bytes, preserving the file position if requested.
    position = binary_io.tell() if preserve_position else None
    try:
        maybe_bom = binary_io.read(4)
    finally:
        if position is not None:
            binary_io.seek(position)
    # For these encodings, TextIOWrapper will skip over the BOM during decoding.
    if maybe_bom.startswith(codecs.BOM_UTF32_LE) or maybe_bom.startswith(codecs.BOM_UTF32_BE):
        return "utf-32"
    if maybe_bom.startswith(codecs.BOM_UTF16_LE) or maybe_bom.startswith(codecs.BOM_UTF16_BE):
        return "utf-16"
    if maybe_bom.startswith(codecs.BOM_UTF8):
        return "utf-8-sig"
    return None


def decode_with_bom(
    file: BinaryIO,
    encoding: str | None = None,
    errors: str | None = None,
    newline: str | None = None,
) -> TextIO:
    """Wrap an open binary file with a text decoder.

    This has the same semantics as the built-in `open()` call, except that if the encoding is not specified and the
    file is seekable then it will be checked for a BOM. If a BOM marker is found, that encoding is used. When neither
    an encoding nor a BOM are present the encoding of the system locale is used.

    Args:
          file: the open (binary) file to wrap in text mode.
          encoding: force decoding with a specific locale. If not present the file BOM and system locale are used.
          errors: how decoding errors should be handled, as per open().
          newline: how newlines should be handled, as per open().
    Raises:
          ValueError: if the encoding should be detected via potential BOM marker but the file is not seekable.
    Returns:
          a text-based IO wrapper that will decode the underlying binary-mode file as text.
    """
    use_encoding = _detect_encoding_bom(file, preserve_position=True) if encoding is None else encoding
    return io.TextIOWrapper(file, encoding=use_encoding, errors=errors, newline=newline)


def read_text(path: Path, size: int = -1) -> str:
    """Read a file as text, decoding according to the BOM marker if that is present.

    This differs to the normal `.read_text()` method on path which does not support BOM markers.

    Arguments:
        path: the path to read text from.
        size: how much text (measured in characters) to read. If negative, all text is read. Less may be read if the
            file is smaller than the specified size.
    Returns:
        The string content of the file, up to the specified size.
    """
    with path.open("rb") as binary_io:
        # If the open file is seekable, we can detect the BOM and decode without re-opening.
        if binary_io.seekable():
            with decode_with_bom(binary_io) as f:
                return f.read(size)
        encoding = _detect_encoding_bom(binary_io, preserve_position=False)
    # Otherwise having read the BOM there's no way to rewind so we need to re-open and read from that.
    with path.open("rt", encoding=encoding) as f:
        return f.read(size)


def safe_read_text(path: Path, size: int = -1) -> str | None:
    """Safe read a text file by handling reading exceptions, see :func:_read_text.

    Returns
        str : Content of file
        None : If error occurred during reading.
    """
    try:
        return read_text(path, size=size)
    except (FileNotFoundError, UnicodeDecodeError, PermissionError) as e:
        logger.warning(f"Could not read file: {path}", exc_info=e)
        return None


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
    file_header = safe_read_text(path, size=len(magic_header))
    return file_header == magic_header

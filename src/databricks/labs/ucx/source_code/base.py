from __future__ import annotations

import codecs
import dataclasses
import io
import logging
import os
import shutil
import sys
from abc import abstractmethod, ABC
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any, BinaryIO, TextIO, TypeVar

from astroid import NodeNG  # type: ignore

from databricks.labs.blueprint.paths import WorkspacePath
from databricks.sdk.service import compute
from databricks.sdk.service.workspace import Language


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


T = TypeVar("T", bound="Advice")


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
    def from_node(cls: type[T], *, code: str, message: str, node: NodeNG) -> T:
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
    """The advice with a path location."""

    advice: Advice
    """The advice"""

    path: Path
    """The path location"""

    def __str__(self) -> str:
        return f"{self.path.as_posix()}:{self.advice.start_line + 1}:{self.advice.start_col}: [{self.advice.code}] {self.advice.message}"

    def has_missing_path(self) -> bool:
        """Flag if the path is missing, or not."""
        return self.path == Path("<MISSING_SOURCE_PATH>")  # Reusing flag from DependencyProblem


class Advisory(Advice):
    """A warning that does not prevent the code from running."""


class Failure(Advice):
    """An error that prevents the code from running."""


class Deprecation(Advice):
    """An advisory that suggests to replace the code with a newer version."""


class Convention(Advice):
    """A suggestion for a better way to write the code."""


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

    source_timestamp: datetime = field(default_factory=lambda: datetime.fromtimestamp(0), compare=False)
    """Unused attribute, kept for legacy reasons"""

    source_lineage: list[LineageAtom] = field(default_factory=list)

    assessment_start_timestamp: datetime = field(default_factory=lambda: datetime.fromtimestamp(0), compare=False)
    """Unused attribute, kept for legacy reasons"""

    assessment_end_timestamp: datetime = field(default_factory=lambda: datetime.fromtimestamp(0), compare=False)
    """Unused attribute, kept for legacy reasons"""

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


SUPPORTED_EXTENSION_LANGUAGES = {
    '.py': Language.PYTHON,
    '.sql': Language.SQL,
}


def infer_file_language_if_supported(path: Path) -> Language | None:
    """Infer the file language if it is supported by UCX's linter module.

    This function returns the language of the file based on the file
    extension. If the file extension is not supported, it returns None.

    Use this function to filter paths before passing it to the linters.
    """
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
    except (OSError, UnicodeError) as e:
        logger.warning(f"Could not read file: {path}", exc_info=e)
        return None


def write_text(path: Path, contents: str, *, encoding: str | None = None) -> int:
    """Write content to a file as text, encode according to the BOM marker if that is present.

    This differs to the normal `.read_text()` method on path which does not support BOM markers.

    Arguments:
        path (Path): The file path to write text to.
        contents (str) : The content to write to the file.
        encoding (str) : Force encoding with a specific locale. If not present the file BOM and
            system locale are used.

    Returns:
        int : The number of characters written to the file.
    """
    if not encoding and path.exists():
        with path.open("rb") as binary_io:
            encoding = _detect_encoding_bom(binary_io, preserve_position=False)
    # If encoding=None, the system locale is used for encoding (as per open()).
    return path.write_text(contents, encoding=encoding)


def safe_write_text(path: Path, contents: str, *, encoding: str | None = None) -> int | None:
    """Safe write content to a file by handling writing exceptions, see :func:write_text.

    Returns:
        int | None : The number of characters written to the file. If None, no content was written.
    """
    try:
        return write_text(path, contents, encoding=encoding)
    except OSError as e:
        logger.warning(f"Cannot write to file: {path}", exc_info=e)
        return None


# duplicated from CellLanguage to prevent cyclic import
LANGUAGE_COMMENT_PREFIXES = {Language.PYTHON: '#', Language.SCALA: '//', Language.SQL: '--'}
NOTEBOOK_HEADER = "Databricks notebook source"


def is_a_notebook(path: Path, content: str | None = None) -> bool:
    if isinstance(path, WorkspacePath):
        return path.is_notebook()
    if not path.is_file():
        return False
    language = infer_file_language_if_supported(path)
    if not language:
        return False
    magic_header = f"{LANGUAGE_COMMENT_PREFIXES.get(language)} {NOTEBOOK_HEADER}"
    if content is not None:
        return content.startswith(magic_header)
    file_header = safe_read_text(path, size=len(magic_header))
    return file_header == magic_header


def _add_backup_suffix(path: Path) -> Path:
    """Add a backup suffix to a path.

    The backed up path is the same as the original path with an additional
    `.bak` appended to the suffix.

    Reuse this method so that the backup path is consistent in this module.
    """
    # Not checking for the backup suffix to allow making backups of backups.
    return path.with_suffix(path.suffix + ".bak")


def back_up_path(path: Path) -> Path | None:
    """Back up a path.

    The backed up path is the same as the original path with an additional
    `.bak` appended to the suffix.

    Returns :
        path | None : The backed up path. If None, the backup failed.
    """
    path_backed_up = _add_backup_suffix(path)
    try:
        shutil.copyfile(path, path_backed_up)
    except OSError as e:
        logger.warning(f"Cannot back up file: {path}", exc_info=e)
        return None
    return path_backed_up


def revert_back_up_path(path: Path) -> bool | None:
    """Revert a backed up path, see :func:back_up_path.

    The backed up path is the same as the original path with an additional
    `.bak` appended to the suffix.

    Args :
        path : The original path, NOT the backed up path.

    Returns :
        bool : Flag if the revert was successful. If None, the backed up path
        does not exist, thus it cannot be reverted and the operation is not
        successful nor failed.
    """
    path_backed_up = _add_backup_suffix(path)
    if not path_backed_up.exists():
        logger.warning(f"Backup is missing: {path_backed_up}")
        return None
    try:
        shutil.copyfile(path_backed_up, path)
    except OSError as e:
        logger.warning(f"Cannot revert backup: {path}", exc_info=e)
        return False
    try:
        os.unlink(path_backed_up)
    except OSError as e:
        # The backup revert is successful, but the backup file cannot be removed
        logger.warning(f"Cannot remove backup file: {path_backed_up}", exc_info=e)
    return True

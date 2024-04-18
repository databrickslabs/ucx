from __future__ import annotations  # for type hints

import abc
import ast
import logging
from abc import ABC, abstractmethod
from ast import parse as parse_python
from enum import Enum

from sqlglot import ParseError as SQLParseError
from sqlglot import parse as parse_sql
from databricks.sdk.service.workspace import Language, ObjectInfo, ObjectType

from databricks.labs.ucx.source_code.dependencies import (
    DependencyGraph,
    SourceContainer,
    DependencyType,
    UnresolvedDependency,
)
from databricks.labs.ucx.source_code.python_linter import ASTLinter, PythonLinter


logger = logging.getLogger(__name__)
# use a specific logger for sqlglot warnings so we can disable them selectively
sqlglot_logger = logging.getLogger(f"{__name__}.sqlglot")

NOTEBOOK_HEADER = "Databricks notebook source"
CELL_SEPARATOR = "COMMAND ----------"
MAGIC_PREFIX = 'MAGIC'
LANGUAGE_PREFIX = '%'
LANGUAGE_PI = 'LANGUAGE'
COMMENT_PI = 'COMMENT'


class Cell(ABC):

    def __init__(self, source: str):
        self._original_code = source
        self._migrated_code = source

    @property
    def original_code(self):
        return self._original_code

    @property
    def migrated_code(self):
        return self._migrated_code

    @migrated_code.setter
    def migrated_code(self, value: str):
        self._migrated_code = value

    @property
    @abstractmethod
    def language(self) -> CellLanguage:
        raise NotImplementedError()

    @abstractmethod
    def is_runnable(self) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def build_dependency_graph(self, parent: DependencyGraph):
        raise NotImplementedError()


class PythonCell(Cell):

    @property
    def language(self):
        return CellLanguage.PYTHON

    def is_runnable(self) -> bool:
        try:
            tree = parse_python(self._original_code)
            return tree is not None
        except SyntaxError:
            return True

    def build_dependency_graph(self, parent: DependencyGraph):
        linter = ASTLinter.parse(self._original_code)
        calls = linter.locate(ast.Call, [("run", ast.Attribute), ("notebook", ast.Attribute), ("dbutils", ast.Name)])
        for call in calls:
            assert isinstance(call, ast.Call)
            path = PythonLinter.get_dbutils_notebook_run_path_arg(call)
            if isinstance(path, ast.Constant):
                path = path.value.strip().strip("'").strip('"')
                object_info = ObjectInfo(object_type=ObjectType.NOTEBOOK, path=path)
                dependency = parent.resolver.resolve_object_info(object_info)
                if dependency is not None:
                    parent.register_dependency(dependency)
                else:
                    # TODO raise Advice, see https://github.com/databrickslabs/ucx/issues/1439
                    raise ValueError(f"Invalid notebook path in dbutils.notebook.run command: {path}")
        names = PythonLinter.list_import_sources(linter)
        for name in names:
            unresolved = UnresolvedDependency(name)
            parent.register_dependency(unresolved)


class RCell(Cell):

    @property
    def language(self):
        return CellLanguage.R

    def is_runnable(self) -> bool:
        return True  # TODO

    def build_dependency_graph(self, parent: DependencyGraph):
        pass  # not in scope


class ScalaCell(Cell):

    @property
    def language(self):
        return CellLanguage.SCALA

    def is_runnable(self) -> bool:
        return True  # TODO

    def build_dependency_graph(self, parent: DependencyGraph):
        pass  # TODO


class SQLCell(Cell):

    @property
    def language(self):
        return CellLanguage.SQL

    def is_runnable(self) -> bool:
        try:
            statements = parse_sql(self._original_code)
            return len(statements) > 0
        except SQLParseError as e:
            sqlglot_logger.warning(f"Failed to parse SQL using 'sqlglot': {self._original_code}", exc_info=e)
            return True

    def build_dependency_graph(self, parent: DependencyGraph):
        pass  # not in scope


class MarkdownCell(Cell):

    @property
    def language(self):
        return CellLanguage.MARKDOWN

    def is_runnable(self) -> bool:
        return True  # TODO

    def build_dependency_graph(self, parent: DependencyGraph):
        pass  # not in scope


class RunCell(Cell):

    @property
    def language(self):
        return CellLanguage.RUN

    def is_runnable(self) -> bool:
        return True  # TODO

    def build_dependency_graph(self, parent: DependencyGraph):
        command = f'{LANGUAGE_PREFIX}{self.language.magic_name}'
        lines = self._original_code.split('\n')
        for line in lines:
            start = line.index(command)
            if start >= 0:
                path = line[start + len(command) :]
                path = path.strip().strip("'").strip('"')
                object_info = ObjectInfo(object_type=ObjectType.NOTEBOOK, path=path)
                dependency = parent.resolver.resolve_object_info(object_info)
                if dependency is not None:
                    parent.register_dependency(dependency)
                else:
                    # TODO raise Advice, see https://github.com/databrickslabs/ucx/issues/1439
                    raise ValueError(f"Invalid notebook path in %run command: {path}")
                return
        raise ValueError("Missing notebook path in %run command")

    def migrate_notebook_path(self):
        pass


class ShellCell(Cell):

    @property
    def language(self):
        return CellLanguage.SHELL

    def is_runnable(self) -> bool:
        return True  # TODO

    def build_dependency_graph(self, parent: DependencyGraph):
        pass  # nothing to do

    def migrate_notebook_path(self):
        pass  # nothing to do


class PipCell(Cell):

    @property
    def language(self):
        return CellLanguage.PIP

    def is_runnable(self) -> bool:
        return True  # TODO

    def build_dependency_graph(self, parent: DependencyGraph):
        pass  # nothing to do

    def migrate_notebook_path(self):
        pass


class CellLanguage(Enum):
    # long magic_names must come first to avoid shorter ones being matched
    PYTHON = Language.PYTHON, 'python', '#', True, PythonCell
    SCALA = Language.SCALA, 'scala', '//', True, ScalaCell
    SQL = Language.SQL, 'sql', '--', True, SQLCell
    RUN = None, 'run', '', False, RunCell
    PIP = None, 'pip', '', False, PipCell
    SHELL = None, 'sh', '', False, ShellCell
    # see https://spec.commonmark.org/0.31.2/#html-comment
    MARKDOWN = None, 'md', "<!--->", False, MarkdownCell
    R = Language.R, 'r', '#', True, RCell

    def __init__(self, *args):
        super().__init__()
        self._language = args[0]
        self._magic_name = args[1]
        self._comment_prefix = args[2]
        # PI stands for Processing Instruction
        self._requires_isolated_pi = args[3]
        self._new_cell = args[4]

    @property
    def language(self) -> Language:
        return self._language

    @property
    def magic_name(self) -> str:
        return self._magic_name

    @property
    def comment_prefix(self) -> str:
        return self._comment_prefix

    @property
    def requires_isolated_pi(self) -> str:
        return self._requires_isolated_pi

    @classmethod
    def of_language(cls, language: Language) -> CellLanguage:
        return next((cl for cl in CellLanguage if cl.language == language))

    @classmethod
    def of_magic_name(cls, magic_name: str) -> CellLanguage | None:
        return next((cl for cl in CellLanguage if magic_name.startswith(cl.magic_name)), None)

    def read_cell_language(self, lines: list[str]) -> CellLanguage | None:
        magic_prefix = f'{self.comment_prefix} {MAGIC_PREFIX} '
        magic_language_prefix = f'{magic_prefix}{LANGUAGE_PREFIX}'
        for line in lines:
            # if we find a non-comment then we're done
            if not line.startswith(self.comment_prefix):
                return None
            # if it's not a magic comment, skip it
            if not line.startswith(magic_prefix):
                continue
            if line.startswith(magic_language_prefix):
                line = line[len(magic_language_prefix) :]
                return CellLanguage.of_magic_name(line.strip())
            return None
        return None

    def new_cell(self, source: str) -> Cell:
        return self._new_cell(source)

    def extract_cells(self, source: str) -> list[Cell] | None:
        lines = source.split('\n')
        header = f"{self.comment_prefix} {NOTEBOOK_HEADER}"
        if not lines[0].startswith(header):
            raise ValueError("Not a Databricks notebook source!")

        def make_cell(cell_lines: list[str]):
            # trim leading blank lines
            while len(cell_lines) > 0 and len(cell_lines[0]) == 0:
                cell_lines.pop(0)
            # trim trailing blank lines
            while len(cell_lines) > 0 and len(cell_lines[-1]) == 0:
                cell_lines.pop(-1)
            cell_language = self.read_cell_language(cell_lines)
            if cell_language is None:
                cell_language = self
            else:
                self._remove_magic_wrapper(cell_lines, cell_language)
            cell_source = '\n'.join(cell_lines)
            return cell_language.new_cell(cell_source)

        cells = []
        cell_lines: list[str] = []
        separator = f"{self.comment_prefix} {CELL_SEPARATOR}"
        for i in range(1, len(lines)):
            line = lines[i].strip()
            if line.startswith(separator):
                cell = make_cell(cell_lines)
                cells.append(cell)
                cell_lines = []
            else:
                cell_lines.append(lines[i])
        if len(cell_lines) > 0:
            cell = make_cell(cell_lines)
            cells.append(cell)

        return cells

    def _remove_magic_wrapper(self, lines: list[str], cell_language: CellLanguage):
        prefix = f"{self.comment_prefix} {MAGIC_PREFIX} "
        prefix_len = len(prefix)
        for i, line in enumerate(lines):
            if line.startswith(prefix):
                line = line[prefix_len:]
                if cell_language.requires_isolated_pi and line.startswith(LANGUAGE_PREFIX):
                    line = f"{cell_language.comment_prefix} {LANGUAGE_PI}"
                lines[i] = line
                continue
            if line.startswith(self.comment_prefix):
                line = f"{cell_language.comment_prefix} {COMMENT_PI}{line}"
                lines[i] = line

    def wrap_with_magic(self, code: str, cell_language: CellLanguage) -> str:
        language_pi_prefix = f"{cell_language.comment_prefix} {LANGUAGE_PI}"
        comment_pi_prefix = f"{cell_language.comment_prefix} {COMMENT_PI}"
        comment_pi_prefix_len = len(comment_pi_prefix)
        lines = code.split('\n')
        for i, line in enumerate(lines):
            if line.startswith(language_pi_prefix):
                line = f"{self.comment_prefix} {MAGIC_PREFIX} {LANGUAGE_PREFIX}{cell_language.magic_name}"
                lines[i] = line
                continue
            if line.startswith(comment_pi_prefix):
                lines[i] = line[comment_pi_prefix_len:]
                continue
            line = f"{self.comment_prefix} {MAGIC_PREFIX} {line}"
            lines[i] = line
        if code.endswith('./'):
            lines.append('\n')
        return "\n".join(lines)


class Notebook(SourceContainer, abc.ABC):

    @staticmethod
    def _parse(path: str, source: str, default_language: Language, ctor: type):
        default_cell_language = CellLanguage.of_language(default_language)
        cells = default_cell_language.extract_cells(source)
        if cells is None:
            raise ValueError(f"Could not parse Notebook: {path}")
        return ctor(path, source, default_language, cells, source.endswith('\n'))

    def __init__(self, path: str, source: str, language: Language, cells: list[Cell], ends_with_lf):
        self._path = path
        self._source = source
        self._language = language
        self._cells = cells
        self._ends_with_lf = ends_with_lf

    @property
    @abstractmethod
    def dependency_type(self) -> DependencyType:
        raise NotImplementedError()

    @property
    def path(self) -> str:
        return self._path

    @property
    def cells(self) -> list[Cell]:
        return self._cells

    @property
    def original_code(self) -> str:
        return self._source

    def to_migrated_code(self):
        default_language = CellLanguage.of_language(self._language)
        header = f"{default_language.comment_prefix} {NOTEBOOK_HEADER}"
        sources = [header]
        for i, cell in enumerate(self._cells):
            migrated_code = cell.migrated_code
            if cell.language is not default_language:
                migrated_code = default_language.wrap_with_magic(migrated_code, cell.language)
            sources.append(migrated_code)
            if i < len(self._cells) - 1:
                sources.append('')
                sources.append(f'{default_language.comment_prefix} {CELL_SEPARATOR}')
                sources.append('')
        if self._ends_with_lf:
            sources.append('')  # following join will append lf
        return '\n'.join(sources)

    def build_dependency_graph(self, parent: DependencyGraph) -> None:
        for cell in self._cells:
            cell.build_dependency_graph(parent)


class WorkspaceNotebook(Notebook):

    @classmethod
    def parse(cls, path: str, source: str, default_language: Language) -> WorkspaceNotebook:
        return cls._parse(path, source, default_language, cls)

    @property
    def dependency_type(self) -> DependencyType:
        return DependencyType.WORKSPACE_NOTEBOOK


class LocalNotebook(Notebook):

    @classmethod
    def parse(cls, path: str, source: str, default_language: Language) -> WorkspaceNotebook:
        return cls._parse(path, source, default_language, cls)

    @property
    def dependency_type(self) -> DependencyType:
        return DependencyType.LOCAL_NOTEBOOK

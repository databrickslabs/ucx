from __future__ import annotations  # for type hints

import logging
from abc import ABC, abstractmethod
from ast import parse as parse_python
from collections.abc import Callable
from enum import Enum
from pathlib import Path

from sqlglot import ParseError as SQLParseError
from sqlglot import parse as parse_sql
from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.source_code.dependencies import (
    DependencyGraph,
    DependencyProblem,
)
from databricks.labs.ucx.source_code.dependency_loaders import SourceContainer


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

    def __init__(self, source: str, original_offset: int = 0):
        self._original_offset = original_offset
        self._original_code = source
        self._migrated_code = source

    @property
    def original_offset(self) -> int:
        return self._original_offset

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
    def build_dependency_graph(self, parent: DependencyGraph, problem_collector: Callable[[DependencyProblem], None]):
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

    def build_dependency_graph(self, parent: DependencyGraph, problem_collector: Callable[[DependencyProblem], None]):
        parent.build_graph_from_python_source(self._original_code, problem_collector)


class RCell(Cell):

    @property
    def language(self):
        return CellLanguage.R

    def is_runnable(self) -> bool:
        return True  # TODO

    def build_dependency_graph(self, parent: DependencyGraph, problem_collector: Callable[[DependencyProblem], None]):
        pass  # not in scope


class ScalaCell(Cell):

    @property
    def language(self):
        return CellLanguage.SCALA

    def is_runnable(self) -> bool:
        return True  # TODO

    def build_dependency_graph(self, parent: DependencyGraph, problem_collector: Callable[[DependencyProblem], None]):
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

    def build_dependency_graph(self, parent: DependencyGraph, problem_collector: Callable[[DependencyProblem], None]):
        pass  # not in scope


class MarkdownCell(Cell):

    @property
    def language(self):
        return CellLanguage.MARKDOWN

    def is_runnable(self) -> bool:
        return True  # TODO

    def build_dependency_graph(self, parent: DependencyGraph, problem_collector: Callable[[DependencyProblem], None]):
        pass  # not in scope


class RunCell(Cell):

    @property
    def language(self):
        return CellLanguage.RUN

    def is_runnable(self) -> bool:
        return True  # TODO

    def build_dependency_graph(self, parent: DependencyGraph, problem_collector: Callable[[DependencyProblem], None]):
        command = f'{LANGUAGE_PREFIX}{self.language.magic_name}'
        lines = self._original_code.split('\n')
        for idx, line in enumerate(lines):
            start = line.index(command)
            if start >= 0:
                path = line[start + len(command) :]
                path = path.strip().strip("'").strip('"')
                if len(path) == 0:
                    continue
                problems: list[DependencyProblem] = []
                parent.register_notebook(Path(path), problems.append)
                start_line = self._original_offset + idx + 1
                for problem in problems:
                    problem = problem.replace(
                        start_line=start_line, start_col=0, end_line=start_line, end_col=len(line)
                    )
                    problem_collector(problem)
                return
        start_line = self._original_offset + 1
        problem = DependencyProblem(
            'dependency-check',
            "Missing notebook path in %run command",
            start_line=start_line,
            start_col=0,
            end_line=start_line,
            end_col=len(self._original_code),
        )
        problem_collector(problem)

    def migrate_notebook_path(self):
        pass


class ShellCell(Cell):

    @property
    def language(self):
        return CellLanguage.SHELL

    def is_runnable(self) -> bool:
        return True  # TODO

    def build_dependency_graph(self, parent: DependencyGraph, problem_collector: Callable[[DependencyProblem], None]):
        pass  # nothing to do

    def migrate_notebook_path(self):
        pass  # nothing to do


class PipCell(Cell):

    @property
    def language(self):
        return CellLanguage.PIP

    def is_runnable(self) -> bool:
        return True  # TODO

    def build_dependency_graph(self, parent: DependencyGraph, problem_collector: Callable[[DependencyProblem], None]):
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
        # TODO: Should this not raise a ValueError if the language is not found?
        #  It also  causes a GeneratorExit exception to be raised. Maybe an explicit loop is better.
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

    def new_cell(self, source: str, original_offset: int) -> Cell:
        return self._new_cell(source, original_offset)

    def extract_cells(self, source: str) -> list[Cell] | None:
        lines = source.split('\n')
        header = f"{self.comment_prefix} {NOTEBOOK_HEADER}"
        if not lines[0].startswith(header):
            raise ValueError("Not a Databricks notebook source!")

        def make_cell(cell_lines: list[str], start: int):
            # trim leading blank lines
            while len(cell_lines) > 0 and len(cell_lines[0]) == 0:
                cell_lines.pop(0)
                start += 1

            # trim trailing blank lines
            while len(cell_lines) > 0 and len(cell_lines[-1]) == 0:
                cell_lines.pop(-1)
            cell_language = self.read_cell_language(cell_lines)
            if cell_language is None:
                cell_language = self
            else:
                cell_lines = self._remove_magic_wrapper(cell_lines, cell_language)

            cell_source = '\n'.join(cell_lines)
            cell = cell_language.new_cell(cell_source, start)
            return cell

        cells = []
        cell_lines: list[str] = []
        separator = f"{self.comment_prefix} {CELL_SEPARATOR}"

        next_cell_pos = 1
        for i in range(1, len(lines)):
            line = lines[i].strip()
            if line.startswith(separator):
                cell = make_cell(cell_lines, next_cell_pos)
                cells.append(cell)
                cell_lines = []
                next_cell_pos = i
            else:
                cell_lines.append(lines[i])

        if len(cell_lines) > 0:
            cell = make_cell(cell_lines, next_cell_pos)
            cells.append(cell)

        return cells

    def _process_line(self, line: str, prefix: str, lang_prefix: str, cell_language: CellLanguage) -> list[str]:
        if line.startswith(prefix):
            line = line[len(prefix) :]
            if cell_language.requires_isolated_pi and line.startswith(lang_prefix):
                new_line = line[len(lang_prefix) :].strip()
                if new_line:
                    return [f"{cell_language.comment_prefix} {LANGUAGE_PI}", new_line.strip()]
                return [f"{cell_language.comment_prefix} {LANGUAGE_PI}"]
            return [line]
        if line.startswith(self.comment_prefix):
            return [f"{cell_language.comment_prefix} {COMMENT_PI}{line}"]
        return [line]

    def _remove_magic_wrapper(self, lines: list[str], cell_language: CellLanguage) -> list[str]:
        prefix = f"{self.comment_prefix} {MAGIC_PREFIX} "
        lang_prefix = f"{LANGUAGE_PREFIX}{cell_language.magic_name}"
        new_lines = []
        for line in lines:
            new_lines.extend(self._process_line(line, prefix, lang_prefix, cell_language))
        return new_lines

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


class Notebook(SourceContainer):

    @staticmethod
    def parse(path: str, source: str, default_language: Language) -> Notebook:
        default_cell_language = CellLanguage.of_language(default_language)
        cells = default_cell_language.extract_cells(source)
        if cells is None:
            raise ValueError(f"Could not parse Notebook: {path}")
        return Notebook(path, source, default_language, cells, source.endswith('\n'))

    def __init__(self, path: str, source: str, language: Language, cells: list[Cell], ends_with_lf):
        self._path = path
        self._source = source
        self._language = language
        self._cells = cells
        self._ends_with_lf = ends_with_lf

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
        problems: list[DependencyProblem] = []
        for cell in self._cells:
            cell.build_dependency_graph(parent, problems.append)
        parent.add_problems(problems)

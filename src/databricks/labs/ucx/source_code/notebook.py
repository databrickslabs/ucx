from __future__ import annotations  # for type hints

from abc import ABC, abstractmethod
from ast import parse as parse_python
from collections.abc import Callable
from enum import Enum

from sqlglot import ParseError as SQLParseError
from sqlglot import parse as parse_sql
from databricks.sdk.service.workspace import Language

NOTEBOOK_HEADER = " Databricks notebook source"
CELL_SEPARATOR = " COMMAND ----------"
MAGIC_PREFIX = ' MAGIC'
LANGUAGE_PREFIX = ' %'


class Cell(ABC):

    def __init__(self, source: str):
        self._original_code = source

    @property
    def migrated_code(self):
        return self._original_code  # for now since we're not doing any migration yet

    @property
    @abstractmethod
    def language(self) -> CellLanguage:
        raise NotImplementedError()

    @abstractmethod
    def is_runnable(self) -> bool:
        raise NotImplementedError()

    @abstractmethod
    def build_dependency_graph(self, parent: NotebookDependencyGraph):
        raise NotImplementedError()


class PythonCell(Cell):

    @property
    def language(self):
        return CellLanguage.PYTHON

    def is_runnable(self) -> bool:
        try:
            ast = parse_python(self._original_code)
            return ast is not None
        except SyntaxError:
            return False

    def build_dependency_graph(self, parent: NotebookDependencyGraph):
        # TODO https://github.com/databrickslabs/ucx/issues/1200
        # TODO https://github.com/databrickslabs/ucx/issues/1202
        pass


class RCell(Cell):

    @property
    def language(self):
        return CellLanguage.R

    def is_runnable(self) -> bool:
        return True  # TODO

    def build_dependency_graph(self, parent: NotebookDependencyGraph):
        pass  # not in scope


class ScalaCell(Cell):

    @property
    def language(self):
        return CellLanguage.SCALA

    def is_runnable(self) -> bool:
        return True  # TODO

    def build_dependency_graph(self, parent: NotebookDependencyGraph):
        pass  # TODO


class SQLCell(Cell):

    @property
    def language(self):
        return CellLanguage.SQL

    def is_runnable(self) -> bool:
        try:
            statements = parse_sql(self._original_code)
            return len(statements) > 0
        except SQLParseError:
            return False

    def build_dependency_graph(self, parent: NotebookDependencyGraph):
        pass  # not in scope


class MarkdownCell(Cell):

    @property
    def language(self):
        return CellLanguage.MARKDOWN

    def is_runnable(self) -> bool:
        return True  # TODO

    def build_dependency_graph(self, parent: NotebookDependencyGraph):
        pass  # not in scope


class RunCell(Cell):

    @property
    def language(self):
        return CellLanguage.RUN

    def is_runnable(self) -> bool:
        return True  # TODO

    def build_dependency_graph(self, parent: NotebookDependencyGraph):
        command = f'{LANGUAGE_PREFIX}{self.language.magic_name}'.strip()
        lines = self._original_code.split('\n')
        for line in lines:
            start = line.index(command)
            if start >= 0:
                path = line[start + len(command) :].strip()
                parent.register_dependency(path.strip('"'))
                return
        raise ValueError("Missing notebook path in %run command")


class CellLanguage(Enum):
    # long magic_names must come first to avoid shorter ones being matched
    PYTHON = Language.PYTHON, 'python', '#', PythonCell
    SCALA = Language.SCALA, 'scala', '//', ScalaCell
    SQL = Language.SQL, 'sql', '--', SQLCell
    RUN = None, 'run', None, RunCell
    MARKDOWN = None, 'md', None, MarkdownCell
    R = Language.R, 'r', '#', RCell

    def __init__(self, *args):
        super().__init__()
        self._language = args[0]
        self._magic_name = args[1]
        self._comment_prefix = args[2]
        self._new_cell = args[3]

    @property
    def language(self) -> Language:
        return self._language

    @property
    def magic_name(self) -> str:
        return self._magic_name

    @property
    def comment_prefix(self) -> str:
        return self._comment_prefix

    @classmethod
    def of_language(cls, language: Language) -> CellLanguage:
        return next((cl for cl in CellLanguage if cl.language == language))

    @classmethod
    def of_magic_name(cls, magic_name: str) -> CellLanguage | None:
        return next((cl for cl in CellLanguage if magic_name.startswith(cl.magic_name)), None)

    def read_cell_language(self, lines: list[str]) -> CellLanguage | None:
        magic_prefix = f'{self.comment_prefix}{MAGIC_PREFIX}'
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
        header = f"{self.comment_prefix}{NOTEBOOK_HEADER}"
        if not lines[0].startswith(header):
            raise ValueError("Not a Databricks notebook source!")

        def make_cell(lines_: list[str]):
            # trim leading blank lines
            while len(lines_) > 0 and len(lines_[0]) == 0:
                lines_.pop(0)
            # trim trailing blank lines
            while len(lines_) > 0 and len(lines_[-1]) == 0:
                lines_.pop(-1)
            cell_language = self.read_cell_language(lines_)
            if cell_language is None:
                cell_language = self
            cell_source = '\n'.join(lines_)
            return cell_language.new_cell(cell_source)

        cells = []
        cell_lines: list[str] = []
        separator = f"{self.comment_prefix}{CELL_SEPARATOR}"
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


class NotebookDependencyGraph:

    def __init__(self, path: str, parent: NotebookDependencyGraph | None, locator: Callable[[str], Notebook]):
        self._path = path
        self._parent = parent
        self._locator = locator
        self._dependencies: dict[str, NotebookDependencyGraph] = {}

    @property
    def path(self):
        return self._path

    def register_dependency(self, path: str) -> NotebookDependencyGraph:
        # already registered ?
        child_graph = self.locate_dependency(path)
        if child_graph is not None:
            self._dependencies[path] = child_graph
            return child_graph
        # nay, create the child graph and populate it
        child_graph = NotebookDependencyGraph(path, self, self._locator)
        self._dependencies[path] = child_graph
        notebook = self._locator(path)
        notebook.build_dependency_graph(child_graph)
        return child_graph

    def locate_dependency(self, path: str) -> NotebookDependencyGraph | None:
        # need a list since unlike JS, Python won't let you assign closure variables
        found: list[NotebookDependencyGraph] = []
        path = path[2:] if path.startswith('./') else path

        def check_registered_dependency(graph):
            graph_path = graph.path[2:] if graph.path.startswith('./') else graph.path
            if graph_path == path:
                found.append(graph)
                return True
            return False

        self.root.visit(check_registered_dependency)
        return found[0] if len(found) > 0 else None

    @property
    def root(self):
        return self if self._parent is None else self._parent.root

    @property
    def paths(self) -> set[str]:
        paths: set[str] = set()

        def add_to_paths(graph: NotebookDependencyGraph) -> bool:
            if graph.path in paths:
                return True
            paths.add(graph.path)
            return False

        self.visit(add_to_paths)
        return paths

    # when visit_node returns True it interrupts the visit
    def visit(self, visit_node: Callable[[NotebookDependencyGraph], bool | None]) -> bool | None:
        if visit_node(self):
            return True
        for dependency in self._dependencies.values():
            if dependency.visit(visit_node):
                return True
        return False


class Notebook:

    @staticmethod
    def parse(path: str, source: str, default_language: Language) -> Notebook | None:
        default_cell_language = CellLanguage.of_language(default_language)
        cells = default_cell_language.extract_cells(source)
        return None if cells is None else Notebook(path, default_language, cells, source.endswith('\n'))

    def __init__(self, path: str, language: Language, cells: list[Cell], ends_with_lf):
        self._path = path
        self._language = language
        self._cells = cells
        self._ends_with_lf = ends_with_lf

    @property
    def cells(self) -> list[Cell]:
        return self._cells

    def to_migrated_code(self):
        default_language = CellLanguage.of_language(self._language)
        header = f"{default_language.comment_prefix}{NOTEBOOK_HEADER}"
        sources = [header]
        for i, cell in enumerate(self._cells):
            sources.append(cell.migrated_code)
            if i < len(self._cells) - 1:
                sources.append('')
                sources.append(f'{default_language.comment_prefix}{CELL_SEPARATOR}')
                sources.append('')
        if self._ends_with_lf:
            sources.append('')  # following join will append lf
        return '\n'.join(sources)

    def build_dependency_graph(self, graph: NotebookDependencyGraph) -> None:
        for cell in self._cells:
            cell.build_dependency_graph(graph)

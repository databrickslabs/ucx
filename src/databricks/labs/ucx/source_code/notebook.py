from __future__ import annotations  # for type hints

import ast
import logging
from abc import ABC, abstractmethod
from ast import parse as parse_python
from collections.abc import Callable, Iterable
from enum import Enum

from sqlglot import ParseError as SQLParseError
from sqlglot import parse as parse_sql
from databricks.sdk.service.workspace import Language

from databricks.labs.ucx.source_code.astlinter import ASTLinter
from databricks.labs.ucx.source_code.base import Linter, Advice, Advisory


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
        return self._migrated_code  # for now since we're not doing any migration yet

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


class PythonLinter(ASTLinter, Linter):
    def lint(self, code: str) -> Iterable[Advice]:
        self.parse(code)
        nodes = self.locate(ast.Call, [("run", ast.Attribute), ("notebook", ast.Attribute), ("dbutils", ast.Name)])
        return [self._convert_dbutils_notebook_run_to_advice(node) for node in nodes]

    @classmethod
    def _convert_dbutils_notebook_run_to_advice(cls, node: ast.AST) -> Advisory:
        assert isinstance(node, ast.Call)
        path = cls.get_dbutils_notebook_run_path_arg(node)
        if isinstance(path, ast.Constant):
            return Advisory(
                'migrate-path-literal',
                "Call to 'dbutils.notebook.run' will be migrated automatically",
                node.lineno,
                node.col_offset,
                node.end_lineno or 0,
                node.end_col_offset or 0,
            )
        return Advisory(
            'migrate-path',
            "Path for 'dbutils.notebook.run' is not a constant and requires adjusting the notebook path",
            node.lineno,
            node.col_offset,
            node.end_lineno or 0,
            node.end_col_offset or 0,
        )

    @staticmethod
    def get_dbutils_notebook_run_path_arg(node: ast.Call):
        if len(node.args) > 0:
            return node.args[0]
        arg = next(kw for kw in node.keywords if kw.arg == "path")
        return arg.value if arg is not None else None


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
        # TODO https://github.com/databrickslabs/ucx/issues/1202
        linter = ASTLinter()
        linter.parse(self._original_code)
        nodes = linter.locate(ast.Call, [("run", ast.Attribute), ("notebook", ast.Attribute), ("dbutils", ast.Name)])
        for node in nodes:
            assert isinstance(node, ast.Call)
            path = PythonLinter.get_dbutils_notebook_run_path_arg(node)
            if isinstance(path, ast.Constant):
                parent.register_dependency(path.value.strip("'").strip('"'))


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
        except SQLParseError:
            sqlglot_logger.warning(f"Failed to parse SQL using 'sqlglot': {self._original_code}")
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
                path = line[start + len(command) :].strip()
                parent.register_dependency(path.strip('"'))
                return
        raise ValueError("Missing notebook path in %run command")

    def migrate_notebook_path(self):
        pass


class CellLanguage(Enum):
    # long magic_names must come first to avoid shorter ones being matched
    PYTHON = Language.PYTHON, 'python', '#', True, PythonCell
    SCALA = Language.SCALA, 'scala', '//', True, ScalaCell
    SQL = Language.SQL, 'sql', '--', True, SQLCell
    RUN = None, 'run', '', False, RunCell
    # see https://spec.commonmark.org/0.31.2/#html-comment
    MARKDOWN = None, 'md', "<!--->", False, MarkdownCell
    R = Language.R, 'r', '#', True, RCell

    def __init__(self, *args):
        super().__init__()
        self._language = args[0]
        self._magic_name = args[1]
        self._comment_prefix = args[2]
        # PI stands for Processing Instruction
        # pylint: disable=invalid-name
        self._requires_isolated_PI = args[3]
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
        return self._requires_isolated_PI

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
                self._make_runnable(cell_lines, cell_language)
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

    def _make_runnable(self, lines: list[str], cell_language: CellLanguage):
        prefix = f"{self.comment_prefix} {MAGIC_PREFIX} "
        prefix_len = len(prefix)
        # pylint: disable=too-many-nested-blocks
        for i, line in enumerate(lines):
            if line.startswith(prefix):
                line = line[prefix_len:]
                if cell_language.requires_isolated_pi:
                    if line.startswith(LANGUAGE_PREFIX):
                        line = f"{cell_language.comment_prefix} {LANGUAGE_PI}"
                lines[i] = line
                continue
            if line.startswith(self.comment_prefix):
                line = f"{cell_language.comment_prefix} {COMMENT_PI}{line}"
                lines[i] = line

    def make_unrunnable(self, code: str, cell_language: CellLanguage) -> str:
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


class DependencyGraph:

    def __init__(self, path: str, parent: DependencyGraph | None, locator: Callable[[str], Notebook]):
        self._path = path
        self._parent = parent
        self._locator = locator
        self._dependencies: dict[str, DependencyGraph] = {}

    @property
    def path(self):
        return self._path

    def register_dependency(self, path: str) -> DependencyGraph:
        # already registered ?
        child_graph = self.locate_dependency(path)
        if child_graph is not None:
            self._dependencies[path] = child_graph
            return child_graph
        # nay, create the child graph and populate it
        child_graph = DependencyGraph(path, self, self._locator)
        self._dependencies[path] = child_graph
        notebook = self._locator(path)
        notebook.build_dependency_graph(child_graph)
        return child_graph

    def locate_dependency(self, path: str) -> DependencyGraph | None:
        # need a list since unlike JS, Python won't let you assign closure variables
        found: list[DependencyGraph] = []
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

        def add_to_paths(graph: DependencyGraph) -> bool:
            if graph.path in paths:
                return True
            paths.add(graph.path)
            return False

        self.visit(add_to_paths)
        return paths

    # when visit_node returns True it interrupts the visit
    def visit(self, visit_node: Callable[[DependencyGraph], bool | None]) -> bool | None:
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
        return None if cells is None else Notebook(path, source, default_language, cells, source.endswith('\n'))

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
                migrated_code = default_language.make_unrunnable(migrated_code, cell.language)
            sources.append(migrated_code)
            if i < len(self._cells) - 1:
                sources.append('')
                sources.append(f'{default_language.comment_prefix} {CELL_SEPARATOR}')
                sources.append('')
        if self._ends_with_lf:
            sources.append('')  # following join will append lf
        return '\n'.join(sources)

    def build_dependency_graph(self, graph: DependencyGraph) -> None:
        for cell in self._cells:
            cell.build_dependency_graph(graph)

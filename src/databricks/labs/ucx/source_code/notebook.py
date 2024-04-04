from __future__ import annotations  # for type hints

from abc import ABC, abstractmethod
from enum import Enum

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


class PythonCell(Cell):

    @property
    def language(self):
        return CellLanguage.PYTHON


class RCell(Cell):

    @property
    def language(self):
        return CellLanguage.R


class ScalaCell(Cell):

    @property
    def language(self):
        return CellLanguage.SCALA


class SQLCell(Cell):

    @property
    def language(self):
        return CellLanguage.SQL


class MarkdownCell(Cell):

    @property
    def language(self):
        return CellLanguage.MARKDOWN


class RunCell(Cell):

    @property
    def language(self):
        return CellLanguage.RUN


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
        return next((cl for cl in CellLanguage if magic_name.startswith(cl.magic_name) ), None)

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
                line = line[len(magic_language_prefix):]
                return CellLanguage.of_magic_name(line.strip())
            else:
                return None


    def new_cell(self, source: str) -> Cell:
        return self._new_cell(source)


def extract_cells(source: str, default_language: CellLanguage) -> list[Cell] | None:
    lines = source.split('\n')
    header = f"{default_language.comment_prefix}{NOTEBOOK_HEADER}"
    if not lines[0].startswith(header):
        raise ValueError("Not a Databricks notebook source!")

    def make_cell(lines_: list[str]):
        # trim leading blank lines
        while len(lines_) > 0 and len(lines_[0]) == 0:
            lines_.pop(0)
        # trim trailing blank lines
        while len(lines_) > 0 and len(lines_[-1]) == 0:
            lines_.pop(-1)
        cell_language = default_language.read_cell_language(lines_)
        if cell_language is None:
            cell_language = default_language
        cell_source = '\n'.join(lines_)
        return cell_language.new_cell(cell_source)

    cells = []
    cell_lines: list[str] = []
    separator = f"{default_language.comment_prefix}{CELL_SEPARATOR}"
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

    def __init__(self, parent: NotebookDependencyGraph | None):
        self._paths: dict[str, NotebookDependencyGraph | None] = dict()
        self._parent = parent

    def __contains__(self, path: str):
        return self._paths.get(path, None) is not None

    def register(self, path: str) -> NotebookDependencyGraph:
        assert path not in self
        child = NotebookDependencyGraph(self)
        self._paths[path] = child
        return child

    @property
    def paths(self) -> list[str]:
        return list(self._paths.keys())


class Notebook:

    @staticmethod
    def parse(path: str, source: str, default_language: Language) -> Notebook | None:
        default_cell_language = CellLanguage.of_language(default_language)
        cells = extract_cells(source, default_cell_language)
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
            sources.append('') # following join will append lf
        return '\n'.join(sources)


    def build_dependency_graph(self, graph: NotebookDependencyGraph):
        child = graph.register(self._path)
#        for cell in self._cells:
#            cell.build_dependency_graph(child)

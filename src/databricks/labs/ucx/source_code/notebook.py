from __future__ import annotations

from abc import ABC, abstractmethod
from enum import Enum

from databricks.sdk.service.workspace import Language

NOTEBOOK_HEADER = " Databricks notebook source"
CELL_SEPARATOR = " COMMAND ----------"
MAGIC_PREFIX = ' MAGIC'


class Cell(ABC):

    def __init__(self, source: str):
        self._source = source

    @property
    @abstractmethod
    def language(self) -> CellLanguage:
        raise NotImplementedError()


class PythonCell(Cell):

    @property
    def language(self) -> CellLanguage:
        return CellLanguage.PYTHON


class RCell(Cell):

    @property
    def language(self) -> CellLanguage:
        return CellLanguage.R


class ScalaCell(Cell):

    @property
    def language(self) -> CellLanguage:
        return CellLanguage.SCALA


class SQLCell(Cell):

    @property
    def language(self) -> CellLanguage:
        return CellLanguage.SQL


class MarkdownCell(Cell):

    @property
    def language(self) -> CellLanguage:
        return CellLanguage.MARKDOWN


class CellLanguage(Enum):
    PYTHON = Language.PYTHON, '%python', '#', PythonCell
    R = Language.R, '%r', '#', RCell
    SCALA = Language.SCALA, '%scala', '//', ScalaCell
    SQL = Language.SQL, '%sql', '--', SQLCell
    MARKDOWN = None, '%md', None, MarkdownCell

    def __init__(self, *args, **kwargs):
        super().__init__(self)
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
        return next((cl for cl in CellLanguage if cl._language == language))

    @classmethod
    def of_magic_name(cls, magic_name: str) -> CellLanguage | None:
        return next((cl for cl in CellLanguage if cl._magic_name == magic_name), None)

    def read_cell_language(self, line: str) -> CellLanguage | None:
        magic_prefix = f'{self.comment_prefix}{MAGIC_PREFIX}'
        if not line.startswith(magic_prefix):
            return None
        return CellLanguage.of_magic_name(line[len(magic_prefix) :].strip())

    def new_cell(self, source: str) -> Cell:
        return self._new_cell(source)


def extract_cells(source: str, default_language: CellLanguage) -> list[Cell] | None:
    lines = source.split('\n')
    header = f"{default_language.comment_prefix}{NOTEBOOK_HEADER}"
    if not lines[0].startswith(header):
        raise ValueError("Not a Databricks notebook source!")
    cells = []
    cell_lines: list[str] = []
    separator = f"{default_language.comment_prefix}{CELL_SEPARATOR}"
    for i in range(1, len(lines)):
        line = lines[i].strip()
        if line.startswith(separator):
            # trim leading blank lines
            while len(cell_lines) > 0 and len(cell_lines[0]) == 0:
                cell_lines.pop(0)
            # trim trailing blank lines
            while len(cell_lines) > 0 and len(cell_lines[-1]) == 0:
                cell_lines.pop(-1)
            cell_language = default_language.read_cell_language(cell_lines[0])
            if cell_language is None:
                cell_language = default_language
            else:
                cell_lines.pop(0)
            cell_source = '\n'.join(cell_lines)
            cells.append(cell_language.new_cell(cell_source))
            cell_lines = []
        else:
            cell_lines.append(line)
    return cells


class Notebook:

    @staticmethod
    def parse(source: str, default_language: Language) -> Notebook | None:
        default_cell_language = CellLanguage.of_language(default_language)
        cells = extract_cells(source, default_cell_language)
        return None if cells is None else Notebook(cells)

    def __init__(self, cells: list[Cell]):
        self._cells = cells

    @property
    def cells(self) -> list[Cell]:
        return self._cells

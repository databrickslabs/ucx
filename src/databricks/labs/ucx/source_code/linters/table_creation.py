from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass

from astroid import Attribute, Call, NodeNG  # type: ignore

from databricks.labs.ucx.source_code.base import (
    Advice,
    PythonLinter,
)
from databricks.labs.ucx.source_code.python.python_ast import Tree, TreeHelper


@dataclass
class Position:
    # Lines and columns are both 0-based.
    line: int
    character: int


@dataclass
class Range:
    start: Position
    end: Position


@dataclass
class NoFormatPythonMatcher:
    """Matches Python AST nodes where tables are created with implicit format.
    It performs only the matching, while linting / fixing are separated concerns.
    """

    method_name: str
    min_args: int
    max_args: int
    # some method_names accept 'format' as a direct (optional) argument:
    format_arg_index: int | None = None
    format_arg_name: str | None = None

    def get_advice_span(self, node: NodeNG) -> Range | None:
        # Check 1: check Call:
        if not isinstance(node, Call):
            return None

        # Check 2: check presence of the table-creating method call:
        if not isinstance(node.func, Attribute) or node.func.attrname != self.method_name:
            return None
        call_args_count = TreeHelper.args_count(node)
        if call_args_count < self.min_args or call_args_count > self.max_args:
            return None

        # Check 3: ensure this is a spark call
        if not Tree(node.func.expr).is_from_module("spark"):
            return None

        # Check 4: check presence of the format specifier:
        #   Option A: format specifier may be given as a direct parameter to the table-creating call
        #   >>> df.saveToTable("c.db.table", format="csv")
        format_arg = TreeHelper.get_arg(node, self.format_arg_index, self.format_arg_name)
        if format_arg is not None and not TreeHelper.is_none(format_arg):
            # i.e., found an explicit "format" argument, and its value is not None.
            return None
        #   Option B. format specifier may be a separate ".format(...)" call in this callchain
        #   >>> df.format("csv").saveToTable("c.db.table")
        format_call = TreeHelper.extract_call_by_name(node, "format")
        if format_call is not None:
            # i.e., found an explicit ".format(...)" call in this chain.
            return None

        # Finally: matched the need for advice, so return the corresponding source range.
        # Note that astroid line numbers are 1-based.
        return Range(
            Position(node.lineno - 1, node.col_offset),
            Position((node.end_lineno or 1) - 1, node.end_col_offset or 0),
        )


class NoFormatPythonLinter:
    """Python linting for table-creation with implicit format"""

    def __init__(self, matchers: list[NoFormatPythonMatcher]):
        self._matchers = matchers

    def lint(self, node: NodeNG) -> Iterator[Advice]:
        for matcher in self._matchers:
            span = matcher.get_advice_span(node)
            if span is not None:
                yield Advice(
                    code="default-format-changed-in-dbr8",
                    message="The default format changed in Databricks Runtime 8.0, from Parquet to Delta",
                    start_line=span.start.line,
                    start_col=span.start.character,
                    end_line=span.end.line,
                    end_col=span.end.character,
                )


class DBRv8d0PyLinter(PythonLinter):
    """Performs Python linting for backwards incompatible changes in DBR version 8.0.
    Specifically, it yields advice for table-creation with implicit format.
    """

    # https://docs.databricks.com/en/archive/runtime-release-notes/8.0.html#delta-is-now-the-default-format-when-a-format-is-not-specified

    def __init__(self, dbr_version: tuple[int, int] | None):
        version_cutoff = (8, 0)
        self._skip_dbr = dbr_version is not None and dbr_version >= version_cutoff

        self._linter = NoFormatPythonLinter(
            [
                NoFormatPythonMatcher("writeTo", 1, 1),
                NoFormatPythonMatcher("table", 1, 1),
                NoFormatPythonMatcher("saveAsTable", 1, 4, 2, "format"),
            ]
        )

    def lint_tree(self, tree: Tree) -> Iterable[Advice]:
        if self._skip_dbr:
            return
        for node in tree.walk():
            yield from self._linter.lint(node)

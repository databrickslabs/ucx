from __future__ import annotations

import ast
from collections.abc import Iterable, Iterator
from dataclasses import dataclass


from databricks.labs.ucx.source_code.python_linter import ASTLinter
from databricks.labs.ucx.source_code.base import (
    Advice,
    Linter,
)


@dataclass
class Position:
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

    def get_advice_span(self, node: ast.AST) -> Range | None:
        # Check 1: retrieve full callchain:
        callchain = ASTLinter(node).extract_callchain()
        if callchain is None:
            return None

        # Check 2: check presence of the table-creating method call:
        call = ASTLinter(callchain).extract_call_by_name(self.method_name)
        if call is None:
            return None
        call_args_count = ASTLinter(call).args_count()
        if call_args_count < self.min_args or call_args_count > self.max_args:
            return None

        # Check 3: check presence of the format specifier:
        #   Option A: format specifier may be given as a direct parameter to the table-creating call
        #   >>> df.saveToTable("c.db.table", format="csv")
        format_arg = ASTLinter(call).get_arg(self.format_arg_index, self.format_arg_name)
        if format_arg is not None and not ASTLinter(format_arg).is_none():
            # i.e., found an explicit "format" argument, and its value is not None.
            return None
        #   Option B. format specifier may be a separate ".format(...)" call in this callchain
        #   >>> df.format("csv").saveToTable("c.db.table")
        format_call = ASTLinter(callchain).extract_call_by_name("format")
        if format_call is not None:
            # i.e., found an explicit ".format(...)" call in this chain.
            return None

        # Finally: matched the need for advice, so return the corresponding source range:
        return Range(
            Position(call.lineno, call.col_offset),
            Position(call.end_lineno or 0, call.end_col_offset or 0),
        )


class NoFormatPythonLinter:
    """Python linting for table-creation with implicit format"""

    def __init__(self, matchers: list[NoFormatPythonMatcher]):
        self._matchers = matchers

    def lint(self, node: ast.AST) -> Iterator[Advice]:
        for matcher in self._matchers:
            span = matcher.get_advice_span(node)
            if span is not None:
                yield Advice(
                    code="table-migrate",
                    message="The default format changed in Databricks Runtime 8.0, from Parquet to Delta",
                    start_line=span.start.line,
                    start_col=span.start.character,
                    end_line=span.end.line,
                    end_col=span.end.character,
                )


class DBRv8d0Linter(Linter):
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
                NoFormatPythonMatcher("insertInto", 1, 2),
                NoFormatPythonMatcher("saveAsTable", 1, 4, 2, "format"),
            ]
        )

    def lint(self, code: str) -> Iterable[Advice]:
        if self._skip_dbr:
            return

        tree = ast.parse(code)
        for node in ast.walk(tree):
            yield from self._linter.lint(node)

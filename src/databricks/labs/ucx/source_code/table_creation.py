from __future__ import annotations

import ast
from collections.abc import Iterable, Iterator
from dataclasses import dataclass

from databricks.labs.ucx.source_code.python_ast_util import (
    AstUtil,
    Span,
)
from databricks.labs.ucx.source_code.base import (
    Advice,
    Linter,
)


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

    def get_advice_span(self, node: ast.AST) -> Span | None:
        # retrieve full callchain:
        callchain = AstUtil.extract_callchain(node)
        if callchain is None:
            return None

        # check presence of the table-creating method call:
        call = AstUtil.extract_call_by_name(callchain, self.method_name)
        if call is None:
            return None
        call_args_count = AstUtil.args_count(call)
        if call_args_count < self.min_args or call_args_count > self.max_args:
            return None

        # check presence of the format specifier:
        format_arg = AstUtil.get_arg(call, self.format_arg_index, self.format_arg_name)
        if format_arg is not None and not AstUtil.is_none(format_arg):
            return None
        format_call = AstUtil.extract_call_by_name(callchain, "format")
        if format_call is not None:
            return None

        # matched need for issuing advice:
        return Span(
            call.lineno,
            call.col_offset,
            call.end_lineno or 0,
            call.end_col_offset or 0,
        )


@dataclass
class NoFormatPythonLinter:
    """Python linting for table-creation with implicit format"""

    _matchers: list[NoFormatPythonMatcher]

    def lint(self, node: ast.AST) -> Iterator[Advice]:
        for matcher in self._matchers:
            span = matcher.get_advice_span(node)
            if span is not None:
                yield Advice(
                    code="table-migrate",
                    message="The default format changed in Databricks Runtime 8.0, from Parquet to Delta",
                    start_line=span.start_line,
                    start_col=span.start_col,
                    end_line=span.end_line,
                    end_col=span.end_col,
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

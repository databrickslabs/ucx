import ast
from typing import Iterable

from databricks.labs.ucx.code.base import Advice, Fixer, Linter
from databricks.labs.ucx.code.queries import FromTable


class SparkSql(Linter, Fixer):
    def __init__(self, from_table: FromTable):
        self._from_table = from_table

    def name(self) -> str:
        # this is the same fixer, just in a different language context
        return self._from_table.name()

    def lint(self, code: str) -> Iterable[Advice]:
        tree = ast.parse(code)
        for x in ast.walk(tree):
            if not isinstance(x, ast.Call):
                continue
            if not isinstance(x.func, ast.Attribute):
                continue
            if x.func.attr != "sql":
                continue
            if len(x.args) != 1:
                continue
            first_arg = x.args[0]
            if not isinstance(first_arg, ast.Constant):
                # `astroid` library supports inference and parent node lookup,
                # which makes traversing the AST a bit easier.
                continue
            query = first_arg.value
            for advice in self._from_table.lint(query):
                yield advice.replace(
                    start_line=x.lineno, start_col=x.col_offset, end_line=x.end_lineno, end_col=x.end_col_offset
                )

    def apply(self, code: str) -> str:
        tree = ast.parse(code)
        # we won't be doing it like this in production, but for the sake of the example
        for x in ast.walk(tree):
            if not isinstance(x, ast.Call):
                continue
            if not isinstance(x.func, ast.Attribute):
                continue
            if x.func.attr != "sql":
                continue
            if len(x.args) != 1:
                continue
            first_arg = x.args[0]
            if not isinstance(first_arg, ast.Constant):
                continue
            query = first_arg.value
            new_query = self._from_table.apply(query)
            first_arg.value = new_query
        return ast.unparse(tree)

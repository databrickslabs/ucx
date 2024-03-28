import ast
from collections.abc import Iterable

from databricks.labs.ucx.source_code.base import Advice, Fixer, Linter
from databricks.labs.ucx.source_code.queries import FromTable


class SparkSql(Linter, Fixer):
    def __init__(self, from_table: FromTable):
        self._from_table = from_table

    def name(self) -> str:
        # this is the same fixer, just in a different language context
        return self._from_table.name()

    def lint(self, code: str) -> Iterable[Advice]:
        tree = ast.parse(code)
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if not isinstance(node.func, ast.Attribute):
                continue
            if node.func.attr != "sql":
                continue
            if len(node.args) != 1:
                continue
            first_arg = node.args[0]
            if not isinstance(first_arg, ast.Constant):
                # `astroid` library supports inference and parent node lookup,
                # which makes traversing the AST a bit easier.
                continue
            query = first_arg.value
            for advice in self._from_table.lint(query):
                yield advice.replace(
                    start_line=node.lineno,
                    start_col=node.col_offset,
                    end_line=node.end_lineno,
                    end_col=node.end_col_offset,
                )

    def apply(self, code: str) -> str:
        tree = ast.parse(code)
        # we won't be doing it like this in production, but for the sake of the example
        for node in ast.walk(tree):
            if not isinstance(node, ast.Call):
                continue
            if not isinstance(node.func, ast.Attribute):
                continue
            if node.func.attr != "sql":
                continue
            if len(node.args) != 1:
                continue
            first_arg = node.args[0]
            if not isinstance(first_arg, ast.Constant):
                continue
            query = first_arg.value
            new_query = self._from_table.apply(query)
            first_arg.value = new_query
        return ast.unparse(tree)

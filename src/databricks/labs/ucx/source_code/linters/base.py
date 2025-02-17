from __future__ import annotations

import logging
from abc import abstractmethod, ABC
from collections.abc import Iterable

from sqlglot import parse as parse_sql, Expression
from sqlglot.errors import SqlglotError

from databricks.labs.ucx.source_code.base import (
    Advice,
    Failure,
    DfsaCollector,
    TableCollector,
    DfsaSqlCollector,
    TableSqlCollector,
    DirectFsAccess,
    UsedTable,
    DirectFsAccessNode,
    UsedTableNode,
)
from databricks.labs.ucx.source_code.python.python_ast import MaybeTree, Tree


logger = logging.getLogger(__name__)


class Linter:
    @abstractmethod
    def lint(self, code: str) -> Iterable[Advice]: ...


class SqlLinter(Linter):

    def lint(self, code: str) -> Iterable[Advice]:
        try:
            # TODO: unify with SqlParser.walk_expressions(...)
            expressions = parse_sql(code, read='databricks')
            for expression in expressions:
                if not expression:
                    continue
                yield from self.lint_expression(expression)
        except SqlglotError as e:
            logger.debug(f"Failed to parse SQL: {code}", exc_info=e)
            yield self.sql_parse_failure(code)

    @staticmethod
    def sql_parse_failure(code: str) -> Failure:
        return Failure(
            code='sql-parse-error',
            message=f"SQL expression is not supported yet: {code}",
            # SQLGlot does not propagate tokens yet. See https://github.com/tobymao/sqlglot/issues/3159
            start_line=0,
            start_col=0,
            end_line=0,
            end_col=1024,
        )

    @abstractmethod
    def lint_expression(self, expression: Expression) -> Iterable[Advice]: ...


class Fixer(ABC):

    @property
    @abstractmethod
    def diagnostic_code(self) -> str:
        """The diagnostic code that this fixer fixes."""

    def is_supported(self, diagnostic_code: str) -> bool:
        """Indicate if the diagnostic code is supported by this fixer."""
        return self.diagnostic_code is not None and diagnostic_code == self.diagnostic_code

    @abstractmethod
    def apply(self, code: str) -> str: ...


class SqlSequentialLinter(SqlLinter, DfsaCollector, TableCollector):

    def __init__(
        self,
        linters: list[SqlLinter],
        dfsa_collectors: list[DfsaSqlCollector],
        table_collectors: list[TableSqlCollector],
    ):
        self._linters = linters
        self._dfsa_collectors = dfsa_collectors
        self._table_collectors = table_collectors

    def lint_expression(self, expression: Expression) -> Iterable[Advice]:
        for linter in self._linters:
            yield from linter.lint_expression(expression)

    def collect_dfsas(self, source_code: str) -> Iterable[DirectFsAccess]:
        for collector in self._dfsa_collectors:
            yield from collector.collect_dfsas(source_code)

    def collect_tables(self, source_code: str) -> Iterable[UsedTable]:
        for collector in self._table_collectors:
            yield from collector.collect_tables(source_code)


class PythonLinter(Linter):

    def lint(self, code: str) -> Iterable[Advice]:
        maybe_tree = MaybeTree.from_source_code(code)
        if maybe_tree.failure:
            yield maybe_tree.failure
            return
        assert maybe_tree.tree is not None
        yield from self.lint_tree(maybe_tree.tree)

    @abstractmethod
    def lint_tree(self, tree: Tree) -> Iterable[Advice]: ...


class PythonFixer(Fixer):
    """Fix python source code."""

    def apply(self, code: str) -> str:
        """Apply the changes to Python source code.

        The source code is parsed into an AST tree, and the fixes are applied
        to the tree.
        """
        maybe_tree = MaybeTree.from_source_code(code)
        if maybe_tree.failure:
            # Fixing does not yield parse failures, linting does
            logger.warning(f"Parsing source code resulted in failure `{maybe_tree.failure}`: {code}")
            return code
        assert maybe_tree.tree is not None
        tree = self.apply_tree(maybe_tree.tree)
        return tree.node.as_string()

    @abstractmethod
    def apply_tree(self, tree: Tree) -> Tree:
        """Apply the fixes to the AST tree.

        For Python, the fixes are applied to a Tree so that we
        - Can chain multiple fixers without transpiling back and forth between
          source code and AST tree
        - Can extend the tree with (brought into scope) nodes, e.g. to add imports
        """


class DfsaPyCollector(DfsaCollector, ABC):

    def collect_dfsas(self, source_code: str) -> Iterable[DirectFsAccess]:
        maybe_tree = MaybeTree.from_source_code(source_code)
        if maybe_tree.failure:
            logger.warning(maybe_tree.failure.message)
            return
        assert maybe_tree.tree is not None
        for dfsa_node in self.collect_dfsas_from_tree(maybe_tree.tree):
            yield dfsa_node.dfsa

    @abstractmethod
    def collect_dfsas_from_tree(self, tree: Tree) -> Iterable[DirectFsAccessNode]: ...


class TablePyCollector(TableCollector, ABC):

    def collect_tables(self, source_code: str) -> Iterable[UsedTable]:
        maybe_tree = MaybeTree.from_source_code(source_code)
        if maybe_tree.failure:
            logger.warning(maybe_tree.failure.message)
            return
        assert maybe_tree.tree is not None
        for table_node in self.collect_tables_from_tree(maybe_tree.tree):
            yield table_node.table

    @abstractmethod
    def collect_tables_from_tree(self, tree: Tree) -> Iterable[UsedTableNode]: ...

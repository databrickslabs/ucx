import logging
from collections.abc import Callable, Iterable
from typing import TypeVar

from sqlglot import parse
from sqlglot.errors import SqlglotError, UnsupportedError
from sqlglot.expressions import Create, Delete, Drop, Expression, Select, Table, Update, Use, Identifier

from databricks.labs.ucx.source_code.base import UsedTable, CurrentSessionState

logger = logging.getLogger(__name__)

T = TypeVar("T")
E = TypeVar("E", bound=Expression)


class SqlExpression:

    def __init__(self, expression: Expression):
        self._expression = expression

    def collect_used_tables(self, required_catalog: str, session_state: CurrentSessionState) -> Iterable[UsedTable]:
        for table in self.find_all(Table):
            info = self._collect_table_info(table, required_catalog, session_state)
            if info:
                yield info

    def _collect_table_info(
        self,
        table: Table,
        required_catalog: str,
        session_state: CurrentSessionState,
    ) -> UsedTable | None:
        if isinstance(self._expression, Use):
            # Sqlglot captures the database name in the Use statement as a Table, with
            # the schema  as the table name.
            session_state.schema = table.name
            return None
        if isinstance(self._expression, Drop) and getattr(self._expression, "kind", None) == "SCHEMA":
            # Sqlglot captures the schema name in the Drop statement as a Table, with
            # the schema  as the db name.
            return None
        if isinstance(self._expression, Create) and getattr(self._expression, "kind", None) == "SCHEMA":
            # Sqlglot captures the schema name in the Create statement as a Table, with
            # the schema  as the db name.
            session_state.schema = table.db
            return None
        # we only return tables in the required catalog
        catalog_name = table.catalog or required_catalog
        if catalog_name != required_catalog:
            return None
        # Sqlglot uses db instead of schema, watch out for that
        src_schema = table.db if table.db else session_state.schema
        if not src_schema:
            logger.warning(f"Could not determine schema for table {table.name}")
            return None
        # Sqlglot handlers parameter markers by returning an Identifier as the name instead of a string.
        # For example: {foo} -> Identifier(this=foo)
        if isinstance(table.name, Identifier):
            # TODO: Support these properly, for example by inferring the placeholder value from the outside context.
            msg = f"Table placeholder detected, not yet supported: {{{table.name}}}"
            logger.debug(msg)
            raise UnsupportedError(msg)
        return UsedTable(
            catalog_name=catalog_name,
            schema_name=src_schema,
            table_name=table.name,
            is_read=isinstance(self._expression, Select),
            is_write=isinstance(self._expression, (Create, Update, Delete)),
        )

    def find_all(self, klass: type[E]) -> Iterable[E]:
        try:
            return self._expression.find_all(klass)
        except SqlglotError as e:
            logger.warning(f"Failed to find all {klass} in expression: {self._expression}", exc_info=e)
            return []


class SqlParser:

    @classmethod
    def walk_expressions(cls, sql_code: str, callback: Callable[[SqlExpression], Iterable[T]]) -> Iterable[T]:
        try:
            expressions = parse(sql_code, read='databricks')
            for expression in expressions:
                if not expression:
                    continue
                yield from callback(SqlExpression(expression))
        except SqlglotError as e:
            logger.warning(f"Failed to parse SQL: {sql_code}", exc_info=e)

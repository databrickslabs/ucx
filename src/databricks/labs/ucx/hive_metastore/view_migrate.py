import logging
from collections.abc import Collection
from dataclasses import dataclass
from functools import cached_property

import sqlglot
from sqlglot import ParseError, expressions

from databricks.labs.ucx.hive_metastore.migration_status import MigrationIndex, TableView
from databricks.labs.ucx.hive_metastore.mapping import TableToMigrate
from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.queries import FromTable

logger = logging.getLogger(__name__)


@dataclass
class ViewToMigrate(TableToMigrate):
    def __post_init__(self):
        if self.src.view_text is None:
            raise RuntimeError("Should never get there! A view must have 'view_text'!")

    @cached_property
    def dependencies(self) -> list[TableView]:
        return list(self._view_dependencies())

    def _view_dependencies(self):
        try:
            statements = sqlglot.parse(self.src.view_text, read='databricks')
        except ParseError as e:
            raise ValueError(f"Could not analyze view SQL: {self.src.view_text}") from e
        if len(statements) != 1 or statements[0] is None:
            raise ValueError(f"Could not analyze view SQL: {self.src.view_text}")
        statement = statements[0]
        aliases = self._read_aliases(statement)
        for old_table in statement.find_all(expressions.Table):
            if old_table.name in aliases:
                continue
            if old_table.catalog and old_table.catalog != 'hive_metastore':
                continue
            src_db = old_table.db if old_table.db else self.src.database
            if not src_db:
                logger.error(f"Could not determine schema for table {old_table.name}")
                continue
            yield TableView("hive_metastore", src_db, old_table.name)

    def _read_aliases(self, statement: expressions.Expression):
        aliases = set()
        for with_clause in statement.find_all(expressions.With):
            for expression in with_clause.expressions:
                if isinstance(expression, expressions.CTE):
                    aliases.add(expression.alias_or_name)
        return aliases

    def sql_migrate_view(self, index: MigrationIndex) -> str:
        from_table = FromTable(index, CurrentSessionState(self.src.database))
        assert self.src.view_text is not None, 'Expected a view text'
        migrated_select = from_table.apply(self.src.view_text)
        statements = sqlglot.parse(migrated_select, read='databricks')
        assert len(statements) == 1, 'Expected a single statement'
        create = statements[0]
        assert isinstance(create, expressions.Create), 'Expected a CREATE statement'
        # safely replace current table name with the updated catalog
        for table_name in create.find_all(expressions.Table):
            if table_name.db == self.src.database and table_name.name == self.src.name:
                # See https://github.com/tobymao/sqlglot/issues/3311
                new_view_name = expressions.Table(
                    catalog=expressions.to_identifier(self.rule.catalog_name),
                    db=expressions.to_identifier(self.rule.dst_schema),
                    this=expressions.to_identifier(self.rule.dst_table),
                )
                table_name.replace(new_view_name)
        # safely replace CREATE with CREATE IF NOT EXISTS
        create.args['exists'] = True
        return create.sql('databricks', identify=True)

    def __hash__(self):
        return hash(self.src)

    def __eq__(self, other):
        return isinstance(other, TableToMigrate) and self.src == other.src


class ViewsMigrationSequencer:

    def __init__(self, tables: Collection[TableToMigrate], index: MigrationIndex):
        self._tables = tables
        self._index = index
        self._result_view_list: list[ViewToMigrate] = []

    def sequence_batches(self) -> list[list[ViewToMigrate]]:
        """Sequence the views in batches to migrate them in the right order.

        Batch sequencing uses the following algorithm:
        0. For each view, we register dependencies (extracted from view_text),
        1. Then to create a new batch of views,
           We require the dependencies that are covered already:
             1. The migrated tables
             2. The (growing) set of views from already sequenced previous batches
           For each remaining view, we check if all its dependencies are covered for. If that is the case, then we
           add that view to the new batch of views.
        1. We repeat point from point 1. until all views are sequenced.

        The complexity for a given set of views v and a dependency depth d looks like Ov^d, this seems enormous but in
        practice d remains small and v decreases rapidly
        """
        views_to_migrate = set()
        for table_or_view in self._tables:
            if table_or_view.src.view_text is None:
                continue
            view_to_migrate = ViewToMigrate(table_or_view.src, table_or_view.rule)
            views_to_migrate.add(view_to_migrate)

        batches: list[list[ViewToMigrate]] = []
        sequenced_views: dict[ViewToMigrate: TableView] = {}
        while len(views_to_migrate) > 0:
            next_batch = self._next_batch(views_to_migrate, views_from_previous_batches=sequenced_views)
            self._result_view_list.extend(next_batch)
            for view in next_batch:
                sequenced_views[view] = TableView("hive_metastore", view.src.database, view.src.name)
            views_to_migrate.difference_update(next_batch)
            batches.append(list(next_batch))
        return batches

    def _next_batch(self, views: set[ViewToMigrate], *, views_from_previous_batches: dict[ViewToMigrate: TableView] | None) -> set[ViewToMigrate]:
        views_from_previous_batches = views_from_previous_batches or {}
        # we can't (slightly) optimize by checking len(views) == 0 or 1,
        # because we'd lose the opportunity to check the SQL
        result: set[ViewToMigrate] = set()
        for view in views:
            view_deps = set(view.dependencies)
            self._check_circular_dependency(view, views)
            if len(view_deps) == 0:
                result.add(view)
            else:
                # does the view have at least one view dependency that is not yet processed ?
                not_processed_yet = view_deps - set(views_from_previous_batches.values())
                if len(not_processed_yet) == 0:
                    result.add(view)
                    continue
                # If all dependencies are already processed, we can add the view to the next batch
                if all(self._index.is_migrated(table_view.schema, table_view.name) for table_view in not_processed_yet):
                    result.add(view)
        # prevent infinite loop
        if len(result) == 0 and len(views) > 0:
            raise ValueError(f"Invalid table references are preventing migration: {views}")
        return result

    def _check_circular_dependency(self, initial_view, views):
        queue = []
        queue.extend(dep for dep in initial_view.dependencies)
        while queue:
            current_view = self._get_view_instance(queue.pop(0).key, views)
            if not current_view:
                continue
            if current_view == initial_view:
                raise ValueError(
                    f"Circular dependency detected between {initial_view.src.name} and {current_view.src.name} "
                )
            queue.extend(dep for dep in current_view.dependencies)

    def _get_view_instance(self, key: str, views: set[ViewToMigrate]) -> ViewToMigrate | None:
        # This method acts as a mapper between TableView and ViewToMigrate. We check if the key passed matches with
        # any of the views in the list of views. This means the circular dependency will be identified only
        # if the dependencies are present in the list of views passed to _next_batch() or the _result_view_list
        # ToDo: see if a mapper between TableView and ViewToMigrate can be implemented
        all_views = list(views) + self._result_view_list
        for view in all_views:
            if view.src.key == key:
                return view
        return None

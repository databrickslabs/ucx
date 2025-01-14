import logging
from collections.abc import Collection
from dataclasses import dataclass
from functools import cached_property

import sqlglot
from sqlglot import ParseError, expressions

from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationIndex, TableView
from databricks.labs.ucx.hive_metastore.mapping import TableToMigrate
from databricks.labs.ucx.source_code.base import CurrentSessionState
from databricks.labs.ucx.source_code.linters.from_table import FromTableSqlLinter

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

    def sql_migrate_view(self, index: TableMigrationIndex) -> str:
        from_table = FromTableSqlLinter(index, CurrentSessionState(self.src.database))
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

    def __init__(
        self,
        tables_to_migrate: Collection[TableToMigrate],
        *,
        migration_index: TableMigrationIndex | None = None,
    ):
        self._tables = tables_to_migrate  # Also contains views to migrate
        self._index = migration_index or TableMigrationIndex([])

    @cached_property
    def _views(self) -> dict[ViewToMigrate, TableView]:
        # Views is a mapping as the TableView is required when resolving dependencies
        views = {}
        for table_or_view in self._tables:
            if table_or_view.src.view_text is None:
                continue
            view_to_migrate = ViewToMigrate(table_or_view.src, table_or_view.rule)
            # All views to migrate are stored in the hive_metastore
            views[view_to_migrate] = TableView("hive_metastore", view_to_migrate.src.database, view_to_migrate.src.name)
        return views

    def _get_view_to_migrate(self, key: str) -> ViewToMigrate | None:
        """Get a view to migrate by key"""
        for view in self._views:
            if view.src.key == key:
                return view
        return None

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
        2. We repeat point from point 1. until all views are sequenced.

        The complexity for a given set of views v and a dependency depth d looks like Ov^d, this seems enormous but in
        practice d remains small and v decreases rapidly
        """
        batches: list[list[ViewToMigrate]] = []
        views_to_migrate = set(self._views.keys())
        views_sequenced: set[TableView] = set()
        while len(views_to_migrate) > 0:
            try:
                next_batch = self._next_batch(views_to_migrate, views_from_previous_batches=views_sequenced)
            except RecursionError as e:
                logger.error(
                    f"Cannot sequence views {views_to_migrate} given migration index {self._index}", exc_info=e
                )
                # By returning the current batches, we can migrate the views that are not causing the recursion
                return batches
            for view in next_batch:
                views_sequenced.add(self._views[view])
            batches.append(next_batch)
            views_to_migrate.difference_update(next_batch)
        return batches

    def _next_batch(
        self, views: set[ViewToMigrate], *, views_from_previous_batches: set[TableView] | None
    ) -> list[ViewToMigrate]:
        """For sequencing algorithm see docstring of :meth:sequence_batches.

        Raises:
            RecursionError :
                If an infinite loop is detected.
        """
        views_from_previous_batches = views_from_previous_batches or set()
        # we can't (slightly) optimize by checking len(views) == 0 or 1,
        # because we'd lose the opportunity to check the SQL
        result = []
        for view in views:
            self._check_circular_dependency(view)
            if len(view.dependencies) == 0:
                result.append(view)
                continue
            # If all dependencies are already processed, we can add the view to the next batch
            not_processed_yet = set(view.dependencies) - views_from_previous_batches
            if len(not_processed_yet) == 0:
                result.append(view)
                continue
            if all(self._index.is_migrated(table_view.schema, table_view.name) for table_view in not_processed_yet):
                result.append(view)
        if len(result) == 0 and len(views) > 0:  # prevent infinite loop
            raise RecursionError(f"Unresolved dependencies prevent batch sequencing: {views}")
        return result

    def _check_circular_dependency(self, view: ViewToMigrate) -> None:
        """Check for circular dependencies in the views to migrate.

        Raises:
            RecursionError :
                If a circular dependency is detected between views.
        """
        dependency_keys = [dep.key for dep in view.dependencies]
        while len(dependency_keys) > 0:
            dependency = self._get_view_to_migrate(dependency_keys.pop(0))
            if not dependency:  # Only views (to migrate) can cause a circular dependency, tables can be ignored
                continue
            if dependency == view:
                raise RecursionError(f"Circular dependency detected starting from: {view.src.full_name}")
            dependency_keys.extend(dep.key for dep in dependency.dependencies)

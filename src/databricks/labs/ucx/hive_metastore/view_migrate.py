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
        for old_table in statement.find_all(expressions.Table):
            if old_table.catalog and old_table.catalog != 'hive_metastore':
                continue
            src_db = old_table.db if old_table.db else self.src.database
            if not src_db:
                logger.error(f"Could not determine schema for table {old_table.name}")
                continue
            yield TableView("hive_metastore", src_db, old_table.name)

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
                    catalog=self.rule.catalog_name,
                    db=self.rule.dst_schema,
                    this=self.rule.dst_table,
                )
                table_name.replace(new_view_name)
        # safely replace CREATE with CREATE IF NOT EXISTS
        create.args['exists'] = True
        return create.sql('databricks')

    def __hash__(self):
        return hash(self.src)

    def __eq__(self, other):
        return isinstance(other, TableToMigrate) and self.src == other.src


class ViewsMigrationSequencer:

    def __init__(self, tables: Collection[TableToMigrate], index: MigrationIndex):
        self._tables = tables
        self._index = index
        self._result_view_list: list[ViewToMigrate] = []
        self._result_tables_set: set[TableView] = set()

    def sequence_batches(self) -> list[list[ViewToMigrate]]:
        # sequencing is achieved using a very simple algorithm:
        # for each view, we register dependencies (extracted from view_text)
        # then given the remaining set of views to process,
        # and the growing set of views already processed
        # we check if each remaining view refers to not yet processed views
        # if none, then it's safe to add that view to the next batch of views
        # the complexity for a given set of views v and a dependency depth d looks like Ov^d
        # this seems enormous but in practice d remains small and v decreases rapidly
        all_tables: dict[str, TableToMigrate] = {}
        views = set()
        for table in self._tables:
            if table.src.view_text is not None:
                table = ViewToMigrate(table.src, table.rule)
            all_tables[table.src.key] = table
            if isinstance(table, ViewToMigrate):
                views.add(table)
        # when migrating views we want them in batches
        batches: list[list[ViewToMigrate]] = []
        while len(views) > 0:
            next_batch = self._next_batch(views)
            self._result_view_list.extend(next_batch)
            table_views = {TableView("hive_metastore", t.src.database, t.src.name) for t in next_batch}
            self._result_tables_set.update(table_views)
            views.difference_update(next_batch)
            batches.append(list(next_batch))
        return batches

    def _next_batch(self, views: set[ViewToMigrate]) -> set[ViewToMigrate]:
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
                not_processed_yet = view_deps - self._result_tables_set
                if len(not_processed_yet) == 0:
                    result.add(view)
                    continue
                if not [
                    table_view
                    for table_view in not_processed_yet
                    if not self._index.is_migrated(table_view.schema, table_view.name)
                ]:
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

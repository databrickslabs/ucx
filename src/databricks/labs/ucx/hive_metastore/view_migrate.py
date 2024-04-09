from collections.abc import Collection
from dataclasses import dataclass

from databricks.labs.ucx.hive_metastore.migration_status import MigrationIndex, TableView
from databricks.labs.ucx.hive_metastore.mapping import TableToMigrate
from databricks.labs.ucx.source_code.queries import FromTable


@dataclass
class ViewToMigrate(TableToMigrate):
    _table_dependencies: list[TableView] | None = None

    def __post_init__(self):
        if self.src.view_text is None:
            raise RuntimeError("Should never get there! A view must have 'view_text'!")

    @property
    def dependencies(self) -> list[TableView]:
        if self._table_dependencies is None:
            self._table_dependencies = self._compute_dependencies()
        # assert self._table_dependencies is not None
        return self._table_dependencies

    def _compute_dependencies(self):
        return list(FromTable.view_dependencies(self.src.view_text, use_schema=self.src.database))

    @staticmethod
    def get_view_updated_text(src: str, index: MigrationIndex, schema: str):
        from_table = FromTable(index, use_schema=schema)
        return from_table.apply(src)

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
            if len(view_deps) == 0:
                result.add(view)
            else:
                # does the view have at least one view dependency that is not yet processed ?
                not_processed_yet = view_deps - self._result_tables_set
                if not not_processed_yet:
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
            raise ValueError(f"Circular view references are preventing migration: {views}")
        return result

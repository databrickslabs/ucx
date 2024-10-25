from databricks.labs.lsql.backends import SqlBackend

from databricks.labs.ucx.hive_metastore.tables import Table
from databricks.labs.ucx.hive_metastore.table_migration_status import TableMigrationIndex
from databricks.labs.ucx.hive_metastore.ownership import TableOwnership
from databricks.labs.ucx.progress.history import ProgressEncoder
from databricks.labs.ucx.progress.grants import GrantsProgressEncoder


class TableProgressEncoder(ProgressEncoder[Table]):
    """Encoder class:Table to class:History.

    A progress failure for a table means:
    - the table is not migrated yet
    - the associated grants have a failure
    """

    def __init__(
        self,
        sql_backend: SqlBackend,
        ownership: TableOwnership,
        table_migration_index: TableMigrationIndex,
        grants_progress_encoder: GrantsProgressEncoder,
        run_id: int,
        workspace_id: int,
        catalog: str,
        schema: str = "multiworkspace",
        table: str = "historical",
    ) -> None:
        super().__init__(
            sql_backend,
            ownership,
            Table,
            run_id,
            workspace_id,
            catalog,
            schema,
            table,
        )
        self._table_migration_index = table_migration_index
        self._grants_progress_encoder = grants_progress_encoder
